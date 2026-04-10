//! Event-based extractor for fetching events from specific contracts.
//!
//! This extractor uses `starknet_getEvents` with contract address filtering for efficient
//! historical event fetching. It supports batch requests for multiple contracts in a single
//! RPC call, making it ideal for backfilling or catching up new contracts without
//! re-processing entire block ranges.
//!
//! # Key Features
//!
//! - **Batch requests**: Fetches events from N contracts in a single RPC call
//! - **Per-contract cursors**: Each contract tracks its own pagination and block progress
//! - **ETL integration**: Produces standard `ExtractionBatch` output for the decoder/sink pipeline
//! - **Block timestamp caching**: Efficiently fetches and caches block timestamps
//! - **Chain head following**: Set `to_block = u64::MAX` to follow chain head indefinitely
//!
//! # Example
//!
//! ```rust,ignore
//! use torii::etl::extractor::{EventExtractor, EventExtractorConfig, ContractEventConfig};
//! use starknet::providers::jsonrpc::{JsonRpcClient, HttpTransport};
//!
//! let config = EventExtractorConfig {
//!     contracts: vec![
//!         ContractEventConfig {
//!             address: eth_address,
//!             from_block: 100_000,
//!             to_block: 500_000,  // Fixed range
//!         },
//!         ContractEventConfig {
//!             address: strk_address,
//!             from_block: 0,
//!             to_block: u64::MAX,  // Follow chain head
//!         },
//!     ],
//!     chunk_size: 1000,
//!     block_batch_size: 10000,
//!     retry_policy: RetryPolicy::default(),
//! };
//!
//! let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
//! let extractor = EventExtractor::new(provider, config);
//!
//! // Use with torii::run() or in custom ETL loop
//! let torii_config = ToriiConfig::builder()
//!     .with_extractor(Box::new(extractor))
//!     .build();
//! ```

use crate::etl::engine_db::EngineDb;
use crate::etl::extractor::{event_common, ExtractionBatch, Extractor, RetryPolicy};
use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet::core::types::requests::GetEventsRequest;
use starknet::core::types::{BlockId, EventFilter, EventFilterWithPage, ResultPageRequest};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use torii_types::event::StarknetEvent;

const EXTRACTOR_TYPE: &str = "event";

/// Delay between polls when following chain head and caught up.
const CHAIN_HEAD_POLL_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Configuration for a single contract's event extraction.
#[derive(Debug, Clone)]
pub struct ContractEventConfig {
    /// Contract address to fetch events from.
    pub address: Felt,

    /// Starting block number.
    pub from_block: u64,

    /// Ending block number (inclusive).
    pub to_block: u64,
}

/// Configuration for the event extractor.
#[derive(Debug, Clone)]
pub struct EventExtractorConfig {
    /// Contracts to fetch events from.
    pub contracts: Vec<ContractEventConfig>,

    /// Events per RPC request (max 1024 for most providers, default: 1000).
    pub chunk_size: u64,

    /// Block range to query per iteration before pagination (default: 10000).
    /// After all events in this range are fetched (via pagination), move to next range.
    pub block_batch_size: u64,

    /// Retry policy for network failures.
    pub retry_policy: RetryPolicy,

    /// Ignore any persisted per-contract cursor state in EngineDb.
    pub ignore_saved_state: bool,

    /// Maximum number of independent RPC request chunks to execute concurrently.
    /// `0` means auto-tune from available CPU.
    pub rpc_parallelism: usize,
}

impl Default for EventExtractorConfig {
    fn default() -> Self {
        Self {
            contracts: Vec::new(),
            chunk_size: 1000,
            block_batch_size: 10000,
            retry_policy: RetryPolicy::default(),
            ignore_saved_state: false,
            rpc_parallelism: 0,
        }
    }
}

/// Per-contract extraction state.
#[derive(Debug, Clone)]
struct ContractState {
    /// Contract address.
    address: Felt,

    /// Current block being fetched (start of current range).
    current_block: u64,

    /// Target block (when to stop). Use `u64::MAX` to follow chain head.
    to_block: u64,

    /// Continuation token for pagination within current block range.
    continuation_token: Option<String>,

    /// Whether this contract has finished extraction.
    /// Note: When following chain head (to_block = u64::MAX), this is never true.
    finished: bool,

    /// Whether this contract is waiting for new blocks (caught up to chain head).
    waiting_for_blocks: bool,
}

impl ContractState {
    fn new(config: &ContractEventConfig) -> Self {
        Self {
            address: config.address,
            current_block: config.from_block,
            to_block: config.to_block,
            continuation_token: None,
            finished: false,
            waiting_for_blocks: false,
        }
    }

    /// Check if this contract has more data to fetch.
    fn is_active(&self) -> bool {
        !self.finished && !self.waiting_for_blocks
    }

    /// Check if this contract is following chain head (no fixed end).
    fn is_following_head(&self) -> bool {
        self.to_block == u64::MAX
    }

    /// Advance to next block range after current range is fully paginated.
    /// When following chain head, uses `chain_head` to cap the range.
    fn advance_block_range(&mut self, block_batch_size: u64, chain_head: Option<u64>) {
        let effective_to_block = if self.is_following_head() {
            chain_head.unwrap_or(self.current_block)
        } else {
            self.to_block
        };

        let range_end = (self.current_block + block_batch_size - 1).min(effective_to_block);

        if range_end >= effective_to_block {
            if self.is_following_head() {
                // Following chain head - mark as waiting, not finished
                self.waiting_for_blocks = true;
                self.current_block = range_end + 1;
                self.continuation_token = None;
            } else {
                // Fixed range - mark as finished
                self.finished = true;
            }
        } else {
            self.current_block = range_end + 1;
            self.continuation_token = None;
        }
    }

    /// Get the end block for the current query range.
    /// When following chain head, uses `chain_head` to cap the range.
    fn range_end(&self, block_batch_size: u64, chain_head: Option<u64>) -> u64 {
        let effective_to_block = if self.is_following_head() {
            chain_head.unwrap_or(self.current_block)
        } else {
            self.to_block
        };
        (self.current_block + block_batch_size - 1).min(effective_to_block)
    }

    /// Wake up contracts that were waiting for new blocks.
    fn wake_if_new_blocks(&mut self, chain_head: u64) {
        if self.waiting_for_blocks && chain_head >= self.current_block {
            self.waiting_for_blocks = false;
        }
    }

    /// Create state key for EngineDb persistence.
    fn state_key(&self) -> String {
        format!("{:#x}", self.address)
    }

    /// Serialize state for persistence.
    fn serialize(&self) -> String {
        match &self.continuation_token {
            Some(token) => format!("block:{}|token:{}", self.current_block, token),
            None => format!("block:{}", self.current_block),
        }
    }

    /// Deserialize state from persistence.
    fn deserialize(address: Felt, to_block: u64, value: &str) -> Result<Self> {
        let parts: Vec<&str> = value.split('|').collect();

        let block_part = parts
            .first()
            .ok_or_else(|| anyhow::anyhow!("Invalid state format: missing block"))?;
        let current_block = block_part
            .strip_prefix("block:")
            .ok_or_else(|| anyhow::anyhow!("Invalid state format: expected 'block:N'"))?
            .parse::<u64>()
            .context("Invalid block number")?;

        let continuation_token = parts.get(1).and_then(|token_part| {
            token_part
                .strip_prefix("token:")
                .map(std::string::ToString::to_string)
                .filter(|t| !t.is_empty())
        });

        // For fixed ranges, check if finished
        // For following mode (u64::MAX), never start as finished
        let finished = to_block != u64::MAX && current_block > to_block;

        Ok(Self {
            address,
            current_block,
            to_block,
            continuation_token,
            finished,
            waiting_for_blocks: false,
        })
    }
}

/// Event-based extractor using `starknet_getEvents`.
///
/// Fetches events for specific contracts using batch JSON-RPC requests.
/// Each contract tracks its own pagination and block progress.
#[derive(Debug)]
pub struct EventExtractor {
    /// Provider for RPC requests.
    provider: Arc<JsonRpcClient<HttpTransport>>,

    /// Configuration.
    config: EventExtractorConfig,

    /// Per-contract extraction state.
    contract_states: HashMap<Felt, ContractState>,

    start_block: u64,

    /// Whether the extractor has been initialized.
    initialized: bool,

    /// Cached chain head block number. Updated periodically.
    chain_head: Option<u64>,

    /// Whether any contract is following chain head.
    has_following_contracts: bool,
}

impl EventExtractor {
    fn resolved_rpc_parallelism(&self) -> usize {
        event_common::resolved_rpc_parallelism(self.config.rpc_parallelism)
    }

    /// Create a new event extractor.
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>, config: EventExtractorConfig) -> Self {
        let has_following_contracts = config.contracts.iter().any(|c| c.to_block == u64::MAX);
        Self {
            provider,
            config,
            contract_states: HashMap::new(),
            initialized: false,
            chain_head: None,
            has_following_contracts,
            start_block: 0,
        }
    }

    /// Fetch the current chain head block number.
    async fn fetch_chain_head(&self) -> Result<u64> {
        let start = std::time::Instant::now();
        let block = self
            .config
            .retry_policy
            .execute(|| {
                let provider = self.provider.clone();
                async move {
                    provider
                        .block_number()
                        .await
                        .context("Failed to fetch chain head")
                }
            })
            .await;
        ::metrics::histogram!("torii_rpc_request_duration_seconds", "method" => "block_number")
            .record(start.elapsed().as_secs_f64());
        let block = match block {
            Ok(block) => {
                ::metrics::counter!(
                    "torii_rpc_requests_total",
                    "method" => "block_number",
                    "status" => "ok"
                )
                .increment(1);
                block
            }
            Err(err) => {
                ::metrics::counter!(
                    "torii_rpc_requests_total",
                    "method" => "block_number",
                    "status" => "error"
                )
                .increment(1);
                return Err(err);
            }
        };
        Ok(block)
    }

    /// Initialize contract states from config or persisted state.
    async fn initialize(&mut self, engine_db: &EngineDb) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        for contract_config in &self.config.contracts {
            let address = contract_config.address;
            let state_key = format!("{address:#x}");

            let state = if self.config.ignore_saved_state {
                tracing::info!(
                    target: "torii::etl::event",
                    contract = %state_key,
                    from_block = contract_config.from_block,
                    to_block = contract_config.to_block,
                    "Ignoring saved state for contract; starting from configured from_block"
                );
                ContractState::new(contract_config)
            } else {
                // Try to load persisted state
                if let Some(saved_state) = engine_db
                    .get_extractor_state(EXTRACTOR_TYPE, &state_key)
                    .await?
                {
                    tracing::info!(
                        target: "torii::etl::event",
                        contract = %state_key,
                        saved_state = %saved_state,
                        configured_from_block = contract_config.from_block,
                        "Resuming contract from saved state"
                    );
                    ContractState::deserialize(address, contract_config.to_block, &saved_state)?
                } else {
                    tracing::info!(
                        target: "torii::etl::event",
                        contract = %state_key,
                        from_block = contract_config.from_block,
                        to_block = contract_config.to_block,
                        "Starting fresh extraction for contract"
                    );
                    ContractState::new(contract_config)
                }
            };

            self.contract_states.insert(address, state);
        }

        self.refresh_dynamic_contract_states(engine_db).await?;

        self.initialized = true;
        Ok(())
    }

    async fn refresh_dynamic_contract_states(&mut self, engine_db: &EngineDb) -> Result<()> {
        let persisted_states = engine_db.get_all_extractor_states(EXTRACTOR_TYPE).await?;

        for (state_key, state_value) in persisted_states {
            let Ok(address) = Felt::from_hex(&state_key) else {
                tracing::debug!(
                    target: "torii::etl::event",
                    state_key,
                    "Skipping non-contract extractor state"
                );
                continue;
            };

            if self.contract_states.contains_key(&address) {
                continue;
            }

            let to_block = self
                .config
                .contracts
                .iter()
                .find(|contract| contract.address == address)
                .map_or(u64::MAX, |contract| contract.to_block);
            let state =
                ContractState::deserialize(address, to_block, &state_value).with_context(|| {
                    format!("failed to deserialize extractor state for {state_key}")
                })?;

            if state.is_following_head() {
                self.has_following_contracts = true;
            }

            tracing::info!(
                target: "torii::etl::event",
                contract = state_key,
                state = state_value,
                to_block,
                "Loaded dynamic contract extractor state"
            );
            self.contract_states.insert(address, state);
        }

        Ok(())
    }

    /// Build batch request for all active contracts.
    fn build_batch_requests(&self) -> Vec<(Felt, ProviderRequestData)> {
        self.contract_states
            .values()
            .filter(|state| state.is_active())
            .map(|state| {
                let range_end = state.range_end(self.config.block_batch_size, self.chain_head);
                if state.continuation_token.is_none() {
                    tracing::trace!(
                        target: "torii::etl::event",
                        contract = %state.state_key(),
                        from_block = state.current_block.max(self.start_block),
                        to_block = range_end,
                        following_head = state.is_following_head(),
                        "Queueing event range request"
                    );
                } else {
                    tracing::debug!(
                        target: "torii::etl::event",
                        contract = %state.state_key(),
                        from_block = state.current_block.max(self.start_block),
                        to_block = range_end,
                        continuation_token = %state.continuation_token.as_deref().unwrap_or_default(),
                        "Queueing paginated event request"
                    );
                }
                let request = ProviderRequestData::GetEvents(GetEventsRequest {
                    filter: EventFilterWithPage {
                        event_filter: EventFilter {
                            from_block: Some(BlockId::Number(state.current_block.max(self.start_block))),
                            to_block: Some(BlockId::Number(range_end)),
                            address: Some(state.address.into()),
                            keys: None,
                        },
                        result_page_request: ResultPageRequest {
                            continuation_token: state.continuation_token.clone(),
                            chunk_size: self.config.chunk_size,
                        },
                    },
                });
                (state.address, request)
            })
            .collect()
    }

    async fn fetch_successful_transaction_hashes(
        &self,
        tx_hashes: &[Felt],
    ) -> Result<HashSet<Felt>> {
        event_common::fetch_successful_transaction_hashes(
            self.provider.clone(),
            &self.config.retry_policy,
            self.resolved_rpc_parallelism(),
            tx_hashes,
            "event",
        )
        .await
    }

    fn filter_events_by_tx_hashes(
        events: Vec<StarknetEvent>,
        successful_transaction_hashes: &HashSet<Felt>,
    ) -> Vec<StarknetEvent> {
        event_common::filter_events_by_tx_hashes(events, successful_transaction_hashes)
    }

    /// Build ExtractionBatch from events with block context.
    async fn build_batch(
        &self,
        events: Vec<StarknetEvent>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        event_common::build_batch(
            self.provider.clone(),
            &self.config.retry_policy,
            self.resolved_rpc_parallelism(),
            events,
            engine_db,
        )
        .await
    }

    /// Build composite cursor from all contract states.
    fn build_cursor(&self) -> String {
        // Format: contract1_state;contract2_state;...
        // Each state: address=block:N|token:T
        self.contract_states
            .values()
            .map(|state| format!("{}={}", state.state_key(), state.serialize()))
            .collect::<Vec<_>>()
            .join(";")
    }
}

#[async_trait]
impl Extractor for EventExtractor {
    fn set_start_block(&mut self, start_block: u64) {
        self.start_block = start_block;
    }
    async fn extract(
        &mut self,
        _cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        // Initialize on first call
        self.initialize(engine_db).await?;
        self.refresh_dynamic_contract_states(engine_db).await?;

        // Check if all contracts are finished (only possible when no following contracts)
        if self.is_finished() {
            return Ok(ExtractionBatch::empty());
        }

        // Fetch chain head if any contract is following chain head
        if self.has_following_contracts {
            let chain_head = self.fetch_chain_head().await?;
            self.chain_head = Some(chain_head);

            // Wake up any contracts that were waiting for new blocks
            for state in self.contract_states.values_mut() {
                state.wake_if_new_blocks(chain_head);
            }
        }

        // Check if all active contracts are waiting for new blocks
        let all_waiting = self
            .contract_states
            .values()
            .all(|s| s.finished || s.waiting_for_blocks);

        if all_waiting {
            tracing::debug!(
                target: "torii::etl::event",
                "All contracts caught up to chain head, waiting for new blocks"
            );
            tokio::time::sleep(CHAIN_HEAD_POLL_DELAY).await;
            return Ok(ExtractionBatch::empty());
        }

        // Build batch requests for active contracts
        let requests_with_addresses = self.build_batch_requests();
        if requests_with_addresses.is_empty() {
            return Ok(ExtractionBatch::empty());
        }

        let addresses: Vec<Felt> = requests_with_addresses.iter().map(|(a, _)| *a).collect();
        let requests: Vec<ProviderRequestData> = requests_with_addresses
            .into_iter()
            .map(|(_, r)| r)
            .collect();

        tracing::trace!(
            target: "torii::etl::event",
            contracts = requests.len(),
            chain_head = ?self.chain_head,
            "Fetching events batch"
        );

        // Execute batch request
        let responses = self
            .config
            .retry_policy
            .execute(|| {
                let provider = self.provider.clone();
                let requests_ref = &requests;
                async move {
                    provider
                        .batch_requests(requests_ref)
                        .await
                        .context("Failed to fetch events batch")
                }
            })
            .await?;

        anyhow::ensure!(
            responses.len() == addresses.len(),
            "Event batch response length mismatch: expected {} responses, got {}",
            addresses.len(),
            responses.len()
        );

        // Process responses and update state
        let mut all_events: Vec<StarknetEvent> = Vec::new();
        let mut any_advanced = false;

        for (address, response) in addresses.iter().zip(responses) {
            let state = self
                .contract_states
                .get_mut(address)
                .expect("Contract state must exist");

            if let ProviderResponseData::GetEvents(events_page) = response {
                let event_count = events_page.events.len();
                all_events.extend(StarknetEvent::filter_pending(events_page.events));

                tracing::debug!(
                    target: "torii::etl::event",
                    contract = %state.state_key(),
                    events = event_count,
                    has_more = events_page.continuation_token.is_some(),
                    "Fetched events page"
                );

                // Update state based on response
                if let Some(token) = events_page.continuation_token {
                    // More pages in current range
                    state.continuation_token = Some(token);
                } else {
                    // Current range complete, advance to next
                    let completed_from = state.current_block;
                    let completed_to =
                        state.range_end(self.config.block_batch_size, self.chain_head);
                    state.advance_block_range(self.config.block_batch_size, self.chain_head);
                    any_advanced = true;

                    tracing::trace!(
                        target: "torii::etl::event",
                        contract = %state.state_key(),
                        range_from = completed_from,
                        range_to = completed_to,
                        next_from_block = state.current_block,
                        events = event_count,
                        "Completed contract event range"
                    );

                    if state.finished {
                        tracing::info!(
                            target: "torii::etl::event",
                            contract = %state.state_key(),
                            to_block = state.to_block,
                            "Contract extraction complete"
                        );
                    } else if state.waiting_for_blocks {
                        tracing::debug!(
                            target: "torii::etl::event",
                            contract = %state.state_key(),
                            current_block = state.current_block,
                            "Contract caught up to chain head, waiting for new blocks"
                        );
                    }
                }
            } else {
                anyhow::bail!("Unexpected response type for contract {address:#x}");
            }
        }

        if all_events.is_empty() && !any_advanced {
            // No events and no progress - all contracts must be done
            return Ok(ExtractionBatch::empty());
        }

        tracing::trace!(
            target: "torii::etl::event",
            events = all_events.len(),
            "Extracted events from batch"
        );

        let unique_tx_hashes: Vec<Felt> = all_events
            .iter()
            .map(|event| event.transaction_hash)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let successful_transaction_hashes = self
            .fetch_successful_transaction_hashes(&unique_tx_hashes)
            .await?;
        let events_before_filter = all_events.len();
        let reverted_txs = unique_tx_hashes
            .len()
            .saturating_sub(successful_transaction_hashes.len());
        ::metrics::counter!("torii_reverted_transactions_filtered_total")
            .increment(reverted_txs as u64);
        let filtered_events =
            Self::filter_events_by_tx_hashes(all_events, &successful_transaction_hashes);

        tracing::trace!(
            target: "torii::etl::event",
            events_before_filter,
            events_after_filter = filtered_events.len(),
            unique_txs_checked = unique_tx_hashes.len(),
            reverted_txs,
            "Filtered reverted transactions from event batch"
        );

        // Build batch with block context
        let mut batch = self.build_batch(filtered_events, engine_db).await?;
        batch.cursor = Some(self.build_cursor());
        batch.chain_head = self.chain_head;

        Ok(batch)
    }

    fn is_finished(&self) -> bool {
        self.initialized && self.contract_states.values().all(|s| s.finished)
    }

    async fn commit_cursor(&mut self, _cursor: &str, engine_db: &EngineDb) -> Result<()> {
        // Persist each contract's state individually
        for state in self.contract_states.values() {
            engine_db
                .set_extractor_state(EXTRACTOR_TYPE, &state.state_key(), &state.serialize())
                .await
                .with_context(|| {
                    format!("Failed to persist state for contract {}", state.state_key())
                })?;
        }

        tracing::debug!(
            target: "torii::etl::event",
            contracts = self.contract_states.len(),
            "Committed cursor for all contracts"
        );

        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_state_serialize_deserialize() {
        let address = Felt::from_hex("0x123").unwrap();
        let to_block = 1000;

        // Without continuation token
        let state = ContractState {
            address,
            current_block: 500,
            to_block,
            continuation_token: None,
            finished: false,
            waiting_for_blocks: false,
        };
        let serialized = state.serialize();
        assert_eq!(serialized, "block:500");

        let deserialized = ContractState::deserialize(address, to_block, &serialized).unwrap();
        assert_eq!(deserialized.current_block, 500);
        assert!(deserialized.continuation_token.is_none());

        // With continuation token
        let state_with_token = ContractState {
            address,
            current_block: 500,
            to_block,
            continuation_token: Some("abc123".to_string()),
            finished: false,
            waiting_for_blocks: false,
        };
        let serialized = state_with_token.serialize();
        assert_eq!(serialized, "block:500|token:abc123");

        let deserialized = ContractState::deserialize(address, to_block, &serialized).unwrap();
        assert_eq!(deserialized.current_block, 500);
        assert_eq!(deserialized.continuation_token, Some("abc123".to_string()));
    }

    #[test]
    fn test_contract_state_advance_fixed_range() {
        let address = Felt::from_hex("0x123").unwrap();
        let mut state = ContractState {
            address,
            current_block: 0,
            to_block: 25000,
            continuation_token: Some("token".to_string()),
            finished: false,
            waiting_for_blocks: false,
        };

        // First advance (no chain_head needed for fixed range)
        state.advance_block_range(10000, None);
        assert_eq!(state.current_block, 10000);
        assert!(state.continuation_token.is_none());
        assert!(!state.finished);
        assert!(!state.waiting_for_blocks);

        // Second advance
        state.advance_block_range(10000, None);
        assert_eq!(state.current_block, 20000);
        assert!(!state.finished);

        // Final advance - reaches end
        state.advance_block_range(10000, None);
        assert!(state.finished);
        assert!(!state.waiting_for_blocks);
    }

    #[test]
    fn test_contract_state_advance_following_head() {
        let address = Felt::from_hex("0x123").unwrap();
        let mut state = ContractState {
            address,
            current_block: 0,
            to_block: u64::MAX, // Following chain head
            continuation_token: None,
            finished: false,
            waiting_for_blocks: false,
        };

        // Advance with chain_head at 15000
        state.advance_block_range(10000, Some(15000));
        assert_eq!(state.current_block, 10000);
        assert!(!state.finished);
        assert!(!state.waiting_for_blocks);

        // Advance again - should catch up to chain head and wait
        state.advance_block_range(10000, Some(15000));
        assert_eq!(state.current_block, 15001);
        assert!(!state.finished); // Never finishes when following
        assert!(state.waiting_for_blocks); // But waits for new blocks

        // Wake up when new blocks arrive
        state.wake_if_new_blocks(20000);
        assert!(!state.waiting_for_blocks);
    }

    #[test]
    fn test_contract_state_range_end() {
        let state = ContractState {
            address: Felt::ZERO,
            current_block: 5000,
            to_block: 12000,
            continuation_token: None,
            finished: false,
            waiting_for_blocks: false,
        };

        // Normal range (no chain_head)
        assert_eq!(state.range_end(10000, None), 12000); // Capped at to_block

        // Smaller range
        assert_eq!(state.range_end(5000, None), 9999);
    }

    #[test]
    fn test_contract_state_range_end_following_head() {
        let state = ContractState {
            address: Felt::ZERO,
            current_block: 5000,
            to_block: u64::MAX, // Following chain head
            continuation_token: None,
            finished: false,
            waiting_for_blocks: false,
        };

        // With chain_head, should cap at chain_head
        assert_eq!(state.range_end(10000, Some(8000)), 8000);

        // Chain head beyond range_end
        assert_eq!(state.range_end(10000, Some(20000)), 14999);
    }

    #[test]
    fn test_filter_events_by_tx_hashes() {
        let keep = Felt::from(1u64);
        let drop = Felt::from(2u64);
        let events = vec![
            EmittedEvent {
                from_address: Felt::ZERO,
                keys: Vec::new(),
                data: Vec::new(),
                block_hash: None,
                block_number: None,
                transaction_hash: keep,
            },
            EmittedEvent {
                from_address: Felt::ZERO,
                keys: Vec::new(),
                data: Vec::new(),
                block_hash: None,
                block_number: None,
                transaction_hash: drop,
            },
        ];

        let mut successful = HashSet::new();
        successful.insert(keep);

        let filtered = EventExtractor::filter_events_by_tx_hashes(events, &successful);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].transaction_hash, keep);
    }

    #[tokio::test]
    async fn test_build_batch_populates_transaction_context() {
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
            starknet::providers::Url::parse("http://localhost:5050").unwrap(),
        )));
        let extractor = EventExtractor::new(provider, EventExtractorConfig::default());
        let engine_db = EngineDb::new(crate::etl::engine_db::EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();
        let mut timestamps = HashMap::new();
        timestamps.insert(123, 1);
        engine_db
            .insert_block_timestamps(&timestamps)
            .await
            .unwrap();

        let tx_hash = Felt::from(42u64);
        let event = EmittedEvent {
            from_address: Felt::from(7u64),
            keys: Vec::new(),
            data: Vec::new(),
            block_hash: None,
            block_number: Some(123),
            transaction_hash: tx_hash,
        };

        let batch = extractor
            .build_batch(vec![event], &engine_db)
            .await
            .unwrap();
        let tx = batch.transactions.get(&tx_hash).unwrap();
        assert_eq!(tx.hash, tx_hash);
        assert_eq!(tx.block_number, 123);
    }

    #[tokio::test]
    async fn test_initialize_resumes_from_saved_state_by_default() {
        let address = Felt::from(0x123_u64);
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
            starknet::providers::Url::parse("http://localhost:5050").unwrap(),
        )));
        let mut extractor = EventExtractor::new(
            provider,
            EventExtractorConfig {
                contracts: vec![ContractEventConfig {
                    address,
                    from_block: 7,
                    to_block: 100,
                }],
                ..EventExtractorConfig::default()
            },
        );
        let engine_db = EngineDb::new(crate::etl::engine_db::EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();
        engine_db
            .set_extractor_state(EXTRACTOR_TYPE, &format!("{address:#x}"), "block:42")
            .await
            .unwrap();

        extractor.initialize(&engine_db).await.unwrap();

        assert_eq!(
            extractor
                .contract_states
                .get(&address)
                .unwrap()
                .current_block,
            42
        );
    }

    #[tokio::test]
    async fn test_initialize_can_ignore_saved_state() {
        let address = Felt::from(0x456_u64);
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
            starknet::providers::Url::parse("http://localhost:5050").unwrap(),
        )));
        let mut extractor = EventExtractor::new(
            provider,
            EventExtractorConfig {
                contracts: vec![ContractEventConfig {
                    address,
                    from_block: 7,
                    to_block: 100,
                }],
                ignore_saved_state: true,
                ..EventExtractorConfig::default()
            },
        );
        let engine_db = EngineDb::new(crate::etl::engine_db::EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();
        engine_db
            .set_extractor_state(EXTRACTOR_TYPE, &format!("{address:#x}"), "block:42")
            .await
            .unwrap();

        extractor.initialize(&engine_db).await.unwrap();

        assert_eq!(
            extractor
                .contract_states
                .get(&address)
                .unwrap()
                .current_block,
            7
        );
    }

    #[tokio::test]
    async fn test_refresh_dynamic_contract_states_loads_runtime_contracts() {
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
            starknet::providers::Url::parse("http://localhost:5050").unwrap(),
        )));
        let mut extractor = EventExtractor::new(provider, EventExtractorConfig::default());
        let engine_db = EngineDb::new(crate::etl::engine_db::EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();
        let address = Felt::from_hex("0xdead").unwrap();

        engine_db
            .set_extractor_state(EXTRACTOR_TYPE, &format!("{address:#x}"), "block:42")
            .await
            .unwrap();

        extractor
            .refresh_dynamic_contract_states(&engine_db)
            .await
            .unwrap();

        let state = extractor.contract_states.get(&address).unwrap();
        assert_eq!(state.current_block, 42);
        assert_eq!(state.to_block, u64::MAX);
        assert!(!state.finished);
    }
}
