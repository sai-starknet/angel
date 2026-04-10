//! Global event extractor for fetching all events without contract filtering.
//!
//! This extractor uses `starknet_getEvents` with no address filter and a single
//! global cursor. It is suitable for runtime auto-discovery workflows.

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet::core::types::requests::GetEventsRequest;
use starknet::core::types::{BlockId, EventFilter, EventFilterWithPage, ResultPageRequest};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use std::collections::HashSet;
use std::sync::Arc;
use torii_types::event::StarknetEvent;

use crate::etl::engine_db::EngineDb;
use crate::etl::extractor::event_common::{
    build_batch, fetch_successful_transaction_hashes, filter_events_by_tx_hashes,
    resolved_rpc_parallelism,
};
use crate::etl::extractor::{ExtractionBatch, Extractor, RetryPolicy};

const EXTRACTOR_TYPE: &str = "global_event";
const STATE_KEY: &str = "global";
const CHAIN_HEAD_POLL_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct GlobalEventExtractorConfig {
    pub from_block: u64,
    pub to_block: u64,
    pub chunk_size: u64,
    pub block_batch_size: u64,
    pub retry_policy: RetryPolicy,
    pub ignore_saved_state: bool,
    pub rpc_parallelism: usize,
}

impl Default for GlobalEventExtractorConfig {
    fn default() -> Self {
        Self {
            from_block: 0,
            to_block: u64::MAX,
            chunk_size: 1000,
            block_batch_size: 10000,
            retry_policy: RetryPolicy::default(),
            ignore_saved_state: false,
            rpc_parallelism: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct GlobalState {
    current_block: u64,
    to_block: u64,
    continuation_token: Option<String>,
    finished: bool,
    waiting_for_blocks: bool,
}

impl GlobalState {
    fn new(config: &GlobalEventExtractorConfig) -> Self {
        Self {
            current_block: config.from_block,
            to_block: config.to_block,
            continuation_token: None,
            finished: false,
            waiting_for_blocks: false,
        }
    }

    fn is_following_head(&self) -> bool {
        self.to_block == u64::MAX
    }

    fn range_end(&self, block_batch_size: u64, chain_head: Option<u64>) -> u64 {
        let effective_to_block = if self.is_following_head() {
            chain_head.unwrap_or(self.current_block)
        } else {
            self.to_block
        };
        (self.current_block + block_batch_size - 1).min(effective_to_block)
    }

    fn advance_block_range(&mut self, block_batch_size: u64, chain_head: Option<u64>) {
        let effective_to_block = if self.is_following_head() {
            chain_head.unwrap_or(self.current_block)
        } else {
            self.to_block
        };

        let range_end = (self.current_block + block_batch_size - 1).min(effective_to_block);

        if range_end >= effective_to_block {
            if self.is_following_head() {
                self.waiting_for_blocks = true;
                self.current_block = range_end + 1;
                self.continuation_token = None;
            } else {
                self.finished = true;
            }
        } else {
            self.current_block = range_end + 1;
            self.continuation_token = None;
        }
    }

    fn wake_if_new_blocks(&mut self, chain_head: u64) {
        if self.waiting_for_blocks && chain_head >= self.current_block {
            self.waiting_for_blocks = false;
        }
    }

    fn serialize(&self) -> String {
        match &self.continuation_token {
            Some(token) => format!("block:{}|token:{}", self.current_block, token),
            None => format!("block:{}", self.current_block),
        }
    }

    fn deserialize(to_block: u64, value: &str) -> Result<Self> {
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

        let finished = to_block != u64::MAX && current_block > to_block;

        Ok(Self {
            current_block,
            to_block,
            continuation_token,
            finished,
            waiting_for_blocks: false,
        })
    }
}

#[derive(Debug)]
pub struct GlobalEventExtractor {
    provider: Arc<JsonRpcClient<HttpTransport>>,
    config: GlobalEventExtractorConfig,
    state: GlobalState,
    initialized: bool,
    chain_head: Option<u64>,
    follows_head: bool,
}

impl GlobalEventExtractor {
    pub fn new(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        config: GlobalEventExtractorConfig,
    ) -> Self {
        let state = GlobalState::new(&config);
        let follows_head = config.to_block == u64::MAX;
        Self {
            provider,
            config,
            state,
            initialized: false,
            chain_head: None,
            follows_head,
        }
    }

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

    async fn initialize(&mut self, engine_db: &EngineDb) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        if self.config.ignore_saved_state {
            self.state = GlobalState::new(&self.config);
        } else if let Some(saved_state) = engine_db
            .get_extractor_state(EXTRACTOR_TYPE, STATE_KEY)
            .await?
        {
            self.state = GlobalState::deserialize(self.config.to_block, &saved_state)?;
        }

        self.initialized = true;
        Ok(())
    }

    fn build_request(&self) -> ProviderRequestData {
        let range_end = self
            .state
            .range_end(self.config.block_batch_size, self.chain_head);

        ProviderRequestData::GetEvents(GetEventsRequest {
            filter: EventFilterWithPage {
                event_filter: EventFilter {
                    from_block: Some(BlockId::Number(self.state.current_block)),
                    to_block: Some(BlockId::Number(range_end)),
                    address: None,
                    keys: None,
                },
                result_page_request: ResultPageRequest {
                    continuation_token: self.state.continuation_token.clone(),
                    chunk_size: self.config.chunk_size,
                },
            },
        })
    }

    fn build_cursor(&self) -> String {
        self.state.serialize()
    }
}

#[async_trait]
impl Extractor for GlobalEventExtractor {
    fn set_start_block(&mut self, start_block: u64) {
        self.state.current_block = start_block.max(self.state.current_block);
        self.config.from_block = start_block.max(self.config.from_block);
    }
    async fn extract(
        &mut self,
        _cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        self.initialize(engine_db).await?;

        if self.is_finished() {
            return Ok(ExtractionBatch::empty());
        }

        if self.follows_head {
            let chain_head = self.fetch_chain_head().await?;
            self.chain_head = Some(chain_head);
            self.state.wake_if_new_blocks(chain_head);
        }

        if self.state.waiting_for_blocks {
            tokio::time::sleep(CHAIN_HEAD_POLL_DELAY).await;
            return Ok(ExtractionBatch::empty());
        }

        let request = self.build_request();
        let responses = self
            .config
            .retry_policy
            .execute(|| {
                let provider = self.provider.clone();
                let request = request.clone();
                async move {
                    provider
                        .batch_requests(std::slice::from_ref(&request))
                        .await
                        .context("Failed to fetch global events")
                }
            })
            .await?;

        let response = responses
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Missing response for global event request"))?;

        let events_page = match response {
            ProviderResponseData::GetEvents(events_page) => events_page,
            _ => anyhow::bail!("Unexpected response type for global event request"),
        };

        let mut all_events: Vec<StarknetEvent> = StarknetEvent::filter_pending(events_page.events);
        let mut any_advanced = false;

        if let Some(token) = events_page.continuation_token {
            self.state.continuation_token = Some(token);
        } else {
            self.state
                .advance_block_range(self.config.block_batch_size, self.chain_head);
            any_advanced = true;
        }

        if all_events.is_empty() && !any_advanced {
            return Ok(ExtractionBatch::empty());
        }

        let unique_tx_hashes: Vec<Felt> = all_events
            .iter()
            .map(|event| event.transaction_hash)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let successful_transaction_hashes = fetch_successful_transaction_hashes(
            self.provider.clone(),
            &self.config.retry_policy,
            resolved_rpc_parallelism(self.config.rpc_parallelism),
            &unique_tx_hashes,
            "global_event",
        )
        .await?;

        let reverted_txs = unique_tx_hashes
            .len()
            .saturating_sub(successful_transaction_hashes.len());
        ::metrics::counter!("torii_reverted_transactions_filtered_total")
            .increment(reverted_txs as u64);

        all_events = filter_events_by_tx_hashes(all_events, &successful_transaction_hashes);

        let mut batch = build_batch(
            self.provider.clone(),
            &self.config.retry_policy,
            resolved_rpc_parallelism(self.config.rpc_parallelism),
            all_events,
            engine_db,
        )
        .await?;
        batch.cursor = Some(self.build_cursor());
        batch.chain_head = self.chain_head;
        Ok(batch)
    }

    fn is_finished(&self) -> bool {
        self.initialized && self.state.finished
    }

    async fn commit_cursor(&mut self, _cursor: &str, engine_db: &EngineDb) -> Result<()> {
        engine_db
            .set_extractor_state(EXTRACTOR_TYPE, STATE_KEY, &self.state.serialize())
            .await
            .context("Failed to persist global event state")?;
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
    fn test_global_state_serialize_deserialize() {
        let state = GlobalState {
            current_block: 123,
            to_block: 1000,
            continuation_token: Some("token".to_string()),
            finished: false,
            waiting_for_blocks: false,
        };

        let encoded = state.serialize();
        assert_eq!(encoded, "block:123|token:token");

        let decoded = GlobalState::deserialize(1000, &encoded).unwrap();
        assert_eq!(decoded.current_block, 123);
        assert_eq!(decoded.continuation_token, Some("token".to_string()));
    }

    #[test]
    fn test_global_state_following_head_waiting() {
        let mut state = GlobalState {
            current_block: 100,
            to_block: u64::MAX,
            continuation_token: None,
            finished: false,
            waiting_for_blocks: false,
        };

        state.advance_block_range(50, Some(120));
        assert_eq!(state.current_block, 121);
        assert!(state.waiting_for_blocks);
        assert!(!state.finished);

        state.wake_if_new_blocks(150);
        assert!(!state.waiting_for_blocks);
    }

    #[tokio::test]
    async fn test_initialize_resumes_saved_state() {
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(
            starknet::providers::Url::parse("http://localhost:5050").unwrap(),
        )));

        let mut extractor = GlobalEventExtractor::new(
            provider,
            GlobalEventExtractorConfig {
                from_block: 0,
                to_block: 200,
                ..GlobalEventExtractorConfig::default()
            },
        );

        let engine_db = EngineDb::new(crate::etl::engine_db::EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();

        engine_db
            .set_extractor_state(EXTRACTOR_TYPE, STATE_KEY, "block:77")
            .await
            .unwrap();

        extractor.initialize(&engine_db).await.unwrap();
        assert_eq!(extractor.state.current_block, 77);
    }
}
