//! ERC20 sink for processing transfers and approvals
//!
//! This sink:
//! - Stores transfer and approval records in the database
//! - Tracks balances with automatic inconsistency detection
//! - Publishes events via EventBus for real-time subscriptions (simple clients)
//! - Broadcasts events via gRPC service for rich subscriptions (advanced clients)
//!
//! Balance tracking uses a "fetch-on-inconsistency" approach:
//! - Computes balances from transfer events
//! - When a balance would go negative (genesis allocation, airdrop, etc.),
//!   fetches the actual balance from the chain and adjusts

use crate::balance_fetcher::BalanceFetcher;
use crate::decoder::{Approval as DecodedApproval, Transfer as DecodedTransfer};
use crate::grpc_service::Erc20Service;
use crate::handlers::FetchErc20MetadataCommand;
use crate::proto;
use crate::storage::{ApprovalData, Erc20Storage, TransferData};
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use primitive_types::U256;
use prost::Message;
use prost_types::Any;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet_types_raw::Felt;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use torii::command::CommandBusSender;
use torii::etl::sink::{EventBus, TopicInfo};
use torii::etl::{Envelope, ExtractionBatch, Sink, TypeId};
use torii::grpc::UpdateType;
use torii_common::u256_to_bytes;

/// Default threshold for "live" detection: 100 blocks from chain head.
/// Events from blocks older than this won't be broadcast to real-time subscribers.
const LIVE_THRESHOLD_BLOCKS: u64 = 100;

/// ERC20 transfer and approval sink
///
/// Processes ERC20 Transfer and Approval events and:
/// - Stores records in the database
/// - Tracks balances with automatic inconsistency detection
/// - Publishes events via EventBus for real-time subscriptions (only when live)
/// - Broadcasts events via gRPC service for rich subscriptions (only when live)
///
/// During historical indexing (more than 100 blocks from chain head), events are
/// stored but not broadcast to avoid overwhelming real-time subscribers.
pub struct Erc20Sink {
    storage: Arc<Erc20Storage>,
    event_bus: Option<Arc<EventBus>>,
    grpc_service: Option<Erc20Service>,
    /// Balance fetcher for RPC calls (None = balance tracking disabled)
    balance_fetcher: Option<Arc<BalanceFetcher>>,
    /// Whether contract metadata commands should be dispatched.
    metadata_commands_enabled: bool,
    /// Command bus sender for background metadata work.
    command_bus: Option<CommandBusSender>,
    /// Commands already queued but not yet observed in storage.
    pending_metadata_commands: tokio::sync::Mutex<HashSet<Felt>>,
    /// In-memory counters to avoid full-table COUNT(*) in the ingest hot path.
    total_transfers: AtomicU64,
    total_approvals: AtomicU64,
}

impl Erc20Sink {
    pub fn new(storage: Arc<Erc20Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
            grpc_service: None,
            balance_fetcher: None,
            metadata_commands_enabled: false,
            command_bus: None,
            pending_metadata_commands: tokio::sync::Mutex::new(HashSet::new()),
            // Avoid startup full-table COUNT(*) scans on large datasets.
            total_transfers: AtomicU64::new(0),
            total_approvals: AtomicU64::new(0),
        }
    }

    /// Set the gRPC service for dual publishing
    pub fn with_grpc_service(mut self, service: Erc20Service) -> Self {
        self.grpc_service = Some(service);
        self
    }

    /// Enable balance tracking with a provider for RPC calls
    ///
    /// When enabled, the sink will:
    /// - Track balances computed from transfer events
    /// - Detect when a balance would go negative (indicating missed history)
    /// - Fetch actual balance from the chain and adjust
    /// - Record adjustments in an audit table
    pub fn with_balance_tracking(mut self, provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        self.balance_fetcher = Some(Arc::new(BalanceFetcher::new(provider)));
        self
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &Arc<Erc20Storage> {
        &self.storage
    }

    /// Filter function for ERC20 transfer events
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    /// - "from": Filter by sender address (hex string)
    /// - "to": Filter by receiver address (hex string)
    /// - "wallet": Filter by wallet address - matches from OR to (OR logic)
    fn matches_transfer_filters(
        transfer: &proto::Transfer,
        filters: &HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        // Wallet filter with OR logic (matches from OR to)
        if let Some(wallet_filter) = filters.get("wallet") {
            let from_hex = format!("0x{}", hex::encode(&transfer.from));
            let to_hex = format!("0x{}", hex::encode(&transfer.to));
            if !from_hex.eq_ignore_ascii_case(wallet_filter)
                && !to_hex.eq_ignore_ascii_case(wallet_filter)
            {
                return false;
            }
        }

        // Exact token filter
        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&transfer.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        // Exact from filter
        if let Some(from_filter) = filters.get("from") {
            let from_hex = format!("0x{}", hex::encode(&transfer.from));
            if !from_hex.eq_ignore_ascii_case(from_filter) {
                return false;
            }
        }

        // Exact to filter
        if let Some(to_filter) = filters.get("to") {
            let to_hex = format!("0x{}", hex::encode(&transfer.to));
            if !to_hex.eq_ignore_ascii_case(to_filter) {
                return false;
            }
        }

        true
    }

    /// Filter function for ERC20 approval events
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    /// - "owner": Filter by owner address (hex string)
    /// - "spender": Filter by spender address (hex string)
    /// - "account": Filter by account address - matches owner OR spender (OR logic)
    fn matches_approval_filters(
        approval: &proto::Approval,
        filters: &HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        // Account filter with OR logic (matches owner OR spender)
        if let Some(account_filter) = filters.get("account") {
            let owner_hex = format!("0x{}", hex::encode(&approval.owner));
            let spender_hex = format!("0x{}", hex::encode(&approval.spender));
            if !owner_hex.eq_ignore_ascii_case(account_filter)
                && !spender_hex.eq_ignore_ascii_case(account_filter)
            {
                return false;
            }
        }

        // Exact token filter
        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&approval.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        // Exact owner filter
        if let Some(owner_filter) = filters.get("owner") {
            let owner_hex = format!("0x{}", hex::encode(&approval.owner));
            if !owner_hex.eq_ignore_ascii_case(owner_filter) {
                return false;
            }
        }

        // Exact spender filter
        if let Some(spender_filter) = filters.get("spender") {
            let spender_hex = format!("0x{}", hex::encode(&approval.spender));
            if !spender_hex.eq_ignore_ascii_case(spender_filter) {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl Sink for Erc20Sink {
    fn name(&self) -> &'static str {
        "erc20"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("erc20.transfer"), TypeId::new("erc20.approval")]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        context: &torii::etl::sink::SinkContext,
    ) -> Result<()> {
        self.event_bus = Some(event_bus);
        self.command_bus = Some(context.command_bus.clone());
        tracing::info!(target: "torii_erc20::sink", "ERC20 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        let mut transfers: Vec<TransferData> = Vec::with_capacity(envelopes.len());
        let mut approvals: Vec<ApprovalData> = Vec::with_capacity(envelopes.len());
        let mut inserted_transfers: u64 = 0;
        let mut inserted_approvals: u64 = 0;

        // Get block timestamps from batch for enrichment
        let block_timestamps: HashMap<u64, i64> = batch
            .blocks
            .iter()
            .map(|(num, block)| (*num, block.timestamp as i64))
            .collect();

        for envelope in envelopes {
            // Handle transfers
            if envelope.type_id == TypeId::new("erc20.transfer") {
                if let Some(transfer) = envelope.body.as_any().downcast_ref::<DecodedTransfer>() {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(TransferData {
                        id: None,
                        token: transfer.token,
                        from: transfer.from,
                        to: transfer.to,
                        amount: transfer.amount,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle approvals
            else if envelope.type_id == TypeId::new("erc20.approval") {
                if let Some(approval) = envelope.body.as_any().downcast_ref::<DecodedApproval>() {
                    let timestamp = block_timestamps.get(&approval.block_number).copied();
                    approvals.push(ApprovalData {
                        id: None,
                        token: approval.token,
                        owner: approval.owner,
                        spender: approval.spender,
                        amount: approval.amount,
                        block_number: approval.block_number,
                        tx_hash: approval.transaction_hash,
                        timestamp,
                    });
                }
            }
        }

        // Dispatch metadata fetches for new token contracts.
        if self.metadata_commands_enabled {
            if let Some(ref command_bus) = self.command_bus {
                let mut new_tokens: HashSet<Felt> = HashSet::new();
                for transfer in &transfers {
                    new_tokens.insert(transfer.token);
                }
                for approval in &approvals {
                    new_tokens.insert(approval.token);
                }

                let candidate_tokens = new_tokens.into_iter();
                let unchecked_tokens = {
                    let pending = self.pending_metadata_commands.lock().await;
                    candidate_tokens
                        .into_iter()
                        .filter(|token| !pending.contains(token))
                        .collect::<Vec<_>>()
                };

                match self
                    .storage
                    .has_token_metadata_batch(&unchecked_tokens)
                    .await
                {
                    Ok(existing_metadata) => {
                        let mut pending = self.pending_metadata_commands.lock().await;
                        for token in &existing_metadata {
                            pending.remove(token);
                        }

                        for token in unchecked_tokens
                            .into_iter()
                            .filter(|token| !existing_metadata.contains(token))
                        {
                            if !pending.insert(token) {
                                continue;
                            }
                            if let Err(error) =
                                command_bus.dispatch(FetchErc20MetadataCommand { token })
                            {
                                pending.remove(&token);
                                tracing::warn!(
                                    target: "torii_erc20::sink",
                                    token = %format!("{:#x}", token),
                                    error = %error,
                                    "Failed to dispatch ERC20 metadata command"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "torii_erc20::sink",
                            error = %error,
                            "Failed to batch-check token metadata"
                        );
                    }
                }
            }
        }

        // Batch insert transfers
        if !transfers.is_empty() {
            let insert_transfers_start = std::time::Instant::now();
            let transfer_count = match self.storage.insert_transfers_batch(&transfers).await {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc20::sink",
                        count = transfers.len(),
                        error = %e,
                        "Failed to batch insert transfers"
                    );
                    return Err(e);
                }
            };
            ::metrics::histogram!("torii_erc20_sink_insert_transfers_duration_seconds")
                .record(insert_transfers_start.elapsed().as_secs_f64());

            if transfer_count > 0 {
                inserted_transfers = transfer_count as u64;
                self.total_transfers
                    .fetch_add(inserted_transfers, Ordering::Relaxed);

                tracing::info!(
                    target: "torii_erc20::sink",
                    count = transfer_count,
                    "Batch inserted transfers"
                );

                // Update balances if balance tracking is enabled
                if let Some(ref fetcher) = self.balance_fetcher {
                    // Step 1: Check which balances need adjustment (would go negative)
                    let check_balances_start = std::time::Instant::now();
                    let (adjustment_requests, balance_snapshot) = match self
                        .storage
                        .check_balances_batch_with_snapshot(&transfers)
                        .await
                    {
                        Ok(result) => (result.adjustment_requests, Some(result.balance_snapshot)),
                        Err(e) => {
                            tracing::warn!(
                                target: "torii_erc20::sink",
                                error = %e,
                                "Failed to check balance inconsistencies, skipping balance tracking"
                            );
                            (Vec::new(), None)
                        }
                    };
                    ::metrics::histogram!("torii_erc20_sink_check_balances_duration_seconds")
                        .record(check_balances_start.elapsed().as_secs_f64());

                    // Step 2: Batch fetch actual balances from RPC for inconsistent wallets
                    let mut adjustments: HashMap<(Felt, Felt), U256> = HashMap::new();
                    if !adjustment_requests.is_empty() {
                        tracing::info!(
                            target: "torii_erc20::sink",
                            count = adjustment_requests.len(),
                            "Fetching balance adjustments from RPC"
                        );

                        match fetcher.fetch_balances_batch(&adjustment_requests).await {
                            Ok(fetched) => {
                                for (token, wallet, balance) in fetched {
                                    adjustments.insert((token, wallet), balance);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "torii_erc20::sink",
                                    error = %e,
                                    "Failed to fetch balances from RPC, using 0 for adjustments"
                                );
                                // On failure, use 0 for all requested adjustments
                                for req in &adjustment_requests {
                                    adjustments.insert((req.token, req.wallet), U256::from(0u64));
                                }
                            }
                        }
                    }

                    // Step 3: Apply transfers with adjustments to update balances
                    let apply_balances_start = std::time::Instant::now();
                    if let Err(e) = self
                        .storage
                        .apply_transfers_with_adjustments_with_snapshot(
                            &transfers,
                            &adjustments,
                            balance_snapshot,
                        )
                        .await
                    {
                        tracing::error!(
                            target: "torii_erc20::sink",
                            error = %e,
                            "Failed to apply balance updates"
                        );
                        // Don't fail the whole batch - transfers are already inserted
                    }
                    ::metrics::histogram!("torii_erc20_sink_apply_balances_duration_seconds")
                        .record(apply_balances_start.elapsed().as_secs_f64());
                }

                // Only broadcast to real-time subscribers when near chain head
                let is_live = batch.is_live(LIVE_THRESHOLD_BLOCKS);
                if is_live {
                    // Publish transfer events
                    for transfer in &transfers {
                        let proto_transfer = proto::Transfer {
                            token: transfer.token.to_be_bytes_vec(),
                            from: transfer.from.to_be_bytes_vec(),
                            to: transfer.to.to_be_bytes_vec(),
                            amount: u256_to_bytes(transfer.amount),
                            block_number: transfer.block_number,
                            tx_hash: transfer.tx_hash.to_be_bytes_vec(),
                            timestamp: transfer.timestamp.unwrap_or(0),
                        };

                        // Publish to EventBus (simple clients)
                        if let Some(event_bus) = &self.event_bus {
                            let mut buf = Vec::new();
                            proto_transfer.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc20.Transfer"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc20.transfer",
                                "erc20.transfer",
                                &any,
                                &proto_transfer,
                                UpdateType::Created,
                                Self::matches_transfer_filters,
                            );
                        }

                        // Broadcast to gRPC service (rich clients)
                        if let Some(grpc_service) = &self.grpc_service {
                            grpc_service.broadcast_transfer(proto_transfer);
                        }
                    }
                }
            }
        }

        // Batch insert approvals
        if !approvals.is_empty() {
            let insert_approvals_start = std::time::Instant::now();
            let approval_count = match self.storage.insert_approvals_batch(&approvals).await {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc20::sink",
                        count = approvals.len(),
                        error = %e,
                        "Failed to batch insert approvals"
                    );
                    return Err(e);
                }
            };
            ::metrics::histogram!("torii_erc20_sink_insert_approvals_duration_seconds")
                .record(insert_approvals_start.elapsed().as_secs_f64());

            if approval_count > 0 {
                inserted_approvals = approval_count as u64;
                self.total_approvals
                    .fetch_add(inserted_approvals, Ordering::Relaxed);

                tracing::info!(
                    target: "torii_erc20::sink",
                    count = approval_count,
                    "Batch inserted approvals"
                );

                // Only broadcast to real-time subscribers when near chain head
                let is_live = batch.is_live(LIVE_THRESHOLD_BLOCKS);
                if is_live {
                    // Publish approval events
                    for approval in &approvals {
                        let proto_approval = proto::Approval {
                            token: approval.token.to_be_bytes_vec(),
                            owner: approval.owner.to_be_bytes_vec(),
                            spender: approval.spender.to_be_bytes_vec(),
                            amount: u256_to_bytes(approval.amount),
                            block_number: approval.block_number,
                            tx_hash: approval.tx_hash.to_be_bytes_vec(),
                            timestamp: approval.timestamp.unwrap_or(0),
                        };

                        // Publish to EventBus (simple clients)
                        if let Some(event_bus) = &self.event_bus {
                            let mut buf = Vec::new();
                            proto_approval.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc20.Approval"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc20.approval",
                                "erc20.approval",
                                &any,
                                &proto_approval,
                                UpdateType::Created,
                                Self::matches_approval_filters,
                            );
                        }

                        // Broadcast to gRPC service (rich clients)
                        if let Some(grpc_service) = &self.grpc_service {
                            grpc_service.broadcast_approval(proto_approval);
                        }
                    }
                }
            }
        }

        // Log combined statistics without full-table scans.
        if inserted_transfers > 0 || inserted_approvals > 0 {
            tracing::info!(
                target: "torii_erc20::sink",
                batch_transfers = inserted_transfers,
                batch_approvals = inserted_approvals,
                total_transfers = self.total_transfers.load(Ordering::Relaxed),
                total_approvals = self.total_approvals.load(Ordering::Relaxed),
                blocks = batch.blocks.len(),
                "Total statistics"
            );
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![
            TopicInfo::new(
                "erc20.transfer",
                vec![
                    "token".to_string(),
                    "from".to_string(),
                    "to".to_string(),
                    "wallet".to_string(),
                ],
                "ERC20 token transfers. Use 'wallet' filter for from OR to matching.",
            ),
            TopicInfo::new(
                "erc20.approval",
                vec![
                    "token".to_string(),
                    "owner".to_string(),
                    "spender".to_string(),
                    "account".to_string(),
                ],
                "ERC20 token approvals. Use 'account' filter for owner OR spender matching.",
            ),
            TopicInfo::new(
                "erc20.metadata",
                vec!["token".to_string()],
                "ERC20 token metadata updates (registered/updated token attributes).",
            ),
        ]
    }

    fn build_routes(&self) -> Router {
        // No custom HTTP routes for now
        Router::new()
    }
}

impl Erc20Sink {
    /// Enable background metadata commands.
    pub fn with_metadata_pipeline(
        mut self,
        _parallelism: usize,
        _queue_capacity: usize,
        _max_retries: u8,
    ) -> Self {
        self.metadata_commands_enabled = true;
        self
    }
}
