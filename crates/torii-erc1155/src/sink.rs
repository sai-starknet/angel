//! ERC1155 sink for processing token transfers and operator approvals
//!
//! This sink:
//! - Stores transfer and approval records in the database
//! - Tracks balances with automatic inconsistency detection
//! - Publishes events via EventBus for real-time subscriptions
//! - Broadcasts events via gRPC service for rich subscriptions
//!
//! Balance tracking uses a "fetch-on-inconsistency" approach:
//! - Computes balances from transfer events
//! - When a balance would go negative (genesis allocation, airdrop, etc.),
//!   fetches the actual balance from the chain and adjusts

use crate::balance_fetcher::Erc1155BalanceFetcher;
use crate::decoder::{Erc1155Body as DecodedErc1155Body, Erc1155Message, ERC1155_TYPE_ID};
use crate::grpc_service::Erc1155Service;
use crate::handlers::{FetchErc1155MetadataCommand, RefreshErc1155TokenUriCommand};
use crate::proto;
use crate::storage::{Erc1155Storage, OperatorApprovalData, TokenTransferData, TokenUriData};
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
use torii_common::{u256_to_bytes, TokenStandard, TokenUriRequest, TokenUriSender};

/// Default threshold for "live" detection: 100 blocks from chain head.
/// Events from blocks older than this won't be broadcast to real-time subscribers.
const LIVE_THRESHOLD_BLOCKS: u64 = 100;

type SinkResult<T> = std::result::Result<T, Erc1155SinkError>;

#[derive(Debug, thiserror::Error)]
pub enum Erc1155SinkError {
    #[error("failed to batch insert transfers")]
    InsertTransfers(#[source] anyhow::Error),
    #[error("failed to batch insert operator approvals")]
    InsertOperatorApprovals(#[source] anyhow::Error),
    #[error("failed to batch upsert token URI updates")]
    UpsertTokenUriUpdates(#[source] anyhow::Error),
    #[error("failed to encode ERC1155 protobuf payload")]
    Encode(#[from] prost::EncodeError),
}

/// ERC1155 token sink
///
/// Processes ERC1155 TransferSingle, TransferBatch, and ApprovalForAll events:
/// - Stores transfer records in the database
/// - Tracks balances with automatic inconsistency detection
/// - Publishes events via EventBus for real-time subscriptions (only when live)
/// - Broadcasts events via gRPC service for rich subscriptions (only when live)
///
/// During historical indexing (more than 100 blocks from chain head), events are
/// stored but not broadcast to avoid overwhelming real-time subscribers.
pub struct Erc1155Sink {
    storage: Arc<Erc1155Storage>,
    event_bus: Option<Arc<EventBus>>,
    grpc_service: Option<Erc1155Service>,
    /// Balance fetcher for RPC calls (None = balance tracking disabled)
    balance_fetcher: Option<Arc<Erc1155BalanceFetcher>>,
    /// Whether contract metadata commands should be dispatched.
    metadata_commands_enabled: bool,
    /// Whether token URI commands should be dispatched.
    token_uri_commands_enabled: bool,
    /// Command bus sender for background metadata and token URI work.
    command_bus: Option<CommandBusSender>,
    token_uri_sender: Option<TokenUriSender>,
    /// Commands already queued but not yet observed in storage.
    pending_metadata_commands: tokio::sync::Mutex<HashSet<Felt>>,
    pending_token_uri_commands: tokio::sync::Mutex<HashSet<(Felt, U256)>>,
    /// In-memory counters to avoid full-table COUNT(*) in the ingest hot path.
    total_transfers: AtomicU64,
    total_operator_approvals: AtomicU64,
    total_uri_updates: AtomicU64,
}

impl Erc1155Sink {
    pub fn new(storage: Arc<Erc1155Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
            grpc_service: None,
            balance_fetcher: None,
            metadata_commands_enabled: false,
            token_uri_commands_enabled: false,
            command_bus: None,
            token_uri_sender: None,
            pending_metadata_commands: tokio::sync::Mutex::new(HashSet::new()),
            pending_token_uri_commands: tokio::sync::Mutex::new(HashSet::new()),
            // Avoid startup full-table COUNT(*) scans on large datasets.
            total_transfers: AtomicU64::new(0),
            total_operator_approvals: AtomicU64::new(0),
            total_uri_updates: AtomicU64::new(0),
        }
    }

    /// Set the gRPC service for dual publishing
    pub fn with_grpc_service(mut self, service: Erc1155Service) -> Self {
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
        self.balance_fetcher = Some(Arc::new(Erc1155BalanceFetcher::new(provider)));
        self
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &Arc<Erc1155Storage> {
        &self.storage
    }

    /// Filter function for ERC1155 transfer events
    fn matches_transfer_filters(
        transfer: &proto::TokenTransfer,
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

    /// Filter function for ERC1155 URI updates.
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    /// - "token_id": Filter by token id (hex string)
    fn matches_uri_filters(uri: &proto::TokenUri, filters: &HashMap<String, String>) -> bool {
        if filters.is_empty() {
            return true;
        }

        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&uri.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        if let Some(token_id_filter) = filters.get("token_id") {
            let token_id_hex = format!("0x{}", hex::encode(&uri.token_id));
            if !token_id_hex.eq_ignore_ascii_case(token_id_filter) {
                return false;
            }
        }

        true
    }

    fn enqueue_token_uri_request(&self, contract: Felt, token_id: U256) -> bool {
        if let Some(sender) = &self.token_uri_sender {
            return sender.request_update(TokenUriRequest {
                contract,
                token_id,
                standard: TokenStandard::Erc1155,
            });
        }

        if let Some(command_bus) = &self.command_bus {
            if let Err(error) =
                command_bus.dispatch(RefreshErc1155TokenUriCommand { contract, token_id })
            {
                tracing::warn!(
                    target: "torii_erc1155::sink",
                    error = %error,
                    "Failed to dispatch ERC1155 token URI command"
                );
                return false;
            }
            return true;
        }

        false
    }

    async fn initialize_sink(
        &mut self,
        event_bus: Arc<EventBus>,
        context: &torii::etl::sink::SinkContext,
    ) -> SinkResult<()> {
        self.event_bus = Some(event_bus);
        self.command_bus = Some(context.command_bus.clone());
        tracing::info!(target: "torii_erc1155::sink", "ERC1155 sink initialized");
        Ok(())
    }

    async fn process_sink(
        &self,
        envelopes: &[Envelope],
        batch: &ExtractionBatch,
    ) -> SinkResult<()> {
        let mut transfers: Vec<TokenTransferData> = Vec::with_capacity(envelopes.len());
        let mut operator_approvals: Vec<OperatorApprovalData> = Vec::with_capacity(envelopes.len());
        let mut uri_updates: Vec<TokenUriData> = Vec::with_capacity(envelopes.len());
        let mut inserted_transfers: u64 = 0;
        let mut inserted_operator_approvals: u64 = 0;
        let mut inserted_uri_updates: u64 = 0;

        // Get block timestamps from batch
        let block_timestamps: HashMap<u64, i64> = batch
            .blocks
            .iter()
            .map(|(num, block)| (*num, block.timestamp as i64))
            .collect();

        for envelope in envelopes {
            if envelope.type_id != ERC1155_TYPE_ID {
                continue;
            }

            if let Some(body) = envelope.body.as_any().downcast_ref::<DecodedErc1155Body>() {
                match &body.msg {
                    Erc1155Message::Transfer(transfer) => {
                        let timestamp = block_timestamps.get(&transfer.block_number).copied();
                        transfers.push(TokenTransferData {
                            id: None,
                            token: transfer.token,
                            operator: transfer.operator,
                            from: transfer.from,
                            to: transfer.to,
                            token_id: transfer.token_id,
                            amount: transfer.amount,
                            is_batch: transfer.is_batch,
                            batch_index: transfer.batch_index,
                            block_number: transfer.block_number,
                            tx_hash: transfer.transaction_hash,
                            timestamp,
                        });
                    }
                    Erc1155Message::ApprovalForAll(approval) => {
                        let timestamp = block_timestamps.get(&approval.block_number).copied();
                        operator_approvals.push(OperatorApprovalData {
                            id: None,
                            token: approval.token,
                            owner: approval.owner,
                            operator: approval.operator,
                            approved: approval.approved,
                            block_number: approval.block_number,
                            tx_hash: approval.transaction_hash,
                            timestamp,
                        });
                    }
                    Erc1155Message::Uri(uri) => {
                        let timestamp = block_timestamps.get(&uri.block_number).copied();
                        uri_updates.push(TokenUriData {
                            token: uri.token,
                            token_id: uri.token_id,
                            uri: uri.uri.clone(),
                            block_number: uri.block_number,
                            tx_hash: uri.transaction_hash,
                            timestamp,
                        });
                    }
                }
            }
        }

        // Fetch metadata for any new token contracts.
        if self.metadata_commands_enabled {
            if let Some(ref command_bus) = self.command_bus {
                let candidate_tokens = transfers
                    .iter()
                    .map(|transfer| transfer.token)
                    .collect::<HashSet<_>>()
                    .into_iter();
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
                    Ok(existing_tokens) => {
                        let mut pending = self.pending_metadata_commands.lock().await;
                        for token in &existing_tokens {
                            pending.remove(token);
                        }

                        for token in unchecked_tokens
                            .into_iter()
                            .filter(|token| !existing_tokens.contains(token))
                        {
                            if !pending.insert(token) {
                                continue;
                            }
                            if let Err(error) =
                                command_bus.dispatch(FetchErc1155MetadataCommand { token })
                            {
                                pending.remove(&token);
                                tracing::warn!(
                                    target: "torii_erc1155::sink",
                                    token = %format!("{:#x}", token),
                                    error = %error,
                                    "Failed to dispatch ERC1155 metadata command"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "torii_erc1155::sink",
                            error = %error,
                            "Failed to batch-check token metadata"
                        );
                    }
                }
            }
        }

        // Request token URI fetches for new token IDs.
        if self.token_uri_commands_enabled
            && (self.token_uri_sender.is_some() || self.command_bus.is_some())
        {
            let candidate_tokens = transfers
                .iter()
                .map(|transfer| (transfer.token, transfer.token_id))
                .collect::<HashSet<_>>()
                .into_iter();
            let unchecked_tokens = {
                let pending = self.pending_token_uri_commands.lock().await;
                candidate_tokens
                    .filter(|token| !pending.contains(token))
                    .collect::<Vec<_>>()
            };

            match self.storage.has_token_uri_batch(&unchecked_tokens).await {
                Ok(existing_tokens) => {
                    let mut pending = self.pending_token_uri_commands.lock().await;
                    for token in &existing_tokens {
                        pending.remove(token);
                    }

                    for (contract, token_id) in unchecked_tokens
                        .into_iter()
                        .filter(|token| !existing_tokens.contains(token))
                    {
                        if !pending.insert((contract, token_id)) {
                            continue;
                        }
                        if !self.enqueue_token_uri_request(contract, token_id) {
                            pending.remove(&(contract, token_id));
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        target: "torii_erc1155::sink",
                        error = %error,
                        "Failed to batch-check token URI existence"
                    );
                }
            }
        }

        // Batch insert transfers
        if !transfers.is_empty() {
            let transfer_count = match self.storage.insert_transfers_batch(&transfers).await {
                Ok(count) => count,
                Err(error) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        count = transfers.len(),
                        error = %error,
                        "Failed to batch insert transfers"
                    );
                    return Err(Erc1155SinkError::InsertTransfers(error));
                }
            };

            if transfer_count > 0 {
                inserted_transfers = transfer_count as u64;
                self.total_transfers
                    .fetch_add(inserted_transfers, Ordering::Relaxed);

                tracing::info!(
                    target: "torii_erc1155::sink",
                    count = transfer_count,
                    "Batch inserted token transfers"
                );

                if let Some(ref fetcher) = self.balance_fetcher {
                    let adjustment_requests = match self
                        .storage
                        .check_balances_batch(&transfers)
                        .await
                    {
                        Ok(requests) => requests,
                        Err(e) => {
                            tracing::warn!(
                                target: "torii_erc1155::sink",
                                error = %e,
                                "Failed to check balance inconsistencies, skipping balance tracking"
                            );
                            Vec::new()
                        }
                    };

                    let mut adjustments: HashMap<(Felt, Felt, U256), U256> = HashMap::new();
                    if !adjustment_requests.is_empty() {
                        tracing::info!(
                            target: "torii_erc1155::sink",
                            count = adjustment_requests.len(),
                            "Fetching balance adjustments from RPC"
                        );

                        match fetcher.fetch_balances_batch(&adjustment_requests).await {
                            Ok(fetched) => {
                                for (contract, wallet, token_id, balance) in fetched {
                                    adjustments.insert((contract, wallet, token_id), balance);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "torii_erc1155::sink",
                                    error = %e,
                                    "Failed to fetch balances from RPC, using 0 for adjustments"
                                );
                                for req in &adjustment_requests {
                                    adjustments.insert(
                                        (req.contract, req.wallet, req.token_id),
                                        U256::from(0u64),
                                    );
                                }
                            }
                        }
                    }

                    if let Err(e) = self
                        .storage
                        .apply_transfers_with_adjustments(&transfers, &adjustments)
                        .await
                    {
                        tracing::error!(
                            target: "torii_erc1155::sink",
                            error = %e,
                            "Failed to apply balance updates"
                        );
                    }
                }

                let is_live = batch.is_live(LIVE_THRESHOLD_BLOCKS);
                if is_live {
                    for transfer in &transfers {
                        let proto_transfer = proto::TokenTransfer {
                            token: transfer.token.to_be_bytes_vec(),
                            operator: transfer.operator.to_be_bytes_vec(),
                            from: transfer.from.to_be_bytes_vec(),
                            to: transfer.to.to_be_bytes_vec(),
                            token_id: u256_to_bytes(transfer.token_id),
                            amount: u256_to_bytes(transfer.amount),
                            block_number: transfer.block_number,
                            tx_hash: transfer.tx_hash.to_be_bytes_vec(),
                            timestamp: transfer.timestamp.unwrap_or(0),
                            is_batch: transfer.is_batch,
                            batch_index: transfer.batch_index,
                        };

                        if let Some(event_bus) = &self.event_bus {
                            let mut buf = Vec::new();
                            proto_transfer.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc1155.TokenTransfer"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc1155.transfer",
                                "erc1155.transfer",
                                &any,
                                &proto_transfer,
                                UpdateType::Created,
                                Self::matches_transfer_filters,
                            );
                        }

                        if let Some(grpc_service) = &self.grpc_service {
                            grpc_service.broadcast_transfer(proto_transfer);
                        }
                    }
                }
            }
        }

        if !operator_approvals.is_empty() {
            match self
                .storage
                .insert_operator_approvals_batch(&operator_approvals)
                .await
            {
                Ok(count) => {
                    inserted_operator_approvals = count as u64;
                    self.total_operator_approvals
                        .fetch_add(inserted_operator_approvals, Ordering::Relaxed);

                    tracing::info!(
                        target: "torii_erc1155::sink",
                        count = count,
                        "Batch inserted operator approvals"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        count = operator_approvals.len(),
                        error = %error,
                        "Failed to batch insert operator approvals"
                    );
                    return Err(Erc1155SinkError::InsertOperatorApprovals(error));
                }
            }
        }

        if !uri_updates.is_empty() {
            match self.storage.upsert_token_uris_batch(&uri_updates).await {
                Ok(count) => {
                    inserted_uri_updates = count as u64;
                    self.total_uri_updates
                        .fetch_add(inserted_uri_updates, Ordering::Relaxed);

                    tracing::info!(
                        target: "torii_erc1155::sink",
                        count = count,
                        "Batch upserted token URI updates"
                    );

                    if let Some(event_bus) = &self.event_bus {
                        for uri in &uri_updates {
                            let proto_uri = proto::TokenUri {
                                token: uri.token.to_be_bytes_vec(),
                                token_id: u256_to_bytes(uri.token_id),
                                uri: uri.uri.clone(),
                                block_number: uri.block_number,
                            };

                            let mut buf = Vec::new();
                            proto_uri.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc1155.TokenUri"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc1155.uri",
                                "erc1155.uri",
                                &any,
                                &proto_uri,
                                UpdateType::Updated,
                                Self::matches_uri_filters,
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        count = uri_updates.len(),
                        error = %error,
                        "Failed to batch upsert token URI updates"
                    );
                    return Err(Erc1155SinkError::UpsertTokenUriUpdates(error));
                }
            }
        }

        if inserted_transfers > 0 || inserted_operator_approvals > 0 || inserted_uri_updates > 0 {
            tracing::info!(
                target: "torii_erc1155::sink",
                batch_transfers = inserted_transfers,
                batch_operator_approvals = inserted_operator_approvals,
                batch_uri_updates = inserted_uri_updates,
                total_transfers = self.total_transfers.load(Ordering::Relaxed),
                total_operator_approvals = self.total_operator_approvals.load(Ordering::Relaxed),
                total_uri_updates = self.total_uri_updates.load(Ordering::Relaxed),
                blocks = batch.blocks.len(),
                "Total statistics"
            );
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for Erc1155Sink {
    fn name(&self) -> &'static str {
        "erc1155"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![ERC1155_TYPE_ID]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        context: &torii::etl::sink::SinkContext,
    ) -> anyhow::Result<()> {
        self.initialize_sink(event_bus, context)
            .await
            .map_err(Into::into)
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> anyhow::Result<()> {
        self.process_sink(envelopes, batch)
            .await
            .map_err(Into::into)
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![
            TopicInfo::new(
                "erc1155.transfer",
                vec![
                    "token".to_string(),
                    "from".to_string(),
                    "to".to_string(),
                    "wallet".to_string(),
                ],
                "ERC1155 token transfers. Use 'wallet' filter for from OR to matching.",
            ),
            TopicInfo::new(
                "erc1155.metadata",
                vec!["token".to_string()],
                "ERC1155 token metadata updates (registered/updated token attributes).",
            ),
            TopicInfo::new(
                "erc1155.uri",
                vec!["token".to_string(), "token_id".to_string()],
                "ERC1155 token URI updates (registered/updated token attributes).",
            ),
        ]
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }
}

impl Erc1155Sink {
    pub fn with_metadata_commands(mut self) -> Self {
        self.metadata_commands_enabled = true;
        self
    }

    pub fn with_metadata_fetching(self, _provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        self.with_metadata_commands()
    }

    pub fn with_token_uri_commands(mut self) -> Self {
        self.token_uri_commands_enabled = true;
        self
    }

    pub fn with_token_uri_sender(mut self, sender: TokenUriSender) -> Self {
        self.token_uri_commands_enabled = true;
        self.token_uri_sender = Some(sender);
        self
    }
}
