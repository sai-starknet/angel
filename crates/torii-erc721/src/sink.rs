//! ERC721 sink for processing NFT transfers, approvals, and ownership

use crate::conversions::{core_to_primitive_u256, core_to_raw_felt, u256_to_bytes};
use crate::decoder::{Erc721Body, Erc721Msg, ERC721_TYPE};
use crate::grpc_service::Erc721Service;
use crate::handlers::{FetchErc721MetadataCommand, RefreshErc721TokenUriCommand};
use crate::proto;
use crate::storage::{Erc721Storage, NftTransferData, OperatorApprovalData};
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use prost::Message;
use prost_types::Any;
use starknet::core::types::{Felt, U256};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use torii::command::CommandBusSender;
use torii::etl::sink::{EventBus, TopicInfo};
use torii::etl::{Envelope, ExtractionBatch, Sink, TypeId};
use torii::grpc::UpdateType;
use torii_common::{TokenStandard, TokenUriRequest, TokenUriSender};

/// Default threshold for "live" detection: 100 blocks from chain head.
/// Events from blocks older than this won't be broadcast to real-time subscribers.
const LIVE_THRESHOLD_BLOCKS: u64 = 100;

/// ERC721 NFT sink
///
/// Processes ERC721 Transfer, Approval, and ApprovalForAll events:
/// - Stores transfer records and tracks ownership in the database
/// - Publishes events via EventBus for real-time subscriptions (only when live)
/// - Broadcasts events via gRPC service for rich subscriptions (only when live)
///
/// During historical indexing (more than 100 blocks from chain head), events are
/// stored but not broadcast to avoid overwhelming real-time subscribers.
pub struct Erc721Sink {
    storage: Arc<Erc721Storage>,
    event_bus: Option<Arc<EventBus>>,
    grpc_service: Option<Erc721Service>,
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
}

impl Erc721Sink {
    pub fn new(storage: Arc<Erc721Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
            grpc_service: None,
            metadata_commands_enabled: false,
            token_uri_commands_enabled: false,
            command_bus: None,
            token_uri_sender: None,
            pending_metadata_commands: tokio::sync::Mutex::new(HashSet::new()),
            pending_token_uri_commands: tokio::sync::Mutex::new(HashSet::new()),
            // Avoid startup full-table COUNT(*) scans on large datasets.
            total_transfers: AtomicU64::new(0),
            total_operator_approvals: AtomicU64::new(0),
        }
    }

    /// Set the gRPC service for dual publishing
    pub fn with_grpc_service(mut self, service: Erc721Service) -> Self {
        self.grpc_service = Some(service);
        self
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &Arc<Erc721Storage> {
        &self.storage
    }

    /// Filter function for ERC721 transfer events
    fn matches_transfer_filters(
        transfer: &proto::NftTransfer,
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

    fn enqueue_token_uri_request(&self, contract: Felt, token_id: U256) -> bool {
        if let Some(sender) = &self.token_uri_sender {
            return sender.request_update(TokenUriRequest {
                contract: core_to_raw_felt(contract),
                token_id: core_to_primitive_u256(token_id),
                standard: TokenStandard::Erc721,
            });
        }

        if let Some(command_bus) = &self.command_bus {
            if let Err(error) =
                command_bus.dispatch(RefreshErc721TokenUriCommand { contract, token_id })
            {
                tracing::warn!(
                    target: "torii_erc721::sink",
                    error = %error,
                    "Failed to dispatch ERC721 token URI refresh"
                );
                return false;
            }
            return true;
        }

        false
    }

    async fn enqueue_token_uri_range_refresh(
        &self,
        contract: Felt,
        from_token_id: U256,
        to_token_id: U256,
    ) {
        let Ok(uris) = self.storage.get_token_uris_by_contract(contract).await else {
            return;
        };
        let token_ids = uris
            .into_iter()
            .map(|(token_id, _, _)| token_id)
            .filter(|token_id| *token_id >= from_token_id && *token_id <= to_token_id)
            .collect::<Vec<_>>();

        if let Some(sender) = &self.token_uri_sender {
            let primitive_token_ids = token_ids
                .iter()
                .copied()
                .map(core_to_primitive_u256)
                .collect::<Vec<_>>();
            let accepted = sender.request_batch(
                core_to_raw_felt(contract),
                &primitive_token_ids,
                TokenStandard::Erc721,
            );
            if accepted != token_ids.len() {
                tracing::warn!(
                    target: "torii_erc721::sink",
                    contract = %format!("{:#x}", contract),
                    requested = token_ids.len(),
                    accepted,
                    "Failed to enqueue some ERC721 batch token URI refreshes"
                );
            }
            return;
        }

        for token_id in token_ids {
            self.enqueue_token_uri_request(contract, token_id);
        }
    }
}

#[async_trait]
impl Sink for Erc721Sink {
    fn name(&self) -> &'static str {
        "erc721"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![ERC721_TYPE]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        context: &torii::etl::sink::SinkContext,
    ) -> Result<()> {
        self.event_bus = Some(event_bus);
        self.command_bus = Some(context.command_bus.clone());
        tracing::info!(target: "torii_erc721::sink", "ERC721 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        let mut transfers: Vec<NftTransferData> = Vec::with_capacity(envelopes.len());
        let mut operator_approvals: Vec<OperatorApprovalData> = Vec::with_capacity(envelopes.len());
        let mut inserted_transfers: u64 = 0;
        let mut inserted_operator_approvals: u64 = 0;

        // Get block timestamps from batch
        let block_timestamps: HashMap<u64, i64> = batch
            .blocks
            .iter()
            .map(|(num, block)| (*num, block.timestamp as i64))
            .collect();

        for envelope in envelopes {
            if envelope.type_id != ERC721_TYPE {
                continue;
            }

            let Some(body) = envelope.body.as_any().downcast_ref::<Erc721Body>() else {
                continue;
            };

            match &body.msg {
                Erc721Msg::Transfer(transfer) => {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(NftTransferData {
                        id: None,
                        token: transfer.token,
                        token_id: transfer.token_id,
                        from: transfer.from,
                        to: transfer.to,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
                Erc721Msg::ApprovalForAll(approval) => {
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
                Erc721Msg::MetadataUpdate(update) => {
                    if self.token_uri_commands_enabled {
                        self.enqueue_token_uri_request(update.token, update.token_id);
                    }
                }
                Erc721Msg::BatchMetadataUpdate(update) => {
                    if self.token_uri_commands_enabled {
                        self.enqueue_token_uri_range_refresh(
                            update.token,
                            update.from_token_id,
                            update.to_token_id,
                        )
                        .await;
                    }
                }
                Erc721Msg::Approval(_) => {
                    // Single-token approvals are decoded for completeness but not stored yet.
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
                                command_bus.dispatch(FetchErc721MetadataCommand { token })
                            {
                                pending.remove(&token);
                                tracing::warn!(
                                    target: "torii_erc721::sink",
                                    token = %format!("{:#x}", token),
                                    error = %error,
                                    "Failed to dispatch ERC721 metadata command"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "torii_erc721::sink",
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
                        target: "torii_erc721::sink",
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
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc721::sink",
                        count = transfers.len(),
                        error = %e,
                        "Failed to batch insert transfers"
                    );
                    return Err(e);
                }
            };

            if transfer_count > 0 {
                inserted_transfers = transfer_count as u64;
                self.total_transfers
                    .fetch_add(inserted_transfers, Ordering::Relaxed);

                tracing::info!(
                    target: "torii_erc721::sink",
                    count = transfer_count,
                    "Batch inserted NFT transfers"
                );

                // Only broadcast to real-time subscribers when near chain head
                let is_live = batch.is_live(LIVE_THRESHOLD_BLOCKS);
                if is_live {
                    // Publish transfer events
                    for transfer in &transfers {
                        let proto_transfer = proto::NftTransfer {
                            token: transfer.token.to_bytes_be().to_vec(),
                            token_id: u256_to_bytes(transfer.token_id),
                            from: transfer.from.to_bytes_be().to_vec(),
                            to: transfer.to.to_bytes_be().to_vec(),
                            block_number: transfer.block_number,
                            tx_hash: transfer.tx_hash.to_bytes_be().to_vec(),
                            timestamp: transfer.timestamp.unwrap_or(0),
                        };

                        // Publish to EventBus
                        if let Some(event_bus) = &self.event_bus {
                            let mut buf = Vec::new();
                            proto_transfer.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc721.NftTransfer"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc721.transfer",
                                "erc721.transfer",
                                &any,
                                &proto_transfer,
                                UpdateType::Created,
                                Self::matches_transfer_filters,
                            );
                        }

                        // Broadcast to gRPC service
                        if let Some(grpc_service) = &self.grpc_service {
                            grpc_service.broadcast_transfer(proto_transfer);
                        }
                    }
                }
            }
        }

        // Batch insert operator approvals
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
                        target: "torii_erc721::sink",
                        count = count,
                        "Batch inserted operator approvals"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc721::sink",
                        "Failed to batch insert {} operator approvals: {}",
                        operator_approvals.len(),
                        e
                    );
                    return Err(e);
                }
            }
        }

        // Log combined statistics without full-table scans.
        if inserted_transfers > 0 || inserted_operator_approvals > 0 {
            tracing::info!(
                target: "torii_erc721::sink",
                batch_transfers = inserted_transfers,
                batch_operator_approvals = inserted_operator_approvals,
                total_transfers = self.total_transfers.load(Ordering::Relaxed),
                total_operator_approvals = self.total_operator_approvals.load(Ordering::Relaxed),
                blocks = batch.blocks.len(),
                "Total statistics"
            );
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![
            TopicInfo::new(
                "erc721.transfer",
                vec![
                    "token".to_string(),
                    "from".to_string(),
                    "to".to_string(),
                    "wallet".to_string(),
                ],
                "ERC721 NFT transfers. Use 'wallet' filter for from OR to matching.",
            ),
            TopicInfo::new(
                "erc721.metadata",
                vec!["token".to_string()],
                "ERC721 token metadata updates (registered/updated token attributes).",
            ),
        ]
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }
}

impl Erc721Sink {
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
