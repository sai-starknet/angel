//! gRPC service implementation for ERC20 queries and subscriptions
//!
//! Provides:
//! - Historical queries with filtering and pagination (GetTransfers, GetApprovals)
//! - Real-time subscriptions with filtering (SubscribeTransfers, SubscribeApprovals)
//! - Indexer statistics (GetStats)

use crate::proto::erc20_server::Erc20 as Erc20Trait;
use crate::proto::{
    Approval, ApprovalFilter, ApprovalUpdate, BalanceEntry, Cursor, GetApprovalsRequest,
    GetApprovalsResponse, GetBalanceRequest, GetBalanceResponse, GetBalancesRequest,
    GetBalancesResponse, GetStatsRequest, GetStatsResponse, GetTokenMetadataRequest,
    GetTokenMetadataResponse, GetTransfersRequest, GetTransfersResponse, SubscribeApprovalsRequest,
    SubscribeTransfersRequest, TokenMetadataEntry, Transfer, TransferFilter, TransferUpdate,
};
use crate::storage::{
    ApprovalCursor, ApprovalData, Erc20Storage, TransferCursor, TransferData, TransferDirection,
};
use async_trait::async_trait;
use futures::stream::Stream;
use primitive_types::U256;
use starknet_types_raw::Felt;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use torii_common::u256_to_bytes;

/// gRPC service implementation for ERC20
#[derive(Clone)]
pub struct Erc20Service {
    storage: Arc<Erc20Storage>,
    /// Broadcast channel for real-time transfer updates
    pub transfer_tx: broadcast::Sender<TransferUpdate>,
    /// Broadcast channel for real-time approval updates
    pub approval_tx: broadcast::Sender<ApprovalUpdate>,
}

impl Erc20Service {
    /// Creates a new Erc20Service
    pub fn new(storage: Arc<Erc20Storage>) -> Self {
        // Create broadcast channels with capacity for 1000 pending updates
        let (transfer_tx, _) = broadcast::channel(1000);
        let (approval_tx, _) = broadcast::channel(1000);

        Self {
            storage,
            transfer_tx,
            approval_tx,
        }
    }

    /// Broadcasts a transfer to all subscribers
    pub fn broadcast_transfer(&self, transfer: Transfer) {
        let update = TransferUpdate {
            transfer: Some(transfer),
            timestamp: chrono::Utc::now().timestamp(),
        };
        // Send to all subscribers (ignore if no receivers)
        let _ = self.transfer_tx.send(update);
    }

    /// Broadcasts an approval to all subscribers
    pub fn broadcast_approval(&self, approval: Approval) {
        let update = ApprovalUpdate {
            approval: Some(approval),
            timestamp: chrono::Utc::now().timestamp(),
        };
        // Send to all subscribers (ignore if no receivers)
        let _ = self.approval_tx.send(update);
    }

    /// Convert storage TransferData to proto Transfer
    fn transfer_data_to_proto(data: &TransferData) -> Transfer {
        Transfer {
            token: data.token.to_be_bytes_vec(),
            from: data.from.to_be_bytes_vec(),
            to: data.to.to_be_bytes_vec(),
            amount: u256_to_bytes(data.amount),
            block_number: data.block_number,
            tx_hash: data.tx_hash.to_be_bytes_vec(),
            timestamp: data.timestamp.unwrap_or(0),
        }
    }

    /// Convert storage ApprovalData to proto Approval
    fn approval_data_to_proto(data: &ApprovalData) -> Approval {
        Approval {
            token: data.token.to_be_bytes_vec(),
            owner: data.owner.to_be_bytes_vec(),
            spender: data.spender.to_be_bytes_vec(),
            amount: u256_to_bytes(data.amount),
            block_number: data.block_number,
            tx_hash: data.tx_hash.to_be_bytes_vec(),
            timestamp: data.timestamp.unwrap_or(0),
        }
    }

    /// Check if a transfer matches a filter (for subscriptions)
    fn matches_transfer_filter(transfer: &Transfer, filter: &TransferFilter) -> bool {
        // Wallet filter (OR logic: matches from OR to)
        if let Some(ref wallet) = filter.wallet {
            let matches_from = transfer.from == *wallet;
            let matches_to = transfer.to == *wallet;

            if !matches_from && !matches_to {
                return false;
            }

            // Apply direction filter when wallet is set
            match crate::proto::TransferDirection::try_from(filter.direction) {
                Ok(crate::proto::TransferDirection::DirectionSent) => {
                    if !matches_from {
                        return false;
                    }
                }
                Ok(crate::proto::TransferDirection::DirectionReceived) => {
                    if !matches_to {
                        return false;
                    }
                }
                _ => {} // All or unknown - no additional filtering
            }
        }

        // Exact from filter
        if let Some(ref from) = filter.from {
            if transfer.from != *from {
                return false;
            }
        }

        // Exact to filter
        if let Some(ref to) = filter.to {
            if transfer.to != *to {
                return false;
            }
        }

        // Token whitelist
        if !filter.tokens.is_empty() && !filter.tokens.contains(&transfer.token) {
            return false;
        }

        // Block range filters
        if let Some(block_from) = filter.block_from {
            if transfer.block_number < block_from {
                return false;
            }
        }

        if let Some(block_to) = filter.block_to {
            if transfer.block_number > block_to {
                return false;
            }
        }

        true
    }

    /// Check if an approval matches a filter (for subscriptions)
    fn matches_approval_filter(approval: &Approval, filter: &ApprovalFilter) -> bool {
        // Account filter (OR logic: matches owner OR spender)
        if let Some(ref account) = filter.account {
            if approval.owner != *account && approval.spender != *account {
                return false;
            }
        }

        // Exact owner filter
        if let Some(ref owner) = filter.owner {
            if approval.owner != *owner {
                return false;
            }
        }

        // Exact spender filter
        if let Some(ref spender) = filter.spender {
            if approval.spender != *spender {
                return false;
            }
        }

        // Token whitelist
        if !filter.tokens.is_empty() && !filter.tokens.contains(&approval.token) {
            return false;
        }

        // Block range filters
        if let Some(block_from) = filter.block_from {
            if approval.block_number < block_from {
                return false;
            }
        }

        if let Some(block_to) = filter.block_to {
            if approval.block_number > block_to {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl Erc20Trait for Erc20Service {
    /// Query historical transfers with filtering and pagination
    async fn get_transfers(
        &self,
        request: Request<GetTransfersRequest>,
    ) -> Result<Response<GetTransfersResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        // Parse filter fields
        let wallet = filter
            .wallet
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let from = filter
            .from
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let to = filter
            .to
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| Felt::from_be_bytes_slice(b).ok())
            .collect();

        let direction = match crate::proto::TransferDirection::try_from(filter.direction) {
            Ok(crate::proto::TransferDirection::DirectionSent) => TransferDirection::Sent,
            Ok(crate::proto::TransferDirection::DirectionReceived) => TransferDirection::Received,
            _ => TransferDirection::All,
        };

        // Parse cursor
        let cursor = req.cursor.map(|c| TransferCursor {
            block_number: c.block_number,
            id: c.id,
        });

        // Apply limit (default 100, max 1000)
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        tracing::debug!(
            target: "torii_erc20::grpc",
            "GetTransfers: wallet={:?}, from={:?}, to={:?}, tokens={}, limit={}",
            wallet.map(|w| format!("{w:#x}")),
            from.map(|f| format!("{f:#x}")),
            to.map(|t| format!("{t:#x}")),
            tokens.len(),
            limit
        );

        // Execute query
        let (transfers, next_cursor) = self
            .storage
            .get_transfers_filtered(
                wallet,
                from,
                to,
                &tokens,
                direction,
                filter.block_from,
                filter.block_to,
                cursor,
                limit,
            )
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let proto_transfers: Vec<Transfer> =
            transfers.iter().map(Self::transfer_data_to_proto).collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        tracing::debug!(
            target: "torii_erc20::grpc",
            "GetTransfers: returned {} transfers, has_more={}",
            proto_transfers.len(),
            proto_cursor.is_some()
        );

        Ok(Response::new(GetTransfersResponse {
            transfers: proto_transfers,
            next_cursor: proto_cursor,
        }))
    }

    /// Query historical approvals with filtering and pagination
    async fn get_approvals(
        &self,
        request: Request<GetApprovalsRequest>,
    ) -> Result<Response<GetApprovalsResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        // Parse filter fields
        let account = filter
            .account
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let owner = filter
            .owner
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let spender = filter
            .spender
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| Felt::from_be_bytes_slice(b).ok())
            .collect();

        // Parse cursor
        let cursor = req.cursor.map(|c| ApprovalCursor {
            block_number: c.block_number,
            id: c.id,
        });

        // Apply limit (default 100, max 1000)
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        tracing::debug!(
            target: "torii_erc20::grpc",
            "GetApprovals: account={:?}, owner={:?}, spender={:?}, tokens={}, limit={}",
            account.map(|a| format!("{a:#x}")),
            owner.map(|o| format!("{o:#x}")),
            spender.map(|s| format!("{s:#x}")),
            tokens.len(),
            limit
        );

        // Execute query
        let (approvals, next_cursor) = self
            .storage
            .get_approvals_filtered(
                account,
                owner,
                spender,
                &tokens,
                filter.block_from,
                filter.block_to,
                cursor,
                limit,
            )
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let proto_approvals: Vec<Approval> =
            approvals.iter().map(Self::approval_data_to_proto).collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        tracing::debug!(
            target: "torii_erc20::grpc",
            "GetApprovals: returned {} approvals, has_more={}",
            proto_approvals.len(),
            proto_cursor.is_some()
        );

        Ok(Response::new(GetApprovalsResponse {
            approvals: proto_approvals,
            next_cursor: proto_cursor,
        }))
    }

    /// Get balance for a specific token and wallet
    async fn get_balance(
        &self,
        request: Request<GetBalanceRequest>,
    ) -> Result<Response<GetBalanceResponse>, Status> {
        let req = request.into_inner();

        let token = Felt::from_be_bytes_slice(&req.token)
            .map_err(|_| Status::invalid_argument("Invalid token address"))?;
        let wallet = Felt::from_be_bytes_slice(&req.wallet)
            .map_err(|_| Status::invalid_argument("Invalid wallet address"))?;

        tracing::debug!(
            target: "torii_erc20::grpc",
            "GetBalance: token={:#x}, wallet={:#x}",
            token,
            wallet
        );

        let (balance, last_block) = self
            .storage
            .get_balance_with_block(token, wallet)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?
            .unwrap_or((U256::zero(), 0));

        Ok(Response::new(GetBalanceResponse {
            balance: u256_to_bytes(balance),
            last_block,
        }))
    }

    /// Query balances in batch with optional token/wallet filters
    async fn get_balances(
        &self,
        request: Request<GetBalancesRequest>,
    ) -> Result<Response<GetBalancesResponse>, Status> {
        let req = request.into_inner();

        let token = req
            .token
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let wallet = req
            .wallet
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let cursor = req.cursor;
        let limit = if req.limit == 0 {
            1000
        } else {
            req.limit.min(10_000)
        };

        tracing::debug!(
            target: "torii_erc20::grpc",
            "GetBalances: token={:?}, wallet={:?}, cursor={:?}, limit={}",
            token.map(|t| format!("{t:#x}")),
            wallet.map(|w| format!("{w:#x}")),
            cursor,
            limit
        );

        let (balances, next_cursor) = self
            .storage
            .get_balances_filtered(token, wallet, cursor, limit)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let rows = balances
            .into_iter()
            .map(|b| BalanceEntry {
                token: b.token.to_be_bytes_vec(),
                wallet: b.wallet.to_be_bytes_vec(),
                balance: u256_to_bytes(b.balance),
                last_block: b.last_block,
            })
            .collect();

        Ok(Response::new(GetBalancesResponse {
            balances: rows,
            next_cursor,
        }))
    }

    /// Get token metadata (name, symbol, decimals)
    async fn get_token_metadata(
        &self,
        request: Request<GetTokenMetadataRequest>,
    ) -> Result<Response<GetTokenMetadataResponse>, Status> {
        let req = request.into_inner();

        if let Some(token_bytes) = req.token {
            let token = Felt::from_be_bytes_slice(&token_bytes)
                .map_err(|_| Status::invalid_argument("Invalid token address"))?;

            let entries = match self.storage.get_token_metadata(token).await {
                Ok(Some((name, symbol, decimals, total_supply))) => vec![TokenMetadataEntry {
                    token: token.to_be_bytes_vec(),
                    name,
                    symbol,
                    decimals: decimals.map(|d| d as u32),
                    total_supply: total_supply.map(u256_to_bytes),
                }],
                Ok(None) => vec![],
                Err(e) => return Err(Status::internal(format!("Query failed: {e}"))),
            };

            return Ok(Response::new(GetTokenMetadataResponse {
                tokens: entries,
                next_cursor: None,
            }));
        }

        let cursor = req
            .cursor
            .as_ref()
            .and_then(|b| Felt::from_be_bytes_slice(b).ok());
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let (all, next_cursor) = self
            .storage
            .get_token_metadata_paginated(cursor, limit)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let entries = all
            .into_iter()
            .map(
                |(token, name, symbol, decimals, total_supply)| TokenMetadataEntry {
                    token: token.to_be_bytes_vec(),
                    name,
                    symbol,
                    decimals: decimals.map(|d| d as u32),
                    total_supply: total_supply.map(u256_to_bytes),
                },
            )
            .collect();

        Ok(Response::new(GetTokenMetadataResponse {
            tokens: entries,
            next_cursor: next_cursor.map(|c| c.to_be_bytes_vec()),
        }))
    }

    /// Subscribe to real-time transfer events with filtering
    type SubscribeTransfersStream =
        Pin<Box<dyn Stream<Item = Result<TransferUpdate, Status>> + Send>>;

    async fn subscribe_transfers(
        &self,
        request: Request<SubscribeTransfersRequest>,
    ) -> Result<Response<Self::SubscribeTransfersStream>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        tracing::info!(
            target: "torii_erc20::grpc",
            "New transfer subscription from client: {}",
            req.client_id
        );

        let mut rx = self.transfer_tx.subscribe();

        let stream = async_stream::try_stream! {
            tracing::debug!(
                target: "torii_erc20::grpc",
                "Transfer subscription stream started for client: {}",
                req.client_id
            );

            loop {
                match rx.recv().await {
                    Ok(update) => {
                        // Apply filter
                        if let Some(ref transfer) = update.transfer {
                            if !Self::matches_transfer_filter(transfer, &filter) {
                                continue;
                            }
                        }

                        tracing::trace!(
                            target: "torii_erc20::grpc",
                            "Sending transfer update to client {}",
                            req.client_id
                        );

                        yield update;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            target: "torii_erc20::grpc",
                            "Client {} lagged, skipped {} transfer updates",
                            req.client_id,
                            skipped
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!(
                            target: "torii_erc20::grpc",
                            "Transfer broadcast channel closed for client {}",
                            req.client_id
                        );
                        break;
                    }
                }
            }

            tracing::info!(
                target: "torii_erc20::grpc",
                "Transfer subscription stream ended for client: {}",
                req.client_id
            );
        };

        Ok(Response::new(Box::pin(stream)))
    }

    /// Subscribe to real-time approval events with filtering
    type SubscribeApprovalsStream =
        Pin<Box<dyn Stream<Item = Result<ApprovalUpdate, Status>> + Send>>;

    async fn subscribe_approvals(
        &self,
        request: Request<SubscribeApprovalsRequest>,
    ) -> Result<Response<Self::SubscribeApprovalsStream>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        tracing::info!(
            target: "torii_erc20::grpc",
            "New approval subscription from client: {}",
            req.client_id
        );

        let mut rx = self.approval_tx.subscribe();

        let stream = async_stream::try_stream! {
            tracing::debug!(
                target: "torii_erc20::grpc",
                "Approval subscription stream started for client: {}",
                req.client_id
            );

            loop {
                match rx.recv().await {
                    Ok(update) => {
                        // Apply filter
                        if let Some(ref approval) = update.approval {
                            if !Self::matches_approval_filter(approval, &filter) {
                                continue;
                            }
                        }

                        tracing::trace!(
                            target: "torii_erc20::grpc",
                            "Sending approval update to client {}",
                            req.client_id
                        );

                        yield update;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            target: "torii_erc20::grpc",
                            "Client {} lagged, skipped {} approval updates",
                            req.client_id,
                            skipped
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!(
                            target: "torii_erc20::grpc",
                            "Approval broadcast channel closed for client {}",
                            req.client_id
                        );
                        break;
                    }
                }
            }

            tracing::info!(
                target: "torii_erc20::grpc",
                "Approval subscription stream ended for client: {}",
                req.client_id
            );
        };

        Ok(Response::new(Box::pin(stream)))
    }

    /// Get indexer statistics
    async fn get_stats(
        &self,
        _request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let total_transfers = self
            .storage
            .get_transfer_count()
            .await
            .map_err(|e| Status::internal(format!("Failed to get transfer count: {e}")))?;

        let total_approvals = self
            .storage
            .get_approval_count()
            .await
            .map_err(|e| Status::internal(format!("Failed to get approval count: {e}")))?;

        let unique_tokens = self
            .storage
            .get_token_count()
            .await
            .map_err(|e| Status::internal(format!("Failed to get token count: {e}")))?;

        let latest_block = self
            .storage
            .get_latest_block()
            .await
            .map_err(|e| Status::internal(format!("Failed to get latest block: {e}")))?
            .unwrap_or(0);

        tracing::debug!(
            target: "torii_erc20::grpc",
            "GetStats: transfers={}, approvals={}, tokens={}, latest_block={}",
            total_transfers,
            total_approvals,
            unique_tokens,
            latest_block
        );

        Ok(Response::new(GetStatsResponse {
            total_transfers,
            total_approvals,
            unique_tokens,
            latest_block,
        }))
    }
}
