//! gRPC service implementation for ERC721 queries and subscriptions

use crate::conversions::{bytes_to_u256, u256_to_bytes};
use crate::proto::{
    erc721_server::Erc721 as Erc721Trait, AttributeFacetCount, CollectionToken,
    ContractCollectionOverview, Cursor, GetCollectionOverviewRequest,
    GetCollectionOverviewResponse, GetCollectionTokensRequest, GetCollectionTokensResponse,
    GetCollectionTraitFacetsRequest, GetCollectionTraitFacetsResponse, GetOwnerRequest,
    GetOwnerResponse, GetOwnershipRequest, GetOwnershipResponse, GetStatsRequest, GetStatsResponse,
    GetTokenMetadataRequest, GetTokenMetadataResponse, GetTransfersRequest, GetTransfersResponse,
    NftTransfer, Ownership, QueryTokensByAttributesRequest, QueryTokensByAttributesResponse,
    SubscribeTransfersRequest, TokenMetadataEntry, TraitSummary, TransferFilter, TransferUpdate,
};
use crate::storage::{Erc721Storage, NftTransferData, TransferCursor};
use async_trait::async_trait;
use futures::stream::Stream;
use starknet::core::types::Felt;
use starknet::core::types::U256;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

fn bytes_to_felt(bytes: &[u8]) -> Option<Felt> {
    if bytes.len() > 32 {
        return None;
    }

    let mut padded = [0u8; 32];
    padded[32 - bytes.len()..].copy_from_slice(bytes);
    Some(Felt::from_bytes_be(&padded))
}

const DEFAULT_PROJECT_ID: &str = "arcade-main";

/// gRPC service implementation for ERC721
#[derive(Clone)]
pub struct Erc721Service {
    storage: Arc<Erc721Storage>,
    /// Broadcast channel for real-time transfer updates
    pub transfer_tx: broadcast::Sender<TransferUpdate>,
}

impl Erc721Service {
    /// Creates a new Erc721Service
    pub fn new(storage: Arc<Erc721Storage>) -> Self {
        let (transfer_tx, _) = broadcast::channel(1000);

        Self {
            storage,
            transfer_tx,
        }
    }

    /// Broadcasts a transfer to all subscribers
    pub fn broadcast_transfer(&self, transfer: NftTransfer) {
        let update = TransferUpdate {
            transfer: Some(transfer),
            timestamp: chrono::Utc::now().timestamp(),
        };
        let _ = self.transfer_tx.send(update);
    }

    /// Convert storage NftTransferData to proto NftTransfer
    fn transfer_data_to_proto(data: &NftTransferData) -> NftTransfer {
        NftTransfer {
            token: data.token.to_bytes_be().to_vec(),
            token_id: u256_to_bytes(data.token_id),
            from: data.from.to_bytes_be().to_vec(),
            to: data.to.to_bytes_be().to_vec(),
            block_number: data.block_number,
            tx_hash: data.tx_hash.to_bytes_be().to_vec(),
            timestamp: data.timestamp.unwrap_or(0),
        }
    }

    /// Check if a transfer matches a filter (for subscriptions)
    fn matches_transfer_filter(transfer: &NftTransfer, filter: &TransferFilter) -> bool {
        // Wallet filter (OR logic: matches from OR to)
        if let Some(ref wallet) = filter.wallet {
            let matches_from = transfer.from == *wallet;
            let matches_to = transfer.to == *wallet;
            if !matches_from && !matches_to {
                return false;
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

        // Token ID whitelist
        if !filter.token_ids.is_empty() && !filter.token_ids.contains(&transfer.token_id) {
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

    fn hex_without_leading_zeroes(bytes: &[u8]) -> String {
        let mut out = hex::encode(bytes);
        while out.starts_with('0') && out.len() > 1 {
            out.remove(0);
        }
        if out.is_empty() {
            "0".to_owned()
        } else {
            out
        }
    }

    fn static_image_url(contract: Felt, token_id: U256) -> String {
        let token_hex = Self::hex_without_leading_zeroes(&u256_to_bytes(token_id));
        format!(
            "https://api.cartridge.gg/x/{DEFAULT_PROJECT_ID}/torii/static/{contract:#x}/0x{token_hex}/image"
        )
    }

    fn build_trait_summaries(facets: &[AttributeFacetCount]) -> Vec<TraitSummary> {
        let mut per_key: HashMap<String, HashSet<String>> = HashMap::new();
        for facet in facets {
            per_key
                .entry(facet.key.clone())
                .or_default()
                .insert(facet.value.clone());
        }

        let mut summaries: Vec<TraitSummary> = per_key
            .into_iter()
            .map(|(key, values)| TraitSummary {
                key,
                value_count: values.len() as u64,
            })
            .collect();
        summaries.sort_by(|a, b| a.key.cmp(&b.key));
        summaries
    }

    async fn query_collection_tokens(
        &self,
        contract: Felt,
        filters: &[(String, Vec<String>)],
        cursor_token_id: Option<U256>,
        limit: u32,
        include_facets: bool,
        facet_limit: u32,
        include_images: bool,
    ) -> Result<
        (
            Vec<CollectionToken>,
            Option<Vec<u8>>,
            u64,
            Vec<AttributeFacetCount>,
        ),
        Status,
    > {
        let result = self
            .storage
            .query_token_ids_by_attributes(
                contract,
                filters,
                cursor_token_id,
                limit,
                include_facets,
                facet_limit,
            )
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let uri_rows = self
            .storage
            .get_token_uris_batch(contract, &result.token_ids)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;
        let by_token_id: HashMap<Vec<u8>, (Option<String>, Option<String>)> = uri_rows
            .into_iter()
            .map(|(token_id, uri, metadata_json)| (u256_to_bytes(token_id), (uri, metadata_json)))
            .collect();

        let tokens = result
            .token_ids
            .iter()
            .map(|token_id| {
                let token_id_bytes = u256_to_bytes(*token_id);
                let (uri, metadata_json) = by_token_id
                    .get(&token_id_bytes)
                    .cloned()
                    .unwrap_or((None, None));
                let image_url = if include_images {
                    Some(Self::static_image_url(contract, *token_id))
                } else {
                    None
                };

                CollectionToken {
                    contract_address: contract.to_bytes_be().to_vec(),
                    token_id: token_id_bytes,
                    uri,
                    metadata_json,
                    image_url,
                }
            })
            .collect();

        let facets = result
            .facets
            .into_iter()
            .map(|f| AttributeFacetCount {
                key: f.key,
                value: f.value,
                count: f.count,
            })
            .collect();

        Ok((
            tokens,
            result.next_cursor_token_id.map(u256_to_bytes),
            result.total_hits,
            facets,
        ))
    }
}

#[async_trait]
impl Erc721Trait for Erc721Service {
    /// Query historical transfers with filtering and pagination
    async fn get_transfers(
        &self,
        request: Request<GetTransfersRequest>,
    ) -> Result<Response<GetTransfersResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        let wallet = filter.wallet.as_ref().and_then(|b| bytes_to_felt(b));
        let from = filter.from.as_ref().and_then(|b| bytes_to_felt(b));
        let to = filter.to.as_ref().and_then(|b| bytes_to_felt(b));
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| bytes_to_felt(b))
            .collect();
        let token_ids: Vec<U256> = filter.token_ids.iter().map(|b| bytes_to_u256(b)).collect();

        let cursor = req.cursor.map(|c| TransferCursor {
            block_number: c.block_number,
            id: c.id,
        });

        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let (transfers, next_cursor) = self
            .storage
            .get_transfers_filtered(
                wallet,
                from,
                to,
                &tokens,
                &token_ids,
                filter.block_from,
                filter.block_to,
                cursor,
                limit,
            )
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let proto_transfers: Vec<NftTransfer> =
            transfers.iter().map(Self::transfer_data_to_proto).collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        Ok(Response::new(GetTransfersResponse {
            transfers: proto_transfers,
            next_cursor: proto_cursor,
        }))
    }

    /// Query current NFT ownership
    async fn get_ownership(
        &self,
        request: Request<GetOwnershipRequest>,
    ) -> Result<Response<GetOwnershipResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        let owner = filter.owner.as_ref().and_then(|b| bytes_to_felt(b));
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| bytes_to_felt(b))
            .collect();

        let cursor = req.cursor.map(|c| crate::storage::OwnershipCursor {
            block_number: c.block_number,
            id: c.id,
        });

        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let owner = owner.ok_or_else(|| Status::invalid_argument("owner filter is required"))?;

        let (ownership, next_cursor) = self
            .storage
            .get_ownership_by_owner(owner, &tokens, cursor, limit)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let proto_ownership: Vec<Ownership> = ownership
            .iter()
            .map(|o| Ownership {
                token: o.token.to_bytes_be().to_vec(),
                token_id: u256_to_bytes(o.token_id),
                owner: o.owner.to_bytes_be().to_vec(),
                block_number: o.block_number,
            })
            .collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        Ok(Response::new(GetOwnershipResponse {
            ownership: proto_ownership,
            next_cursor: proto_cursor,
        }))
    }

    /// Get the current owner of a specific NFT
    async fn get_owner(
        &self,
        request: Request<GetOwnerRequest>,
    ) -> Result<Response<GetOwnerResponse>, Status> {
        let req = request.into_inner();

        let token = bytes_to_felt(&req.token)
            .ok_or_else(|| Status::invalid_argument("invalid token address"))?;
        let token_id = bytes_to_u256(&req.token_id);

        let owner = self
            .storage
            .get_owner(token, token_id)
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        Ok(Response::new(GetOwnerResponse {
            owner: owner.map(|o| o.to_bytes_be().to_vec()),
        }))
    }

    /// Get token metadata (name, symbol)
    async fn get_token_metadata(
        &self,
        request: Request<GetTokenMetadataRequest>,
    ) -> Result<Response<GetTokenMetadataResponse>, Status> {
        let req = request.into_inner();

        if let Some(token_bytes) = req.token {
            let token = bytes_to_felt(&token_bytes)
                .ok_or_else(|| Status::invalid_argument("Invalid token address"))?;

            let entries = match self.storage.get_token_metadata(token).await {
                Ok(Some((name, symbol, total_supply))) => vec![TokenMetadataEntry {
                    token: token.to_bytes_be().to_vec(),
                    name,
                    symbol,
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

        let cursor = req.cursor.as_ref().and_then(|b| bytes_to_felt(b));
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
            .map(|(token, name, symbol, total_supply)| TokenMetadataEntry {
                token: token.to_bytes_be().to_vec(),
                name,
                symbol,
                total_supply: total_supply.map(u256_to_bytes),
            })
            .collect();

        Ok(Response::new(GetTokenMetadataResponse {
            tokens: entries,
            next_cursor: next_cursor.map(|c| c.to_bytes_be().to_vec()),
        }))
    }

    /// Query token IDs by flattened metadata attributes (OR within key, AND across keys).
    async fn query_tokens_by_attributes(
        &self,
        request: Request<QueryTokensByAttributesRequest>,
    ) -> Result<Response<QueryTokensByAttributesResponse>, Status> {
        let req = request.into_inner();
        let token = bytes_to_felt(&req.token)
            .ok_or_else(|| Status::invalid_argument("Invalid token address"))?;
        let cursor_token_id = req.cursor_token_id.as_ref().map(|b| bytes_to_u256(b));
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };
        let facet_limit = if req.facet_limit == 0 {
            100
        } else {
            req.facet_limit.min(1000)
        };

        let filters: Vec<(String, Vec<String>)> =
            req.filters.into_iter().map(|f| (f.key, f.values)).collect();

        let result = self
            .storage
            .query_token_ids_by_attributes(
                token,
                &filters,
                cursor_token_id,
                limit,
                req.include_facets,
                facet_limit,
            )
            .await
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        Ok(Response::new(QueryTokensByAttributesResponse {
            token_ids: result.token_ids.into_iter().map(u256_to_bytes).collect(),
            next_cursor_token_id: result.next_cursor_token_id.map(u256_to_bytes),
            total_hits: result.total_hits,
            facets: result
                .facets
                .into_iter()
                .map(|f| AttributeFacetCount {
                    key: f.key,
                    value: f.value,
                    count: f.count,
                })
                .collect(),
        }))
    }

    /// Fetch collection token rows with pagination and optional facets.
    async fn get_collection_tokens(
        &self,
        request: Request<GetCollectionTokensRequest>,
    ) -> Result<Response<GetCollectionTokensResponse>, Status> {
        let req = request.into_inner();
        let contract = bytes_to_felt(&req.contract_address)
            .ok_or_else(|| Status::invalid_argument("Invalid contract address"))?;
        let cursor_token_id = req.cursor_token_id.as_ref().map(|b| bytes_to_u256(b));
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };
        let facet_limit = if req.facet_limit == 0 {
            100
        } else {
            req.facet_limit.min(1000)
        };
        let filters: Vec<(String, Vec<String>)> =
            req.filters.into_iter().map(|f| (f.key, f.values)).collect();

        let (tokens, next_cursor_token_id, total_hits, facets) = self
            .query_collection_tokens(
                contract,
                &filters,
                cursor_token_id,
                limit,
                req.include_facets,
                facet_limit,
                req.include_images,
            )
            .await?;

        Ok(Response::new(GetCollectionTokensResponse {
            tokens,
            next_cursor_token_id,
            total_hits,
            facets,
        }))
    }

    /// Fetch trait facets and trait summaries for a collection.
    async fn get_collection_trait_facets(
        &self,
        request: Request<GetCollectionTraitFacetsRequest>,
    ) -> Result<Response<GetCollectionTraitFacetsResponse>, Status> {
        let req = request.into_inner();
        let contract = bytes_to_felt(&req.contract_address)
            .ok_or_else(|| Status::invalid_argument("Invalid contract address"))?;
        let facet_limit = if req.facet_limit == 0 {
            100
        } else {
            req.facet_limit.min(1000)
        };
        let filters: Vec<(String, Vec<String>)> =
            req.filters.into_iter().map(|f| (f.key, f.values)).collect();

        let (_, _, total_hits, facets) = self
            .query_collection_tokens(contract, &filters, None, 1, true, facet_limit, false)
            .await?;
        let traits = Self::build_trait_summaries(&facets);

        Ok(Response::new(GetCollectionTraitFacetsResponse {
            facets,
            traits,
            total_hits,
        }))
    }

    /// Fetch grouped overview blocks for one or more contracts.
    async fn get_collection_overview(
        &self,
        request: Request<GetCollectionOverviewRequest>,
    ) -> Result<Response<GetCollectionOverviewResponse>, Status> {
        let req = request.into_inner();
        if req.contract_addresses.is_empty() {
            return Err(Status::invalid_argument(
                "contract_addresses must not be empty",
            ));
        }

        let per_contract_limit = if req.per_contract_limit == 0 {
            50
        } else {
            req.per_contract_limit.min(200)
        };
        let facet_limit = if req.facet_limit == 0 {
            100
        } else {
            req.facet_limit.min(1000)
        };
        let mut filter_by_contract: HashMap<Felt, Vec<(String, Vec<String>)>> = HashMap::new();
        for cf in req.contract_filters {
            let Some(contract) = bytes_to_felt(&cf.contract_address) else {
                continue;
            };
            let filters = cf.filters.into_iter().map(|f| (f.key, f.values)).collect();
            filter_by_contract.insert(contract, filters);
        }

        let mut contracts = Vec::new();
        let mut seen = HashSet::new();
        for raw in req.contract_addresses {
            let contract = bytes_to_felt(&raw)
                .ok_or_else(|| Status::invalid_argument("Invalid contract address"))?;
            if seen.insert(contract) {
                contracts.push(contract);
            }
        }

        let mut overviews = Vec::with_capacity(contracts.len());
        for contract in contracts {
            let empty_filters = Vec::new();
            let filters = filter_by_contract.get(&contract).unwrap_or(&empty_filters);
            let (tokens, next_cursor_token_id, total_hits, facets) = self
                .query_collection_tokens(
                    contract,
                    filters,
                    None,
                    per_contract_limit,
                    req.include_facets,
                    facet_limit,
                    req.include_images,
                )
                .await?;
            let traits = Self::build_trait_summaries(&facets);

            overviews.push(ContractCollectionOverview {
                contract_address: contract.to_bytes_be().to_vec(),
                tokens,
                next_cursor_token_id,
                total_hits,
                facets,
                traits,
            });
        }

        Ok(Response::new(GetCollectionOverviewResponse { overviews }))
    }

    /// Subscribe to real-time transfer events
    type SubscribeTransfersStream =
        Pin<Box<dyn Stream<Item = Result<TransferUpdate, Status>> + Send>>;

    async fn subscribe_transfers(
        &self,
        request: Request<SubscribeTransfersRequest>,
    ) -> Result<Response<Self::SubscribeTransfersStream>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        tracing::info!(
            target: "torii_erc721::grpc",
            "New transfer subscription from client: {}",
            req.client_id
        );

        let mut rx = self.transfer_tx.subscribe();

        let stream = async_stream::try_stream! {
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        if let Some(ref transfer) = update.transfer {
                            if !Self::matches_transfer_filter(transfer, &filter) {
                                continue;
                            }
                        }
                        yield update;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            target: "torii_erc721::grpc",
                            "Client {} lagged, skipped {} updates",
                            req.client_id,
                            skipped
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
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

        let unique_tokens = self
            .storage
            .get_token_count()
            .await
            .map_err(|e| Status::internal(format!("Failed to get token count: {e}")))?;

        let unique_nfts = self
            .storage
            .get_nft_count()
            .await
            .map_err(|e| Status::internal(format!("Failed to get NFT count: {e}")))?;

        let latest_block = self
            .storage
            .get_latest_block()
            .await
            .map_err(|e| Status::internal(format!("Failed to get latest block: {e}")))?
            .unwrap_or(0);

        Ok(Response::new(GetStatsResponse {
            total_transfers,
            unique_tokens,
            unique_nfts,
            latest_block,
        }))
    }
}
