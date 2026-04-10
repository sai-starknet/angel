//! Balance fetcher for making starknet_call to balance_of
//!
//! Supports batch RPC requests for efficiency when fetching multiple balances
//! at specific block heights (used for inconsistency detection).

use anyhow::{Context, Result};

/// Maximum number of requests per batch to avoid RPC limits
const MAX_BATCH_SIZE: usize = 500;
use primitive_types::U256;
use starknet::core::types::requests::CallRequest;
use starknet::core::types::{BlockId, FunctionCall};
use starknet::macros::selector;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use std::sync::Arc;
use torii_common::utils::felts_to_u256;

/// Request for fetching a balance at a specific block
#[derive(Debug, Clone)]
pub struct BalanceFetchRequest {
    /// ERC20 token contract address
    pub token: Felt,
    /// Wallet address to fetch balance for
    pub wallet: Felt,
    /// Block number to fetch balance at (typically block N-1 before the transfer)
    pub block_number: u64,
}

pub struct Balance {
    pub token: Felt,
    pub wallet: Felt,
    pub balance: U256,
}

impl Balance {
    pub fn new(token: Felt, wallet: Felt, balance: U256) -> Self {
        Self {
            token,
            wallet,
            balance,
        }
    }
}

/// Balance fetcher for ERC20 tokens
///
/// Makes starknet_call requests to fetch balance_of at specific block heights.
/// Used for detecting and correcting balance inconsistencies caused by
/// genesis allocations, airdrops, or other transfers without events.
pub struct BalanceFetcher {
    provider: Arc<JsonRpcClient<HttpTransport>>,
}

impl BalanceFetcher {
    /// Create a new balance fetcher with the given provider
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        Self { provider }
    }

    /// Fetch a single balance at a specific block
    ///
    /// Returns the balance, or 0 if the call fails (contract may not exist yet, etc.)
    pub async fn fetch_balance(
        &self,
        token: Felt,
        wallet: Felt,
        block_number: u64,
    ) -> Result<U256> {
        let call = FunctionCall {
            contract_address: token.into(),
            // TODO: Some contracts use balance_of (snake_case), others use balanceOf (camelCase).
            // We need a strategy to handle both - possibly try one, fallback to other, or use ABI.
            entry_point_selector: selector!("balanceOf"),
            calldata: vec![wallet.into()],
        };

        let block_id = BlockId::Number(block_number);

        match self.provider.call(call, block_id).await {
            Ok(result) => Ok(felts_to_u256(
                &result.into_iter().map(Into::into).collect::<Vec<_>>(),
            )?),
            Err(e) => {
                tracing::warn!(
                    target: "torii_erc20::balance_fetcher",
                    token = %token,
                    wallet = %wallet,
                    block = block_number,
                    error = %e,
                    "Failed to fetch balance, returning 0"
                );
                Ok(U256::from(0u64))
            }
        }
    }

    /// Batch fetch multiple balances at specific blocks
    ///
    /// Uses JSON-RPC batch requests for efficiency.
    /// Returns a vector of (token, wallet, balance) tuples.
    /// On RPC failure for any request, sets that balance to 0 and continues.
    ///
    /// Requests are chunked into batches of MAX_BATCH_SIZE (500) to avoid RPC limits.
    pub async fn fetch_balances_batch(
        &self,
        requests: &[BalanceFetchRequest],
    ) -> Result<Vec<Balance>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_results = Vec::with_capacity(requests.len());

        // Process in chunks of MAX_BATCH_SIZE
        for chunk in requests.chunks(MAX_BATCH_SIZE) {
            // Build batch request for this chunk
            // TODO: Some contracts use balance_of (snake_case), others use balanceOf (camelCase).
            // We need a strategy to handle both - possibly try one, fallback to other, or use ABI.
            let rpc_requests: Vec<ProviderRequestData> = chunk
                .iter()
                .map(|req| {
                    ProviderRequestData::Call(CallRequest {
                        request: FunctionCall {
                            contract_address: req.token.into(),
                            entry_point_selector: selector!("balanceOf"),
                            calldata: vec![req.wallet.into()],
                        },
                        block_id: BlockId::Number(req.block_number),
                    })
                })
                .collect();

            // Execute batch request
            let responses = self
                .provider
                .batch_requests(&rpc_requests)
                .await
                .context("Failed to execute batch balance_of requests")?;

            // Process responses
            for (idx, response) in responses.into_iter().enumerate() {
                let req = &chunk[idx];
                let balance = if let ProviderResponseData::Call(felts) = response {
                    felts_to_u256(felts.into_iter().map(Into::into).collect::<Vec<_>>())?
                } else {
                    tracing::warn!(
                        target: "torii_erc20::balance_fetcher",
                        token = %req.token,
                        wallet = %req.wallet,
                        block = req.block_number,
                        "Unexpected response type for balance_of, using 0"
                    );
                    U256::from(0u64)
                };
                all_results.push(Balance::new(req.token, req.wallet, balance));
            }
        }

        tracing::debug!(
            target: "torii_erc20::balance_fetcher",
            count = all_results.len(),
            "Fetched balances batch"
        );

        Ok(all_results)
    }
}
