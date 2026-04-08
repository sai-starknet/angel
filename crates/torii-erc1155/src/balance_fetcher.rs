//! Balance fetcher for making starknet_call to balance_of
//!
//! Supports batch RPC requests for efficiency when fetching multiple balances
//! at specific block heights (used for inconsistency detection).
//!
//! ERC1155 has a different signature: balance_of(account, token_id)

use anyhow::{Context, Result};
use primitive_types::U256;
use starknet::core::types::{requests::CallRequest, BlockId, FunctionCall};
use starknet::macros::selector;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use std::sync::Arc;
use torii_common::utils::{felt_pair_to_u256, felt_to_u256};

/// Request for fetching an ERC1155 balance at a specific block
#[derive(Debug, Clone)]
pub struct Erc1155BalanceFetchRequest {
    /// ERC1155 contract address
    pub contract: Felt,
    /// Wallet address to fetch balance for
    pub wallet: Felt,
    /// Token ID within the ERC1155 contract
    pub token_id: U256,
    /// Block number to fetch balance at (typically block N-1 before the transfer)
    pub block_number: u64,
}

/// Balance fetcher for ERC1155 tokens
///
/// Makes starknet_call requests to fetch balance_of(account, token_id) at specific block heights.
/// Used for detecting and correcting balance inconsistencies caused by
/// genesis allocations, airdrops, or other transfers without events.
pub struct Erc1155BalanceFetcher {
    provider: Arc<JsonRpcClient<HttpTransport>>,
}

impl Erc1155BalanceFetcher {
    /// Create a new balance fetcher with the given provider
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        Self { provider }
    }

    /// Fetch a single balance at a specific block
    ///
    /// Returns the balance, or 0 if the call fails (contract may not exist yet, etc.)
    pub async fn fetch_balance(
        &self,
        contract: Felt,
        wallet: Felt,
        token_id: U256,
        block_number: u64,
    ) -> Result<U256> {
        // ERC1155 balance_of takes (account, token_id) where token_id is u256
        let calldata = build_balance_of_calldata(wallet, token_id);

        let call = FunctionCall {
            contract_address: to_starknet_felt(contract),
            entry_point_selector: selector!("balance_of"),
            calldata: calldata.into_iter().map(to_starknet_felt).collect(),
        };

        let block_id = BlockId::Number(block_number);

        match self.provider.call(call, block_id).await {
            Ok(result) => Ok(parse_u256_result(
                &result
                    .into_iter()
                    .map(from_starknet_felt)
                    .collect::<Vec<_>>(),
            )),
            Err(e) => {
                tracing::warn!(
                    target: "torii_erc1155::balance_fetcher",
                    contract = %contract,
                    wallet = %wallet,
                    token_id = ?token_id,
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
    /// Returns a vector of (contract, wallet, token_id, balance) tuples.
    /// On RPC failure for any request, sets that balance to 0 and continues.
    pub async fn fetch_balances_batch(
        &self,
        requests: &[Erc1155BalanceFetchRequest],
    ) -> Result<Vec<(Felt, Felt, U256, U256)>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        // Build batch request
        let rpc_requests: Vec<ProviderRequestData> = requests
            .iter()
            .map(|req| {
                let calldata = build_balance_of_calldata(req.wallet, req.token_id);
                ProviderRequestData::Call(CallRequest {
                    request: FunctionCall {
                        contract_address: to_starknet_felt(req.contract),
                        entry_point_selector: selector!("balance_of"),
                        calldata: calldata.into_iter().map(to_starknet_felt).collect(),
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
        let mut results = Vec::with_capacity(requests.len());
        for (idx, response) in responses.into_iter().enumerate() {
            let req = &requests[idx];
            let balance = if let ProviderResponseData::Call(felts) = response {
                parse_u256_result(
                    &felts
                        .into_iter()
                        .map(from_starknet_felt)
                        .collect::<Vec<_>>(),
                )
            } else {
                tracing::warn!(
                    target: "torii_erc1155::balance_fetcher",
                    contract = %req.contract,
                    wallet = %req.wallet,
                    token_id = ?req.token_id,
                    block = req.block_number,
                    "Unexpected response type for balance_of, using 0"
                );
                U256::from(0u64)
            };
            results.push((req.contract, req.wallet, req.token_id, balance));
        }

        tracing::debug!(
            target: "torii_erc1155::balance_fetcher",
            count = results.len(),
            "Fetched balances batch"
        );

        Ok(results)
    }
}

/// Build calldata for balance_of(account, token_id)
/// ERC1155 expects: [account, token_id_low, token_id_high]
fn build_balance_of_calldata(wallet: Felt, token_id: U256) -> Vec<Felt> {
    let (low, high) = u256_low_high(token_id);
    vec![wallet, low, high]
}

/// Parse a U256 result from balance_of return value
///
/// ERC1155 balance_of typically returns:
/// - Cairo 0: A single felt (fits in 252 bits)
/// - Cairo 1 with u256: Two felts [low, high]
fn parse_u256_result(result: &[Felt]) -> U256 {
    match result.len() {
        0 => U256::from(0u64),
        1 => felt_to_u256(result[0]),
        _ => felt_pair_to_u256(result[0], result[1]),
    }
}

fn to_starknet_felt(value: Felt) -> starknet::core::types::Felt {
    starknet::core::types::Felt::from_bytes_be(&value.to_be_bytes())
}

fn from_starknet_felt(value: starknet::core::types::Felt) -> Felt {
    Felt::from_be_bytes_slice(&value.to_bytes_be()).unwrap_or(Felt::ZERO)
}

fn u256_low_high(value: U256) -> (Felt, Felt) {
    let [l0, l1, h0, h1] = value.0;
    (
        Felt::from_le_words([l0, l1, 0, 0]),
        Felt::from_le_words([h0, h1, 0, 0]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u256_empty() {
        let result = parse_u256_result(&[]);
        assert_eq!(result, U256::from(0u64));
    }

    #[test]
    fn test_parse_u256_single_felt() {
        let felt = Felt::from(1000u64);
        let result = parse_u256_result(&[felt]);
        assert_eq!(result, U256::from(1000u64));
    }

    #[test]
    fn test_parse_u256_two_felts() {
        let low = Felt::from(100u64);
        let high = Felt::from(0u64);
        let result = parse_u256_result(&[low, high]);
        assert_eq!(result, U256::from(100u64));
    }

    #[test]
    fn test_build_calldata() {
        let wallet = Felt::from(123u64);
        let token_id = U256::from(456u64);
        let calldata = build_balance_of_calldata(wallet, token_id);

        assert_eq!(calldata.len(), 3);
        assert_eq!(calldata[0], wallet);
        assert_eq!(calldata[1], Felt::from(456u64)); // low
        assert_eq!(calldata[2], Felt::from(0u64)); // high
    }
}
