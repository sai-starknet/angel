//! Token metadata fetcher for ERC20/ERC721/ERC1155 contracts.
//!
//! Fetches `name()`, `symbol()`, `decimals()`, and `token_uri(token_id)` by
//! making `starknet_call` requests. Handles both snake_case and camelCase
//! selectors, felt-encoded strings and ByteArray returns.

use primitive_types::U256;
use starknet::core::codec::Decode;
use starknet::core::types::requests::CallRequest;
use starknet::core::types::{BlockId, BlockTag, ByteArray, Felt as SnFelt, FunctionCall};
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::selector;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use std::sync::Arc;

use crate::utils::felts_to_u256;

/// Token metadata (common fields for all ERC standards)
#[derive(Debug, Clone, Default)]
pub struct TokenMetadata {
    /// Token name (e.g. "Ether")
    pub name: Option<String>,
    /// Token symbol (e.g. "ETH")
    pub symbol: Option<String>,
    /// Token decimals (e.g. 18). Only meaningful for ERC20.
    pub decimals: Option<u8>,
    /// Total supply as U256 string. Meaningful for ERC721/ERC1155.
    pub total_supply: Option<U256>,
}

/// Fetches token metadata from on-chain contracts via RPC calls.
pub struct MetadataFetcher {
    provider: Arc<JsonRpcClient<HttpTransport>>,
}

impl MetadataFetcher {
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        Self { provider }
    }

    /// Fetch metadata for an ERC20 token (name, symbol, decimals).
    pub async fn fetch_erc20_metadata(&self, contract: Felt) -> TokenMetadata {
        let name = self.fetch_string(contract, "name").await;
        let symbol = self.fetch_string(contract, "symbol").await;
        let decimals = self.fetch_decimals(contract).await;
        let total_supply = self.fetch_total_supply(contract).await;

        TokenMetadata {
            name,
            symbol,
            decimals,
            total_supply,
        }
    }

    /// Fetch metadata for an ERC721 contract (name, symbol, totalSupply).
    pub async fn fetch_erc721_metadata(&self, contract: Felt) -> TokenMetadata {
        let name = self.fetch_string(contract, "name").await;
        let symbol = self.fetch_string(contract, "symbol").await;
        let total_supply = self.fetch_total_supply(contract).await;

        TokenMetadata {
            name,
            symbol,
            decimals: None,
            total_supply,
        }
    }

    /// Fetch metadata for an ERC1155 contract (name, symbol, totalSupply if available).
    pub async fn fetch_erc1155_metadata(&self, contract: Felt) -> TokenMetadata {
        let name = self.fetch_string(contract, "name").await;
        let symbol = self.fetch_string(contract, "symbol").await;
        let total_supply = self.fetch_total_supply(contract).await;

        TokenMetadata {
            name,
            symbol,
            decimals: None,
            total_supply,
        }
    }

    /// Fetch `token_uri(token_id)` or `tokenURI(token_id)` for a specific NFT.
    ///
    /// Returns None if the call fails or returns empty data.
    pub async fn fetch_token_uri(&self, contract: Felt, token_id: Felt) -> Option<String> {
        // Try snake_case first, then camelCase
        let contract = contract.into();
        let token_id = token_id.into();
        for sel in [selector!("token_uri"), selector!("tokenURI")] {
            let call = FunctionCall {
                contract_address: contract,
                entry_point_selector: sel,
                calldata: vec![token_id, SnFelt::ZERO], // u256: (low, high)
            };

            if let Ok(result) = self
                .provider
                .call(call, BlockId::Tag(BlockTag::Latest))
                .await
            {
                if let Some(s) = Self::decode_string_result(&result) {
                    if !s.is_empty() {
                        return Some(s);
                    }
                }
            }
        }

        // Try with single felt arg (some contracts don't use u256 for token_id)
        for sel in [selector!("token_uri"), selector!("tokenURI")] {
            let call = FunctionCall {
                contract_address: contract,
                entry_point_selector: sel,
                calldata: vec![token_id],
            };

            if let Ok(result) = self
                .provider
                .call(call, BlockId::Tag(BlockTag::Latest))
                .await
            {
                if let Some(s) = Self::decode_string_result(&result) {
                    if !s.is_empty() {
                        return Some(s);
                    }
                }
            }
        }

        None
    }

    /// Fetch `uri(token_id)` for ERC1155 tokens.
    pub async fn fetch_uri(&self, contract: Felt, token_id: Felt) -> Option<String> {
        // ERC1155 uses `uri(token_id)` — u256 arg
        let contract = contract.into();
        let token_id = token_id.into();

        let call = FunctionCall {
            contract_address: contract,
            entry_point_selector: selector!("uri"),
            calldata: vec![token_id, SnFelt::ZERO],
        };

        if let Ok(result) = self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            if let Some(s) = Self::decode_string_result(&result) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        // Try single felt arg
        let call = FunctionCall {
            contract_address: contract,
            entry_point_selector: selector!("uri"),
            calldata: vec![token_id],
        };

        if let Ok(result) = self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            if let Some(s) = Self::decode_string_result(&result) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        None
    }

    /// Fetch `token_uri(token_id)` or `tokenURI(token_id)` for multiple ERC721 NFTs.
    pub async fn fetch_token_uri_batch(&self, requests: &[(Felt, Felt)]) -> Vec<Option<String>> {
        self.fetch_string_calls_batch(
            requests,
            &[
                (selector!("token_uri"), true),
                (selector!("tokenURI"), true),
                (selector!("token_uri"), false),
                (selector!("tokenURI"), false),
            ],
        )
        .await
    }

    /// Fetch `uri(token_id)` for multiple ERC1155 tokens.
    pub async fn fetch_uri_batch(&self, requests: &[(Felt, Felt)]) -> Vec<Option<String>> {
        self.fetch_string_calls_batch(
            requests,
            &[(selector!("uri"), true), (selector!("uri"), false)],
        )
        .await
    }

    /// Fetch a string value (name or symbol) from a contract.
    ///
    /// Tries snake_case first, returns None on failure.
    async fn fetch_string(&self, contract: Felt, fn_name: &str) -> Option<String> {
        let sel = match fn_name {
            "name" => selector!("name"),
            "symbol" => selector!("symbol"),
            _ => return None,
        };

        let call = FunctionCall {
            contract_address: contract.into(),
            entry_point_selector: sel,
            calldata: vec![],
        };

        match self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            Ok(result) => Self::decode_string_result(&result),
            Err(e) => {
                tracing::debug!(
                    target: "torii_common::metadata",
                    contract = %format!("{:#x}", contract),
                    fn_name = fn_name,
                    error = %e,
                    "Failed to fetch string"
                );
                None
            }
        }
    }

    /// Fetch `decimals()` from an ERC20 contract.
    async fn fetch_decimals(&self, contract: Felt) -> Option<u8> {
        let call = FunctionCall {
            contract_address: contract.into(),
            entry_point_selector: selector!("decimals"),
            calldata: vec![],
        };

        match self
            .provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
        {
            Ok(result) => {
                if result.is_empty() {
                    return None;
                }
                // decimals() returns a single felt (typically u8)
                let val: u64 = result[0].try_into().unwrap_or(0);
                if val > 255 {
                    tracing::warn!(
                        target: "torii_common::metadata",
                        contract = %format!("{:#x}", contract),
                        value = val,
                        "Unexpected decimals value"
                    );
                    return None;
                }
                Some(val as u8)
            }
            Err(e) => {
                tracing::debug!(
                    target: "torii_common::metadata",
                    contract = %format!("{:#x}", contract),
                    error = %e,
                    "Failed to fetch decimals"
                );
                None
            }
        }
    }

    /// Fetch `total_supply()` or `totalSupply()` from a contract.
    async fn fetch_total_supply(&self, contract: Felt) -> Option<U256> {
        for sel in [selector!("total_supply"), selector!("totalSupply")] {
            let call = FunctionCall {
                contract_address: contract.into(),
                entry_point_selector: sel,
                calldata: vec![],
            };

            if let Ok(result) = self
                .provider
                .call(call, BlockId::Tag(BlockTag::Latest))
                .await
            {
                if result.is_empty() {
                    continue;
                }
                // U256 return: [low, high] or single felt
                return felts_to_u256(&result.into_iter().map(Into::into).collect::<Vec<_>>()).ok();
            }
        }

        None
    }

    /// Decode a string result from a contract call.
    ///
    /// Handles multiple return formats using `starknet` crate utilities:
    /// 1. **Single short string** (felt): via `parse_cairo_short_string`
    /// 2. **Cairo ByteArray**: via `ByteArray::decode` (the standard Cairo string type)
    /// 3. **Legacy array**: `[len, felt1, felt2, ...]` where each felt is a short string segment
    fn decode_string_result(result: &[SnFelt]) -> Option<String> {
        if result.is_empty() {
            return None;
        }

        // Single felt — short string (≤31 chars packed into felt)
        if result.len() == 1 {
            return parse_cairo_short_string(&result[0])
                .ok()
                .filter(|s| !s.is_empty());
        }

        // Try ByteArray format via starknet crate's Decode impl
        if let Ok(byte_array) = ByteArray::decode(result) {
            if let Ok(s) = String::try_from(byte_array) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        // Fallback: try as legacy array [len, felt1, felt2, ...]
        if result.len() >= 2 {
            let array_len: u64 = result[0].try_into().unwrap_or(0);
            if array_len > 0 && array_len < 100 && result.len() >= (array_len as usize + 1) {
                let mut s = String::with_capacity(array_len as usize * 31);
                for felt in &result[1..=array_len as usize] {
                    if let Ok(chunk) = parse_cairo_short_string(felt) {
                        s.push_str(&chunk);
                    }
                }
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        // Last resort: try first felt as short string
        parse_cairo_short_string(&result[0])
            .ok()
            .filter(|s| !s.is_empty())
    }

    async fn fetch_string_calls_batch(
        &self,
        requests: &[(Felt, Felt)],
        attempts: &[(SnFelt, bool)],
    ) -> Vec<Option<String>> {
        if requests.is_empty() {
            return Vec::new();
        }

        let mut results = vec![None; requests.len()];
        let mut unresolved = (0..requests.len()).collect::<Vec<_>>();

        for &(selector, use_u256) in attempts {
            if unresolved.is_empty() {
                break;
            }

            let rpc_requests = unresolved
                .iter()
                .map(|&idx| {
                    let (contract, token_id) = requests[idx];
                    let calldata = if use_u256 {
                        vec![token_id.into(), SnFelt::ZERO]
                    } else {
                        vec![token_id.into()]
                    };
                    ProviderRequestData::Call(CallRequest {
                        request: FunctionCall {
                            contract_address: contract.into(),
                            entry_point_selector: selector,
                            calldata,
                        },
                        block_id: BlockId::Tag(BlockTag::Latest),
                    })
                })
                .collect::<Vec<_>>();

            let responses = match self.provider.batch_requests(&rpc_requests).await {
                Ok(responses) => responses,
                Err(error) => {
                    tracing::warn!(
                        target: "torii_common::metadata",
                        batch_size = unresolved.len(),
                        selector = %format!("{selector:#x}"),
                        use_u256,
                        error = %error,
                        "Failed to batch fetch string calls"
                    );
                    break;
                }
            };

            let mut still_unresolved = Vec::with_capacity(unresolved.len());
            for (response_idx, response) in responses.into_iter().enumerate() {
                let request_idx = unresolved[response_idx];
                match response {
                    ProviderResponseData::Call(felts) => {
                        if let Some(value) = Self::decode_string_result(&felts) {
                            if !value.is_empty() {
                                results[request_idx] = Some(value);
                                continue;
                            }
                        }
                        still_unresolved.push(request_idx);
                    }
                    _ => still_unresolved.push(request_idx),
                }
            }

            unresolved = still_unresolved;
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_short_string() {
        // "ETH" = 0x455448
        let felt = SnFelt::from(0x455448u64);
        assert_eq!(parse_cairo_short_string(&felt).unwrap(), "ETH".to_string());
    }

    #[test]
    fn test_decode_single_felt_string() {
        let result = vec![SnFelt::from(0x455448u64)]; // "ETH"
        assert_eq!(
            MetadataFetcher::decode_string_result(&result),
            Some("ETH".to_string())
        );
    }

    #[test]
    fn test_decode_byte_array() {
        // ByteArray: [data_len=0, pending_word="ETH", pending_word_len=3]
        let result = vec![
            SnFelt::from(0u64),        // data_len = 0 chunks
            SnFelt::from(0x455448u64), // pending_word = "ETH"
            SnFelt::from(3u64),        // pending_word_len = 3
        ];
        assert_eq!(
            MetadataFetcher::decode_string_result(&result),
            Some("ETH".to_string())
        );
    }

    #[test]
    fn test_decode_empty() {
        assert_eq!(MetadataFetcher::decode_string_result(&[]), None);
    }
}
