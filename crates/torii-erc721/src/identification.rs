//! ERC721 contract identification rule
//!
//! Identifies contracts as ERC721 NFTs by inspecting their ABI for:
//! - `owner_of` function
//! - `balance_of` function
//! - `Transfer` event (with token_id)

use anyhow::Result;
use starknet_types_raw::Felt;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::ContractAbi;
use torii::etl::identification::IdentificationRule;

/// ERC721 identification rule
///
/// Identifies contracts as ERC721 NFTs by checking for core ERC721 functions
/// and events in the ABI.
///
/// # Identification Criteria
///
/// A contract is identified as ERC721 if its ABI contains:
/// - `owner_of` function (or `ownerOf` camelCase variant)
/// - `balance_of` function (or `balanceOf` camelCase variant)
/// - `Transfer` event
///
/// Note: ERC721 Transfer events differ from ERC20 Transfer events in that
/// they include a `token_id` parameter. However, at the ABI level, both
/// have a `Transfer` event - the decoder distinguishes them by event structure.
pub struct Erc721Rule;

impl Erc721Rule {
    /// Create a new ERC721 identification rule
    pub fn new() -> Self {
        Self
    }
}

impl Default for Erc721Rule {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentificationRule for Erc721Rule {
    fn name(&self) -> &'static str {
        "erc721"
    }

    fn decoder_ids(&self) -> Vec<DecoderId> {
        vec![DecoderId::new("erc721")]
    }

    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        // Check for owner_of function (distinguishes ERC721 from ERC20)
        let has_owner_of = abi.has_function("owner_of") || abi.has_function("ownerOf");

        // Check for balance_of function
        let has_balance_of = abi.has_function("balance_of") || abi.has_function("balanceOf");

        // Check for Transfer event
        let has_transfer_event = abi.has_event("Transfer");

        // ERC721 must have owner_of (which ERC20 doesn't have)
        if has_owner_of && has_balance_of && has_transfer_event {
            tracing::debug!(
                target: "torii_erc721::identification",
                "Contract matches ERC721 pattern"
            );
            Ok(vec![DecoderId::new("erc721")])
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_erc721_rule_name() {
        let rule = Erc721Rule::new();
        assert_eq!(rule.name(), "erc721");
    }

    #[test]
    fn test_erc721_rule_decoder_ids() {
        let rule = Erc721Rule::new();
        let ids = rule.decoder_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], DecoderId::new("erc721"));
    }
}
