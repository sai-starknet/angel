//! ERC20 contract identification rule
//!
//! Identifies contracts as ERC20 by inspecting their ABI for:
//! - `transfer` function
//! - `balance_of` function
//! - `Transfer` event

use anyhow::Result;
use starknet_types_raw::Felt;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::ContractAbi;
use torii::etl::identification::IdentificationRule;

/// ERC20 identification rule
///
/// Identifies contracts as ERC20 tokens by checking for core ERC20 functions
/// and events in the ABI.
///
/// # Identification Criteria
///
/// A contract is identified as ERC20 if its ABI contains:
/// - `transfer` function (or `camelCase` variant)
/// - `balance_of` function (or `balanceOf` camelCase variant)
/// - `Transfer` event
///
/// This covers both snake_case (standard Cairo) and camelCase (legacy) naming.
pub struct Erc20Rule;

impl Erc20Rule {
    /// Create a new ERC20 identification rule
    pub fn new() -> Self {
        Self
    }
}

impl Default for Erc20Rule {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentificationRule for Erc20Rule {
    fn name(&self) -> &'static str {
        "erc20"
    }

    fn decoder_ids(&self) -> Vec<DecoderId> {
        vec![DecoderId::new("erc20")]
    }

    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        // Check for transfer function (snake_case or camelCase)
        let has_transfer = abi.has_function("transfer");

        // Check for balance_of function (snake_case or camelCase)
        let has_balance_of = abi.has_function("balance_of") || abi.has_function("balanceOf");

        // Check for Transfer event
        let has_transfer_event = abi.has_event("Transfer");

        if has_transfer && has_balance_of && has_transfer_event {
            tracing::debug!(
                target: "torii_erc20::identification",
                "Contract matches ERC20 pattern"
            );
            Ok(vec![DecoderId::new("erc20")])
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_erc20_rule_name() {
        let rule = Erc20Rule::new();
        assert_eq!(rule.name(), "erc20");
    }

    #[test]
    fn test_erc20_rule_decoder_ids() {
        let rule = Erc20Rule::new();
        let ids = rule.decoder_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], DecoderId::new("erc20"));
    }
}
