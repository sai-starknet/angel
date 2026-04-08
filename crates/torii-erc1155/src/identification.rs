//! ERC1155 contract identification rule
//!
//! Identifies contracts as ERC1155 semi-fungible tokens by inspecting their ABI for:
//! - `balance_of_batch` function (unique to ERC1155)
//! - `TransferSingle` event
//! - `TransferBatch` event

use anyhow::Result;
use starknet_types_raw::Felt;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::ContractAbi;
use torii::etl::identification::IdentificationRule;

/// ERC1155 identification rule
///
/// Identifies contracts as ERC1155 semi-fungible tokens by checking for
/// unique ERC1155 functions and events in the ABI.
///
/// # Identification Criteria
///
/// A contract is identified as ERC1155 if its ABI contains:
/// - `balance_of_batch` function (unique to ERC1155, not in ERC20/ERC721)
/// - Either `TransferSingle` or `TransferBatch` event
///
/// The `balance_of_batch` function is the key differentiator from ERC20/ERC721.
pub struct Erc1155Rule;

impl Erc1155Rule {
    /// Create a new ERC1155 identification rule
    pub fn new() -> Self {
        Self
    }
}

impl Default for Erc1155Rule {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentificationRule for Erc1155Rule {
    fn name(&self) -> &'static str {
        "erc1155"
    }

    fn decoder_ids(&self) -> Vec<DecoderId> {
        vec![DecoderId::new("erc1155")]
    }

    fn identify_by_abi(
        &self,
        _contract_address: Felt,
        _class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>> {
        // Check for balance_of_batch function (unique to ERC1155)
        let has_balance_of_batch =
            abi.has_function("balance_of_batch") || abi.has_function("balanceOfBatch");

        // Check for ERC1155-specific events
        let has_transfer_single = abi.has_event("TransferSingle");
        let has_transfer_batch = abi.has_event("TransferBatch");

        // ERC1155 must have balance_of_batch (which ERC20/ERC721 don't have)
        // and at least one of the transfer events
        if has_balance_of_batch && (has_transfer_single || has_transfer_batch) {
            tracing::debug!(
                target: "torii_erc1155::identification",
                "Contract matches ERC1155 pattern"
            );
            Ok(vec![DecoderId::new("erc1155")])
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_erc1155_rule_name() {
        let rule = Erc1155Rule::new();
        assert_eq!(rule.name(), "erc1155");
    }

    #[test]
    fn test_erc1155_rule_decoder_ids() {
        let rule = Erc1155Rule::new();
        let ids = rule.decoder_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], DecoderId::new("erc1155"));
    }
}
