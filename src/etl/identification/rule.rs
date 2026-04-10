//! Pluggable contract identification rules (ABI-based).
//!
//! Sink/decoder authors implement [`IdentificationRule`] to define how their contracts
//! can be identified by inspecting the ABI.

use anyhow::Result;
use starknet_types_raw::Felt;

use crate::etl::decoder::DecoderId;
use crate::etl::extractor::ContractAbi;

/// Pluggable contract identification rule (ABI-based).
///
/// Sink/decoder authors implement this trait to define how their contracts
/// can be identified by inspecting the ABI. Rules are run in order, and
/// all matching decoders are collected.
///
/// # Example
///
/// ```rust,ignore
/// use torii::etl::identification::IdentificationRule;
/// use torii::etl::decoder::DecoderId;
/// use torii::etl::extractor::ContractAbi;
/// use starknet::core::types::Felt;
///
/// pub struct Erc20Rule;
///
/// impl IdentificationRule for Erc20Rule {
///     fn name(&self) -> &str { "erc20" }
///
///     fn decoder_ids(&self) -> Vec<DecoderId> {
///         vec![DecoderId::new("erc20")]
///     }
///
///     fn identify_by_abi(
///         &self,
///         _contract_address: Felt,
///         _class_hash: Felt,
///         abi: &ContractAbi,
///     ) -> anyhow::Result<Vec<DecoderId>> {
///         // ERC20 requires: transfer, balance_of, Transfer event
///         if abi.has_function("transfer") &&
///            abi.has_function("balance_of") &&
///            abi.has_event("Transfer") {
///             Ok(vec![DecoderId::new("erc20")])
///         } else {
///             Ok(Vec::new())
///         }
///     }
/// }
/// ```
pub trait IdentificationRule: Send + Sync {
    /// Unique name for this rule (for logging).
    ///
    /// This should be descriptive and unique across all rules in the system.
    /// Example: "erc20", "erc721", "custom_game_contract"
    fn name(&self) -> &str;

    /// Decoder IDs this rule can identify.
    ///
    /// Returns the list of decoder IDs that this rule may return from `identify_by_abi`.
    /// Used for validation and documentation.
    fn decoder_ids(&self) -> Vec<DecoderId>;

    /// Identify contract type by inspecting its ABI.
    ///
    /// Returns decoder IDs if the ABI matches this rule's patterns.
    /// Returns an empty Vec if no match.
    ///
    /// # Arguments
    ///
    /// * `contract_address` - The contract address being identified
    /// * `class_hash` - The class hash of the contract
    /// * `abi` - The parsed contract ABI
    ///
    /// # Returns
    ///
    /// A vector of decoder IDs that should handle events from this contract.
    /// Return an empty vector if the contract doesn't match this rule.
    fn identify_by_abi(
        &self,
        contract_address: Felt,
        class_hash: Felt,
        abi: &ContractAbi,
    ) -> Result<Vec<DecoderId>>;
}
