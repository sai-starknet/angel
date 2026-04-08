//! ERC20 event decoder (Transfer + Approval)

use anyhow::Result;
use async_trait::async_trait;
use primitive_types::U256;
use starknet_types_raw::event::EmittedEvent;
use starknet_types_raw::Felt;
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypeId, TypedBody};
use torii_common::utils::{felt_pair_to_u256, felt_to_u256};
use torii_types::event::EventContext;

const ERC20_TRANSFER_SELECTOR_TYPE_ID: TypeId = torii::etl::envelope::TypeId::new("erc20.transfer");
const ERC20_APPROVAL_SELECTOR_TYPE_ID: TypeId = torii::etl::envelope::TypeId::new("erc20.approval");
pub const TRANSFER_SELECTOR: Felt = Felt::selector("Transfer");
pub const APPROVAL_SELECTOR: Felt = Felt::selector("Approval");
/// Transfer event from ERC20 token
#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for Transfer {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        ERC20_TRANSFER_SELECTOR_TYPE_ID
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Approval event from ERC20 token
#[derive(Debug, Clone)]
pub struct Approval {
    pub owner: Felt,
    pub spender: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for Approval {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        ERC20_APPROVAL_SELECTOR_TYPE_ID
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ERC20 event decoder
///
/// Decodes multiple ERC20 events:
/// - Transfer(from, to, value)
/// - Approval(owner, spender, value)
///
/// # Pattern for Multi-Event Decoders
///
/// This decoder showcases the recommended pattern for handling multiple event types:
/// 1. Check selector first (fast O(1) comparison)
/// 2. Dispatch to specific decode_X() method
/// 3. Each method returns Result<Option<Envelope>> (None if not interested/malformed)
/// 4. Main decode_event() collects results
///
/// This pattern scales cleanly to many event types without complex branching.
pub struct Erc20Decoder;

impl Erc20Decoder {
    pub fn new() -> Self {
        Self
    }

    /// Transfer event selector: sn_keccak("Transfer")
    fn transfer_selector() -> Felt {
        TRANSFER_SELECTOR
    }

    /// Approval event selector: sn_keccak("Approval")
    fn approval_selector() -> Felt {
        APPROVAL_SELECTOR
    }

    pub async fn decode(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
        if event.keys.is_empty() {
            return Ok(Vec::new());
        }

        let selector = event.keys[0];

        if selector == Self::transfer_selector() {
            if let Some(envelope) = self.decode_transfer(event).await? {
                return Ok(vec![envelope]);
            }
        } else if selector == Self::approval_selector() {
            if let Some(envelope) = self.decode_approval(event).await? {
                return Ok(vec![envelope]);
            }
        } else {
            tracing::trace!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                selector = %format!("{:#x}", selector),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                block_number = event.block_number.unwrap_or(0),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                "Unhandled event selector"
            );
        }

        Ok(Vec::new())
    }

    /// Decode Transfer event into envelope
    ///
    /// Transfer event signatures (supports multiple formats):
    ///
    /// Modern ERC20 (standard):
    /// - keys[0]: Transfer selector
    /// - keys[1]: from address
    /// - keys[2]: to address
    /// - data[0]: amount_low (u128)
    /// - data[1]: amount_high (u128)
    ///
    /// All-in-keys format (some tokens put everything in keys):
    /// - keys[0]: Transfer selector
    /// - keys[1]: from address
    /// - keys[2]: to address
    /// - keys[3]: amount_low (u128)
    /// - keys[4]: amount_high (u128)
    /// - data: empty
    ///
    /// Legacy ERC20 (pre-keys era):
    /// - keys[0]: Transfer selector
    /// - data[0]: from address
    /// - data[1]: to address
    /// - data[2]: amount_low (u128)
    /// - data[3]: amount_high (u128)
    ///
    /// Felt-based variants use single felt for amount instead of U256.
    async fn decode_transfer(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let from;
        let to;
        let amount: U256;

        if event.keys.len() == 5 && event.data.is_empty() {
            // All-in-keys format: selector, from, to, amount_low, amount_high all in keys
            from = event.keys[1];
            to = event.keys[2];
            let low: Felt = event.keys[3];
            let high: Felt = event.keys[4];
            amount = felt_pair_to_u256(low, high);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded all-in-keys transfer (U256)"
            );
        } else if event.keys.len() == 4 && event.data.is_empty() {
            // All-in-keys format with felt amount: selector, from, to, amount in keys
            from = event.keys[1];
            to = event.keys[2];
            amount = felt_to_u256(event.keys[3]);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded all-in-keys transfer (felt)"
            );
        } else if event.keys.len() == 1 && event.data.len() == 4 {
            // Legacy ERC20: from, to, amount_low, amount_high in data
            from = event.data[0];
            to = event.data[1];
            amount = felt_pair_to_u256(event.data[2], event.data[3]);
        } else if event.keys.len() == 3 && event.data.len() == 2 {
            // Modern ERC20: from, to in keys; amount_low, amount_high in data
            from = event.keys[1];
            to = event.keys[2];
            amount = felt_pair_to_u256(event.data[0], event.data[1]);
        } else if event.keys.len() == 3 && event.data.len() == 1 {
            // Felt-based modern format: from, to in keys; amount as single felt (fits in u128)
            from = event.keys[1];
            to = event.keys[2];
            amount = felt_to_u256(event.data[0]);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded felt-based modern transfer"
            );
        } else if event.keys.len() == 1 && event.data.len() == 3 {
            // Felt-based legacy format: from, to, amount in data
            from = event.data[0];
            to = event.data[1];
            amount = felt_to_u256(event.data[2]);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                amount = %amount,
                "Decoded felt-based legacy transfer"
            );
        } else if event.keys.len() == 3 && event.data.is_empty() {
            // All-in-keys format with zero amount (or amount omitted): from, to in keys, no data
            from = event.keys[1];
            to = event.keys[2];
            amount = U256::zero();
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                from = %format!("{:#x}", from),
                to = %format!("{:#x}", to),
                "Decoded transfer with zero/omitted amount"
            );
        } else {
            tracing::warn!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed Transfer event"
            );
            return Ok(None);
        }

        let transfer = Transfer {
            from,
            to,
            amount,
            token: event.from_address,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        };

        let mut metadata = HashMap::new();
        metadata.insert("token".to_string(), format!("{:#x}", event.from_address));
        metadata.insert(
            "block_number".to_string(),
            event.block_number.unwrap_or(0).to_string(),
        );
        metadata.insert(
            "tx_hash".to_string(),
            format!("{:#x}", event.transaction_hash),
        );

        let envelope_id = format!(
            "erc20_transfer_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(transfer),
            metadata,
        )))
    }

    /// Decode Approval event into envelope
    ///
    /// Approval event signatures (supports multiple formats):
    ///
    /// Modern ERC20 (standard):
    /// - keys[0]: Approval selector
    /// - keys[1]: owner address
    /// - keys[2]: spender address
    /// - data[0]: amount_low (u128)
    /// - data[1]: amount_high (u128)
    ///
    /// All-in-keys format (some tokens put everything in keys):
    /// - keys[0]: Approval selector
    /// - keys[1]: owner address
    /// - keys[2]: spender address
    /// - keys[3]: amount_low (u128)
    /// - keys[4]: amount_high (u128)
    /// - data: empty
    ///
    /// Legacy ERC20 (pre-keys era):
    /// - keys[0]: Approval selector
    /// - data[0]: owner address
    /// - data[1]: spender address
    /// - data[2]: amount_low (u128)
    /// - data[3]: amount_high (u128)
    ///
    /// Felt-based variants use single felt for amount instead of U256.
    async fn decode_approval(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let owner;
        let spender;
        let amount: U256;

        if event.keys.len() == 5 && event.data.is_empty() {
            // All-in-keys format: selector, owner, spender, amount_low, amount_high all in keys
            owner = event.keys[1];
            spender = event.keys[2];
            amount = felt_pair_to_u256(event.keys[3], event.keys[4]);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded all-in-keys approval (U256)"
            );
        } else if event.keys.len() == 4 && event.data.is_empty() {
            // All-in-keys format with felt amount: selector, owner, spender, amount in keys
            owner = event.keys[1];
            spender = event.keys[2];
            amount = felt_to_u256(event.keys[3]);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded all-in-keys approval (felt)"
            );
        } else if event.keys.len() == 1 && event.data.len() == 4 {
            // Legacy ERC20: owner, spender, amount_low, amount_high in data
            owner = event.data[0];
            spender = event.data[1];
            amount = felt_pair_to_u256(event.data[2], event.data[3]);
        } else if event.keys.len() == 3 && event.data.len() == 2 {
            // Modern ERC20: owner, spender in keys; amount_low, amount_high in data
            owner = event.keys[1];
            spender = event.keys[2];
            amount = felt_pair_to_u256(event.data[0], event.data[1]);
        } else if event.keys.len() == 3 && event.data.len() == 1 {
            // Felt-based modern format: owner, spender in keys; amount as single felt
            owner = event.keys[1];
            spender = event.keys[2];
            amount = felt_to_u256(event.data[0]);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded felt-based modern approval"
            );
        } else if event.keys.len() == 1 && event.data.len() == 3 {
            // Felt-based legacy format: owner, spender, amount in data
            owner = event.data[0];
            spender = event.data[1];
            amount = felt_to_u256(event.data[2]);
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                amount = %amount,
                "Decoded felt-based legacy approval"
            );
        } else if event.keys.len() == 3 && event.data.is_empty() {
            // All-in-keys format with zero amount (or amount omitted)
            owner = event.keys[1];
            spender = event.keys[2];
            amount = U256::zero();
            tracing::debug!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                owner = %format!("{:#x}", owner),
                spender = %format!("{:#x}", spender),
                "Decoded approval with zero/omitted amount"
            );
        } else {
            tracing::warn!(
                target: "torii_erc20::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed Approval event"
            );
            return Ok(None);
        }

        let approval = Approval {
            owner,
            spender,
            amount,
            token: event.from_address,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        };

        let mut metadata = HashMap::new();
        metadata.insert("token".to_string(), format!("{:#x}", event.from_address));
        metadata.insert(
            "block_number".to_string(),
            event.block_number.unwrap_or(0).to_string(),
        );
        metadata.insert(
            "tx_hash".to_string(),
            format!("{:#x}", event.transaction_hash),
        );

        let envelope_id = format!(
            "erc20_approval_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(approval),
            metadata,
        )))
    }
}

impl Default for Erc20Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Decoder for Erc20Decoder {
    fn decoder_name(&self) -> &'static str {
        "erc20"
    }

    async fn decode(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> Result<Vec<Envelope>> {
        let event = EmittedEvent {
            from_address: context.from_address,
            keys: keys.to_vec(),
            data: data.to_vec(),
            block_hash: None,
            block_number: Some(context.block_number),
            transaction_hash: context.transaction_hash,
        };

        Erc20Decoder::decode(self, &event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decode_transfer() {
        let decoder = Erc20Decoder::new();

        let event = EmittedEvent {
            from_address: Felt::from(0x123u64), // Token contract
            keys: vec![
                Erc20Decoder::transfer_selector(),
                Felt::from(0x1u64), // from
                Felt::from(0x2u64), // to
            ],
            data: vec![
                Felt::from(1000u64), // amount_low
                Felt::ZERO,          // amount_high
            ],
            block_hash: None,
            block_number: Some(100),
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Transfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0x1u64));
        assert_eq!(transfer.to, Felt::from(0x2u64));
        assert_eq!(transfer.amount, U256::from(1000u64));
        assert_eq!(transfer.token, Felt::from(0x123u64));
    }

    #[tokio::test]
    async fn test_decode_approval() {
        let decoder = Erc20Decoder::new();

        let event = EmittedEvent {
            from_address: Felt::from(0x456u64), // Token contract
            keys: vec![
                Erc20Decoder::approval_selector(),
                Felt::from(0xau64), // owner
                Felt::from(0xbu64), // spender
            ],
            data: vec![
                Felt::from(5000u64), // amount_low
                Felt::ZERO,          // amount_high
            ],
            block_hash: None,
            block_number: Some(200),
            transaction_hash: Felt::from(0xdef0u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Approval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0xau64));
        assert_eq!(approval.spender, Felt::from(0xbu64));
        assert_eq!(approval.amount, U256::from(5000u64));
        assert_eq!(approval.token, Felt::from(0x456u64));
    }

    #[tokio::test]
    async fn test_decode_unknown_event() {
        let decoder = Erc20Decoder::new();

        // Event with unknown selector
        let event = EmittedEvent {
            from_address: Felt::from(0x789u64),
            keys: vec![
                Felt::from(0xdeadbeef_u64), // Unknown selector
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(300),
            transaction_hash: Felt::from(0x1234u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 0); // Should return empty vec for unknown events
    }

    #[tokio::test]
    async fn test_decode_felt_based_modern_transfer() {
        let decoder = Erc20Decoder::new();

        // Felt-based modern format: keys=3 (selector, from, to), data=1 (amount as felt)
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64), // Token contract
            keys: vec![
                Erc20Decoder::transfer_selector(),
                Felt::from(0x1u64), // from
                Felt::from(0x2u64), // to
            ],
            data: vec![
                Felt::from(5000u64), // amount as single felt
            ],
            block_hash: None,
            block_number: Some(100),
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Transfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0x1u64));
        assert_eq!(transfer.to, Felt::from(0x2u64));
        assert_eq!(transfer.amount, U256::from(5000u64));
        assert_eq!(transfer.token, Felt::from(0x123u64));
    }

    #[tokio::test]
    async fn test_decode_felt_based_legacy_transfer() {
        let decoder = Erc20Decoder::new();

        // Felt-based legacy format: keys=1 (selector), data=3 (from, to, amount)
        let event = EmittedEvent {
            from_address: Felt::from(0x456u64), // Token contract
            keys: vec![Erc20Decoder::transfer_selector()],
            data: vec![
                Felt::from(0xau64),  // from
                Felt::from(0xbu64),  // to
                Felt::from(7500u64), // amount as single felt
            ],
            block_hash: None,
            block_number: Some(50),
            transaction_hash: Felt::from(0xef01u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Transfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0xau64));
        assert_eq!(transfer.to, Felt::from(0xbu64));
        assert_eq!(transfer.amount, U256::from(7500u64));
        assert_eq!(transfer.token, Felt::from(0x456u64));
    }

    #[tokio::test]
    async fn test_decode_legacy_approval() {
        let decoder = Erc20Decoder::new();

        // Legacy format: keys=1 (selector), data=4 (owner, spender, amount_low, amount_high)
        let event = EmittedEvent {
            from_address: Felt::from(0x789u64), // Token contract
            keys: vec![Erc20Decoder::approval_selector()],
            data: vec![
                Felt::from(0xcu64),   // owner
                Felt::from(0xdu64),   // spender
                Felt::from(10000u64), // amount_low
                Felt::ZERO,           // amount_high
            ],
            block_hash: None,
            block_number: Some(150),
            transaction_hash: Felt::from(0xabc1u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Approval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0xcu64));
        assert_eq!(approval.spender, Felt::from(0xdu64));
        assert_eq!(approval.amount, U256::from(10000u64));
        assert_eq!(approval.token, Felt::from(0x789u64));
    }

    #[tokio::test]
    async fn test_decode_felt_based_modern_approval() {
        let decoder = Erc20Decoder::new();

        // Felt-based modern format: keys=3 (selector, owner, spender), data=1 (amount as felt)
        let event = EmittedEvent {
            from_address: Felt::from(0xaaau64), // Token contract
            keys: vec![
                Erc20Decoder::approval_selector(),
                Felt::from(0xeu64), // owner
                Felt::from(0xfu64), // spender
            ],
            data: vec![
                Felt::from(8000u64), // amount as single felt
            ],
            block_hash: None,
            block_number: Some(175),
            transaction_hash: Felt::from(0xdef2u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Approval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0xeu64));
        assert_eq!(approval.spender, Felt::from(0xfu64));
        assert_eq!(approval.amount, U256::from(8000u64));
        assert_eq!(approval.token, Felt::from(0xaaau64));
    }

    #[tokio::test]
    async fn test_decode_felt_based_legacy_approval() {
        let decoder = Erc20Decoder::new();

        // Felt-based legacy format: keys=1 (selector), data=3 (owner, spender, amount)
        let event = EmittedEvent {
            from_address: Felt::from(0xbbbu64), // Token contract
            keys: vec![Erc20Decoder::approval_selector()],
            data: vec![
                Felt::from(0x10u64),  // owner
                Felt::from(0x11u64),  // spender
                Felt::from(12000u64), // amount as single felt
            ],
            block_hash: None,
            block_number: Some(180),
            transaction_hash: Felt::from(0xef03u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Approval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0x10u64));
        assert_eq!(approval.spender, Felt::from(0x11u64));
        assert_eq!(approval.amount, U256::from(12000u64));
        assert_eq!(approval.token, Felt::from(0xbbbu64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_transfer_u256() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format: keys=5 (selector, from, to, amount_low, amount_high), data=empty
        let event = EmittedEvent {
            from_address: Felt::from(0x999u64), // Token contract
            keys: vec![
                Erc20Decoder::transfer_selector(),
                Felt::from(0x20u64),  // from
                Felt::from(0x21u64),  // to
                Felt::from(50000u64), // amount_low
                Felt::ZERO,           // amount_high
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(500),
            transaction_hash: Felt::from(0xfff1u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Transfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0x20u64));
        assert_eq!(transfer.to, Felt::from(0x21u64));
        assert_eq!(transfer.amount, U256::from(50000u64));
        assert_eq!(transfer.token, Felt::from(0x999u64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_transfer_felt() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format with felt amount: keys=4 (selector, from, to, amount), data=empty
        let event = EmittedEvent {
            from_address: Felt::from(0xaaau64), // Token contract
            keys: vec![
                Erc20Decoder::transfer_selector(),
                Felt::from(0x30u64),  // from
                Felt::from(0x31u64),  // to
                Felt::from(75000u64), // amount as felt
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(600),
            transaction_hash: Felt::from(0xfff2u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Transfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0x30u64));
        assert_eq!(transfer.to, Felt::from(0x31u64));
        assert_eq!(transfer.amount, U256::from(75000u64));
        assert_eq!(transfer.token, Felt::from(0xaaau64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_approval_u256() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format: keys=5 (selector, owner, spender, amount_low, amount_high), data=empty
        let event = EmittedEvent {
            from_address: Felt::from(0xcccu64), // Token contract
            keys: vec![
                Erc20Decoder::approval_selector(),
                Felt::from(0x40u64),   // owner
                Felt::from(0x41u64),   // spender
                Felt::from(100000u64), // amount_low
                Felt::ZERO,            // amount_high
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(700),
            transaction_hash: Felt::from(0xfff3u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Approval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0x40u64));
        assert_eq!(approval.spender, Felt::from(0x41u64));
        assert_eq!(approval.amount, U256::from(100000u64));
        assert_eq!(approval.token, Felt::from(0xcccu64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_approval_felt() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format with felt amount: keys=4 (selector, owner, spender, amount), data=empty
        let event = EmittedEvent {
            from_address: Felt::from(0xdddu64), // Token contract
            keys: vec![
                Erc20Decoder::approval_selector(),
                Felt::from(0x50u64),   // owner
                Felt::from(0x51u64),   // spender
                Felt::from(125000u64), // amount as felt
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(800),
            transaction_hash: Felt::from(0xfff4u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Approval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0x50u64));
        assert_eq!(approval.spender, Felt::from(0x51u64));
        assert_eq!(approval.amount, U256::from(125000u64));
        assert_eq!(approval.token, Felt::from(0xdddu64));
    }
}
