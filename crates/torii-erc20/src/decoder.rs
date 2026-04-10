//! ERC20 event decoder (Transfer + Approval)

use anyhow::Result;
use async_trait::async_trait;
use starknet_types_raw::Felt;
use torii::etl::{Decoder, Envelope, EventMsg};
use torii_common::utils::U256ParseError;
use torii_types::event::EventContext;

use crate::event::{Erc20Event, Erc20Msg};

pub const TRANSFER_SELECTOR: Felt = Felt::selector("Transfer");
pub const APPROVAL_SELECTOR: Felt = Felt::selector("Approval");

#[derive(Debug, Clone)]
pub enum TransferFormat {
    Standard,  // from, to in keys; amount_low, amount_high in data
    AllInKeys, // from, to, amount_low, amount_high all in keys
    Legacy,    // from, to, amount_low, amount_high all in data
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum Erc20DecodeError {
    #[error("Invalid format: keys_len = {0}, data_len = {1}")]
    InvalidFormat(usize, usize), // keys_len, data_len
    #[error(transparent)]
    U256ParseError(#[from] U256ParseError),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum Erc20EventError {
    #[error("Failed to decode Transfer event: {0}")]
    Transfer(Erc20DecodeError),
    #[error("Failed to decode Approval event: {0}")]
    Approval(Erc20DecodeError),
    #[error("Selector does not match Transfer or Approval: {0:#x}")]
    InvalidSelector(Felt),
    #[error("Selector Missing: Event keys are empty")]
    MissingSelector,
}

impl Erc20EventError {
    pub fn transfer(err: Erc20DecodeError) -> Self {
        Erc20EventError::Transfer(err)
    }

    pub fn approval(err: Erc20DecodeError) -> Self {
        Erc20EventError::Approval(err)
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

// Helper function to decode Events event based on format variants, keys DOES NOT include selector (already stripped)
impl Erc20Event {
    pub fn decode(keys: &[Felt], data: &[Felt]) -> Result<Self, Erc20DecodeError> {
        match (keys.len(), data.len()) {
            (2, 0 | 1 | 2) => {
                Erc20Event::new_from_felts(keys[0], keys[1], data).map_err(Into::into)
            }
            (3 | 4, 0) => {
                Erc20Event::new_from_felts(keys[0], keys[1], &keys[2..]).map_err(Into::into)
            }
            (0, 2 | 3 | 4) => {
                Erc20Event::new_from_felts(data[0], data[1], &data[2..]).map_err(Into::into)
            }
            _ => Err(Erc20DecodeError::InvalidFormat(keys.len(), data.len())),
        }
    }
}

impl Erc20Msg {
    pub fn decode(keys: &[Felt], data: &[Felt]) -> Result<Self, Erc20EventError> {
        match keys.get(0) {
            Some(&TRANSFER_SELECTOR) => Self::decode_transfer(&keys[1..], data),
            Some(&APPROVAL_SELECTOR) => Self::decode_approval(&keys[1..], data),
            Some(selector) => Err(Erc20EventError::InvalidSelector(*selector)),
            None => Err(Erc20EventError::MissingSelector),
        }
    }
    pub fn decode_approval(keys: &[Felt], data: &[Felt]) -> Result<Self, Erc20EventError> {
        Erc20Event::decode(keys, data)
            .map_err(Erc20EventError::approval)
            .map(Erc20Event::approval)
    }

    pub fn decode_transfer(keys: &[Felt], data: &[Felt]) -> Result<Self, Erc20EventError> {
        Erc20Event::decode(keys, data)
            .map_err(Erc20EventError::transfer)
            .map(Erc20Event::transfer)
    }
}

pub struct Erc20Decoder;

impl Erc20Decoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Erc20Decoder {
    fn default() -> Self {
        Self
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
        match Erc20Msg::decode(keys, data) {
            Ok(msg) => msg.to_ok_envelopes(context),
            Err(Erc20EventError::InvalidSelector(_) | Erc20EventError::MissingSelector) => {
                Ok(vec![])
            }
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {

    impl Erc20Msg {
        fn unwrap_transfer(self) -> TransferInfo {
            match self {
                Erc20Msg::Transfer(transfer) => transfer,
                _ => panic!("Expected Transfer message"),
            }
        }

        fn unwrap_approval(self) -> ApprovalMsg {
            match self {
                Erc20Msg::Approval(approval) => approval,
                _ => panic!("Expected Approval message"),
            }
        }
    }

    use primitive_types::U256;
    use torii::etl::StarknetEvent;

    use crate::event::{ApprovalMsg, Erc20Body, TransferInfo};

    use super::*;

    #[tokio::test]
    async fn test_decode_transfer() {
        let decoder = Erc20Decoder::new();

        let event = StarknetEvent {
            from_address: Felt::from(0x123u64), // Token contract
            keys: vec![
                TRANSFER_SELECTOR,
                Felt::from(0x1u64), // from
                Felt::from(0x2u64), // to
            ],
            data: vec![
                Felt::from(1000u64), // amount_low
                Felt::ZERO,          // amount_high
            ],
            block_number: 100,
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let transfer = msg.unwrap_transfer();

        assert_eq!(transfer.from, Felt::from(0x1u64));
        assert_eq!(transfer.to, Felt::from(0x2u64));
        assert_eq!(transfer.amount, U256::from(1000u64));
        assert_eq!(context.from_address, Felt::from(0x123u64));
    }

    #[tokio::test]
    async fn test_decode_approval() {
        let decoder = Erc20Decoder::new();

        let event = StarknetEvent {
            from_address: Felt::from(0x456u64), // Token contract
            keys: vec![
                APPROVAL_SELECTOR,
                Felt::from(0xau64), // owner
                Felt::from(0xbu64), // spender
            ],
            data: vec![
                Felt::from(5000u64), // amount_low
                Felt::ZERO,          // amount_high
            ],
            block_number: 200,
            transaction_hash: Felt::from(0xdef0u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let approval = msg.unwrap_approval();

        assert_eq!(approval.owner, Felt::from(0xau64));
        assert_eq!(approval.spender, Felt::from(0xbu64));
        assert_eq!(approval.amount, U256::from(5000u64));
        assert_eq!(context.from_address, Felt::from(0x456u64));
    }

    #[tokio::test]
    async fn test_decode_unknown_event() {
        let decoder = Erc20Decoder::new();

        // Event with unknown selector
        let event = StarknetEvent {
            from_address: Felt::from(0x789u64),
            keys: vec![
                Felt::from(0xdeadbeef_u64), // Unknown selector
            ],
            data: vec![],
            block_number: 300,
            transaction_hash: Felt::from(0x1234u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 0); // Should return empty vec for unknown events
    }

    #[tokio::test]
    async fn test_decode_felt_based_modern_transfer() {
        let decoder = Erc20Decoder::new();

        // Felt-based modern format: keys=3 (selector, from, to), data=1 (amount as felt)
        let event = StarknetEvent {
            from_address: Felt::from(0x123u64), // Token contract
            keys: vec![
                TRANSFER_SELECTOR,
                Felt::from(0x1u64), // from
                Felt::from(0x2u64), // to
            ],
            data: vec![
                Felt::from(5000u64), // amount as single felt
            ],
            block_number: 100,
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let transfer = msg.unwrap_transfer();

        assert_eq!(transfer.from, Felt::from(0x1u64));
        assert_eq!(transfer.to, Felt::from(0x2u64));
        assert_eq!(transfer.amount, U256::from(5000u64));
        assert_eq!(context.from_address, Felt::from(0x123u64));
    }

    #[tokio::test]
    async fn test_decode_felt_based_legacy_transfer() {
        let decoder = Erc20Decoder::new();

        // Felt-based legacy format: keys=1 (selector), data=3 (from, to, amount)
        let event = StarknetEvent {
            from_address: Felt::from(0x456u64), // Token contract
            keys: vec![TRANSFER_SELECTOR],
            data: vec![
                Felt::from(0xau64),  // from
                Felt::from(0xbu64),  // to
                Felt::from(7500u64), // amount as single felt
            ],
            block_number: 50,
            transaction_hash: Felt::from(0xef01u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let transfer = msg.unwrap_transfer();

        assert_eq!(transfer.from, Felt::from(0xau64));
        assert_eq!(transfer.to, Felt::from(0xbu64));
        assert_eq!(transfer.amount, U256::from(7500u64));
        assert_eq!(context.from_address, Felt::from(0x456u64));
    }

    #[tokio::test]
    async fn test_decode_legacy_approval() {
        let decoder = Erc20Decoder::new();

        // Legacy format: keys=1 (selector), data=4 (owner, spender, amount_low, amount_high)
        let event = StarknetEvent {
            from_address: Felt::from(0x789u64), // Token contract
            keys: vec![APPROVAL_SELECTOR],
            data: vec![
                Felt::from(0xcu64),   // owner
                Felt::from(0xdu64),   // spender
                Felt::from(10000u64), // amount_low
                Felt::ZERO,           // amount_high
            ],
            block_number: 150,
            transaction_hash: Felt::from(0xabc1u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let approval = msg.unwrap_approval();

        assert_eq!(approval.owner, Felt::from(0xcu64));
        assert_eq!(approval.spender, Felt::from(0xdu64));
        assert_eq!(approval.amount, U256::from(10000u64));
        assert_eq!(context.from_address, Felt::from(0x789u64));
    }

    #[tokio::test]
    async fn test_decode_felt_based_modern_approval() {
        let decoder = Erc20Decoder::new();

        // Felt-based modern format: keys=3 (selector, owner, spender), data=1 (amount as felt)
        let event = StarknetEvent {
            from_address: Felt::from(0xaaau64), // Token contract
            keys: vec![
                APPROVAL_SELECTOR,
                Felt::from(0xeu64), // owner
                Felt::from(0xfu64), // spender
            ],
            data: vec![
                Felt::from(8000u64), // amount as single felt
            ],
            block_number: 175,
            transaction_hash: Felt::from(0xdef2u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let approval = msg.unwrap_approval();

        assert_eq!(approval.owner, Felt::from(0xeu64));
        assert_eq!(approval.spender, Felt::from(0xfu64));
        assert_eq!(approval.amount, U256::from(8000u64));
        assert_eq!(context.from_address, Felt::from(0xaaau64));
    }

    #[tokio::test]
    async fn test_decode_felt_based_legacy_approval() {
        let decoder = Erc20Decoder::new();

        // Felt-based legacy format: keys=1 (selector), data=3 (owner, spender, amount)
        let event = StarknetEvent {
            from_address: Felt::from(0xbbbu64), // Token contract
            keys: vec![APPROVAL_SELECTOR],
            data: vec![
                Felt::from(0x10u64),  // owner
                Felt::from(0x11u64),  // spender
                Felt::from(12000u64), // amount as single felt
            ],
            block_number: 180,
            transaction_hash: Felt::from(0xef03u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let approval = msg.unwrap_approval();

        assert_eq!(approval.owner, Felt::from(0x10u64));
        assert_eq!(approval.spender, Felt::from(0x11u64));
        assert_eq!(approval.amount, U256::from(12000u64));
        assert_eq!(context.from_address, Felt::from(0xbbbu64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_transfer_u256() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format: keys=5 (selector, from, to, amount_low, amount_high), data=empty
        let event = StarknetEvent {
            from_address: Felt::from(0x999u64), // Token contract
            keys: vec![
                TRANSFER_SELECTOR,
                Felt::from(0x20u64),  // from
                Felt::from(0x21u64),  // to
                Felt::from(50000u64), // amount_low
                Felt::ZERO,           // amount_high
            ],
            data: vec![],
            block_number: 500,
            transaction_hash: Felt::from(0xfff1u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let transfer = msg.unwrap_transfer();

        assert_eq!(transfer.from, Felt::from(0x20u64));
        assert_eq!(transfer.to, Felt::from(0x21u64));
        assert_eq!(transfer.amount, U256::from(50000u64));
        assert_eq!(context.from_address, Felt::from(0x999u64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_transfer_felt() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format with felt amount: keys=4 (selector, from, to, amount), data=empty
        let event = StarknetEvent {
            from_address: Felt::from(0xaaau64), // Token contract
            keys: vec![
                TRANSFER_SELECTOR,
                Felt::from(0x30u64),  // from
                Felt::from(0x31u64),  // to
                Felt::from(75000u64), // amount as felt
            ],
            data: vec![],
            block_number: 600,
            transaction_hash: Felt::from(0xfff2u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let transfer = msg.unwrap_transfer();

        assert_eq!(transfer.from, Felt::from(0x30u64));
        assert_eq!(transfer.to, Felt::from(0x31u64));
        assert_eq!(transfer.amount, U256::from(75000u64));
        assert_eq!(context.from_address, Felt::from(0xaaau64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_approval_u256() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format: keys=5 (selector, owner, spender, amount_low, amount_high), data=empty
        let event = StarknetEvent {
            from_address: Felt::from(0xcccu64), // Token contract
            keys: vec![
                APPROVAL_SELECTOR,
                Felt::from(0x40u64),   // owner
                Felt::from(0x41u64),   // spender
                Felt::from(100000u64), // amount_low
                Felt::ZERO,            // amount_high
            ],
            data: vec![],
            block_number: 700,
            transaction_hash: Felt::from(0xfff3u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let approval = msg.unwrap_approval();

        assert_eq!(approval.owner, Felt::from(0x40u64));
        assert_eq!(approval.spender, Felt::from(0x41u64));
        assert_eq!(approval.amount, U256::from(100000u64));
        assert_eq!(context.from_address, Felt::from(0xcccu64));
    }

    #[tokio::test]
    async fn test_decode_all_in_keys_approval_felt() {
        let decoder = Erc20Decoder::new();

        // All-in-keys format with felt amount: keys=4 (selector, owner, spender, amount), data=empty
        let event = StarknetEvent {
            from_address: Felt::from(0xdddu64), // Token contract
            keys: vec![
                APPROVAL_SELECTOR,
                Felt::from(0x50u64),   // owner
                Felt::from(0x51u64),   // spender
                Felt::from(125000u64), // amount as felt
            ],
            data: vec![],
            block_number: 800,
            transaction_hash: Felt::from(0xfff4u64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let (msg, context) = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc20Body>()
            .unwrap()
            .into();
        let approval = msg.unwrap_approval();

        assert_eq!(approval.owner, Felt::from(0x50u64));
        assert_eq!(approval.spender, Felt::from(0x51u64));
        assert_eq!(approval.amount, U256::from(125000u64));
        assert_eq!(context.from_address, Felt::from(0xdddu64));
    }
}
