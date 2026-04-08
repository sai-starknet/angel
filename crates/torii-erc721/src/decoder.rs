//! ERC721 event decoder (Transfer, Approval, ApprovalForAll)

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{Felt, U256};
use starknet::macros::selector;
use starknet_types_raw::event::EmittedEvent;
use starknet_types_raw::Felt as RawFelt;
use std::any::Any;
use torii::etl::{Decoder, Envelope, EventContext, TypeId, TypedBody};

pub const ERC721_TYPE: TypeId = TypeId::new("erc721");

/// Transfer event from ERC721 token
#[derive(Debug, Clone)]
pub struct NftTransfer {
    pub from: Felt,
    pub to: Felt,
    /// Token ID as U256 (256-bit)
    pub token_id: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

/// Approval event from ERC721 token (single token approval)
#[derive(Debug, Clone)]
pub struct NftApproval {
    pub owner: Felt,
    pub approved: Felt,
    /// Token ID as U256 (256-bit)
    pub token_id: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

/// ApprovalForAll event from ERC721 token (operator approval)
#[derive(Debug, Clone)]
pub struct OperatorApproval {
    pub owner: Felt,
    pub operator: Felt,
    pub approved: bool,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

/// MetadataUpdate event (EIP-4906) — single token
#[derive(Debug, Clone)]
pub struct MetadataUpdate {
    pub token: Felt,
    pub token_id: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

/// BatchMetadataUpdate event (EIP-4906) — range of tokens
#[derive(Debug, Clone)]
pub struct BatchMetadataUpdate {
    pub token: Felt,
    pub from_token_id: U256,
    pub to_token_id: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Clone)]
pub enum Erc721Msg {
    Transfer(NftTransfer),
    Approval(NftApproval),
    ApprovalForAll(OperatorApproval),
    MetadataUpdate(MetadataUpdate),
    BatchMetadataUpdate(BatchMetadataUpdate),
}

#[derive(Debug, Clone, Copy)]
pub struct Erc721Metadata {
    pub block_number: u64,
    pub transaction_hash: Felt,
    pub from_address: Felt,
}

#[derive(Debug, Clone)]
pub struct Erc721Body {
    pub metadata: Erc721Metadata,
    pub msg: Erc721Msg,
}

impl TypedBody for Erc721Body {
    fn envelope_type_id(&self) -> TypeId {
        ERC721_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Erc721Msg {
    fn event_id(&self) -> String {
        match self {
            Self::Transfer(msg) => {
                format!(
                    "erc721.transfer.{}.{}.{:#x}.",
                    msg.block_number, msg.transaction_hash, msg.token
                ) + &msg.token_id.to_string()
            }
            Self::Approval(msg) => {
                format!(
                    "erc721.approval.{}.{}.{:#x}.",
                    msg.block_number, msg.transaction_hash, msg.token
                ) + &msg.token_id.to_string()
            }
            Self::ApprovalForAll(msg) => format!(
                "erc721.approval_for_all.{}.{:#x}.{:#x}.{:#x}",
                msg.block_number, msg.transaction_hash, msg.token, msg.owner
            ),
            Self::MetadataUpdate(msg) => {
                format!(
                    "erc721.metadata_update.{}.{}.{:#x}.",
                    msg.block_number, msg.transaction_hash, msg.token
                ) + &msg.token_id.to_string()
            }
            Self::BatchMetadataUpdate(msg) => {
                format!(
                    "erc721.batch_metadata_update.{}.{}.{:#x}.{}.",
                    msg.block_number, msg.transaction_hash, msg.token, msg.from_token_id
                ) + &msg.to_token_id.to_string()
            }
        }
    }

    fn into_envelope(self, event: &EmittedEvent) -> Envelope {
        Envelope::new(
            self.event_id(),
            Box::new(Erc721Body {
                metadata: Erc721Metadata {
                    block_number: event.block_number.unwrap_or(0),
                    transaction_hash: Erc721Decoder::felt(event.transaction_hash),
                    from_address: Erc721Decoder::felt(event.from_address),
                },
                msg: self,
            }),
            Default::default(),
        )
    }
}

/// ERC721 event decoder
///
/// Decodes multiple ERC721 events:
/// - Transfer(from, to, token_id)
/// - Approval(owner, approved, token_id)
/// - ApprovalForAll(owner, operator, approved)
/// - MetadataUpdate(token_id) — EIP-4906
/// - BatchMetadataUpdate(from_token_id, to_token_id) — EIP-4906
///
/// Supports both modern (keys) and legacy (data-only) formats from OpenZeppelin.
pub struct Erc721Decoder;

impl Erc721Decoder {
    pub fn new() -> Self {
        Self
    }

    /// Transfer event selector: sn_keccak("Transfer")
    fn transfer_selector() -> Felt {
        selector!("Transfer")
    }

    /// Approval event selector: sn_keccak("Approval")
    fn approval_selector() -> Felt {
        selector!("Approval")
    }

    /// ApprovalForAll event selector: sn_keccak("ApprovalForAll")
    fn approval_for_all_selector() -> Felt {
        selector!("ApprovalForAll")
    }

    /// MetadataUpdate event selector (EIP-4906): sn_keccak("MetadataUpdate")
    fn metadata_update_selector() -> Felt {
        selector!("MetadataUpdate")
    }

    /// BatchMetadataUpdate event selector (EIP-4906): sn_keccak("BatchMetadataUpdate")
    fn batch_metadata_update_selector() -> Felt {
        selector!("BatchMetadataUpdate")
    }

    fn felt(value: RawFelt) -> Felt {
        Felt::from_bytes_be(&value.to_be_bytes())
    }

    fn felt_to_u128(value: RawFelt) -> u128 {
        value.try_into().unwrap_or(0)
    }

    fn parse_split_or_felt_u256(first: RawFelt, second: Option<RawFelt>) -> U256 {
        match second {
            Some(high) => U256::from_words(Self::felt_to_u128(first), Self::felt_to_u128(high)),
            None => U256::from(Self::felt_to_u128(first)),
        }
    }

    fn parse_address_pair_and_token_id(event: &EmittedEvent) -> Option<(Felt, Felt, U256)> {
        if event.keys.len() == 5 && event.data.is_empty() {
            return Some((
                Self::felt(event.keys[1]),
                Self::felt(event.keys[2]),
                Self::parse_split_or_felt_u256(event.keys[3], Some(event.keys[4])),
            ));
        }

        if event.keys.len() == 1 && event.data.len() == 4 {
            return Some((
                Self::felt(event.data[0]),
                Self::felt(event.data[1]),
                Self::parse_split_or_felt_u256(event.data[2], Some(event.data[3])),
            ));
        }

        if event.keys.len() == 4 && event.data.is_empty() {
            return Some((
                Self::felt(event.keys[1]),
                Self::felt(event.keys[2]),
                Self::parse_split_or_felt_u256(event.keys[3], None),
            ));
        }

        if event.keys.len() == 1 && event.data.len() == 3 {
            return Some((
                Self::felt(event.data[0]),
                Self::felt(event.data[1]),
                Self::parse_split_or_felt_u256(event.data[2], None),
            ));
        }

        None
    }

    fn parse_operator_approval(event: &EmittedEvent) -> Option<(Felt, Felt, bool)> {
        if event.keys.len() == 3 && event.data.len() == 1 {
            return Some((
                Self::felt(event.keys[1]),
                Self::felt(event.keys[2]),
                event.data[0] != RawFelt::ZERO,
            ));
        }

        if event.keys.len() == 1 && event.data.len() == 3 {
            return Some((
                Self::felt(event.data[0]),
                Self::felt(event.data[1]),
                event.data[2] != RawFelt::ZERO,
            ));
        }

        None
    }

    fn parse_single_token_id(event: &EmittedEvent) -> Option<U256> {
        if event.data.len() >= 2 {
            return Some(Self::parse_split_or_felt_u256(
                event.data[0],
                Some(event.data[1]),
            ));
        }

        if event.keys.len() >= 3 {
            return Some(Self::parse_split_or_felt_u256(
                event.keys[1],
                Some(event.keys[2]),
            ));
        }

        if event.data.len() == 1 {
            return Some(Self::parse_split_or_felt_u256(event.data[0], None));
        }

        if event.keys.len() == 2 {
            return Some(Self::parse_split_or_felt_u256(event.keys[1], None));
        }

        None
    }

    fn parse_token_range(event: &EmittedEvent) -> Option<(U256, U256)> {
        if event.data.len() >= 4 {
            return Some((
                Self::parse_split_or_felt_u256(event.data[0], Some(event.data[1])),
                Self::parse_split_or_felt_u256(event.data[2], Some(event.data[3])),
            ));
        }

        if event.keys.len() >= 5 {
            return Some((
                Self::parse_split_or_felt_u256(event.keys[1], Some(event.keys[2])),
                Self::parse_split_or_felt_u256(event.keys[3], Some(event.keys[4])),
            ));
        }

        None
    }

    fn log_malformed_event(&self, name: &str, event: &EmittedEvent) {
        tracing::warn!(
            target: "torii_erc721::decoder",
            token = %format!("{:#x}", event.from_address),
            tx_hash = %format!("{:#x}", event.transaction_hash),
            block_number = event.block_number.unwrap_or(0),
            keys_len = event.keys.len(),
            data_len = event.data.len(),
            "Malformed ERC721 {name} event"
        );
    }

    /// Decode Transfer event into a message.
    async fn decode_transfer(&self, event: &EmittedEvent) -> Result<Option<Erc721Msg>> {
        let Some((from, to, token_id)) = Self::parse_address_pair_and_token_id(event) else {
            self.log_malformed_event("Transfer", event);
            return Ok(None);
        };

        Ok(Some(Erc721Msg::Transfer(NftTransfer {
            from,
            to,
            token_id,
            token: Self::felt(event.from_address),
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: Self::felt(event.transaction_hash),
        })))
    }

    /// Decode Approval event into a message.
    async fn decode_approval(&self, event: &EmittedEvent) -> Result<Option<Erc721Msg>> {
        let Some((owner, approved, token_id)) = Self::parse_address_pair_and_token_id(event) else {
            self.log_malformed_event("Approval", event);
            return Ok(None);
        };

        Ok(Some(Erc721Msg::Approval(NftApproval {
            owner,
            approved,
            token_id,
            token: Self::felt(event.from_address),
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: Self::felt(event.transaction_hash),
        })))
    }

    /// Decode ApprovalForAll event into a message.
    async fn decode_approval_for_all(&self, event: &EmittedEvent) -> Result<Option<Erc721Msg>> {
        let Some((owner, operator, approved)) = Self::parse_operator_approval(event) else {
            self.log_malformed_event("ApprovalForAll", event);
            return Ok(None);
        };

        Ok(Some(Erc721Msg::ApprovalForAll(OperatorApproval {
            owner,
            operator,
            approved,
            token: Self::felt(event.from_address),
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: Self::felt(event.transaction_hash),
        })))
    }

    /// Decode MetadataUpdate event (EIP-4906).
    async fn decode_metadata_update(&self, event: &EmittedEvent) -> Result<Option<Erc721Msg>> {
        let Some(token_id) = Self::parse_single_token_id(event) else {
            self.log_malformed_event("MetadataUpdate", event);
            return Ok(None);
        };

        Ok(Some(Erc721Msg::MetadataUpdate(MetadataUpdate {
            token: Self::felt(event.from_address),
            token_id,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: Self::felt(event.transaction_hash),
        })))
    }

    /// Decode BatchMetadataUpdate event (EIP-4906).
    async fn decode_batch_metadata_update(
        &self,
        event: &EmittedEvent,
    ) -> Result<Option<Erc721Msg>> {
        let Some((from_token_id, to_token_id)) = Self::parse_token_range(event) else {
            self.log_malformed_event("BatchMetadataUpdate", event);
            return Ok(None);
        };

        Ok(Some(Erc721Msg::BatchMetadataUpdate(BatchMetadataUpdate {
            token: Self::felt(event.from_address),
            from_token_id,
            to_token_id,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: Self::felt(event.transaction_hash),
        })))
    }

    async fn decode_raw_event(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
        if event.keys.is_empty() {
            return Ok(Vec::new());
        }

        let selector = Self::felt(event.keys[0]);
        let msg = if selector == Self::transfer_selector() {
            self.decode_transfer(event).await?
        } else if selector == Self::approval_selector() {
            self.decode_approval(event).await?
        } else if selector == Self::approval_for_all_selector() {
            self.decode_approval_for_all(event).await?
        } else if selector == Self::metadata_update_selector() {
            self.decode_metadata_update(event).await?
        } else if selector == Self::batch_metadata_update_selector() {
            self.decode_batch_metadata_update(event).await?
        } else {
            tracing::trace!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", event.from_address),
                selector = %format!("{:#x}", selector),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                block_number = event.block_number.unwrap_or(0),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                "Unhandled event selector"
            );
            None
        };

        Ok(msg
            .into_iter()
            .map(|msg| msg.into_envelope(event))
            .collect())
    }
}

impl Default for Erc721Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Decoder for Erc721Decoder {
    fn decoder_name(&self) -> &'static str {
        "erc721"
    }

    async fn decode(
        &self,
        keys: &[RawFelt],
        data: &[RawFelt],
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

        self.decode_raw_event(&event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use torii::etl::StarknetEvent;

    fn raw(value: u64) -> RawFelt {
        RawFelt::from(value)
    }

    fn raw_selector(selector: Felt) -> RawFelt {
        RawFelt::from_be_bytes(selector.to_bytes_be()).unwrap()
    }

    fn starknet_event(event: EmittedEvent) -> StarknetEvent {
        StarknetEvent::new(
            event.from_address,
            event.keys,
            event.data,
            event.block_number.unwrap_or(0),
            event.transaction_hash,
        )
    }

    fn decode_body(event: EmittedEvent) -> Erc721Body {
        let decoder = Erc721Decoder::new();
        let envelopes =
            futures::executor::block_on(decoder.decode_event(&starknet_event(event))).unwrap();
        assert_eq!(envelopes.len(), 1);
        envelopes[0]
            .body
            .as_any()
            .downcast_ref::<Erc721Body>()
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn test_decode_modern_transfer() {
        let event = EmittedEvent {
            from_address: raw(0x123),
            keys: vec![
                raw_selector(Erc721Decoder::transfer_selector()),
                raw(0x1),
                raw(0x2),
                raw(42),
                RawFelt::ZERO,
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(100),
            transaction_hash: raw(0xabcd),
        };

        let body = decode_body(event);
        match body.msg {
            Erc721Msg::Transfer(transfer) => {
                assert_eq!(transfer.from, Felt::from(0x1u64));
                assert_eq!(transfer.to, Felt::from(0x2u64));
                assert_eq!(transfer.token_id, U256::from(42u64));
                assert_eq!(transfer.token, Felt::from(0x123u64));
            }
            msg => panic!("unexpected message: {msg:?}"),
        }
    }

    #[tokio::test]
    async fn test_decode_legacy_transfer() {
        let event = EmittedEvent {
            from_address: raw(0x456),
            keys: vec![raw_selector(Erc721Decoder::transfer_selector())],
            data: vec![raw(0xa), raw(0xb), raw(100), RawFelt::ZERO],
            block_hash: None,
            block_number: Some(200),
            transaction_hash: raw(0xef01),
        };

        let body = decode_body(event);
        match body.msg {
            Erc721Msg::Transfer(transfer) => {
                assert_eq!(transfer.from, Felt::from(0xau64));
                assert_eq!(transfer.to, Felt::from(0xbu64));
                assert_eq!(transfer.token_id, U256::from(100u64));
            }
            msg => panic!("unexpected message: {msg:?}"),
        }
    }

    #[tokio::test]
    async fn test_decode_approval_for_all() {
        let event = EmittedEvent {
            from_address: raw(0x789),
            keys: vec![
                raw_selector(Erc721Decoder::approval_for_all_selector()),
                raw(0xc),
                raw(0xd),
            ],
            data: vec![raw(1)],
            block_hash: None,
            block_number: Some(300),
            transaction_hash: raw(0x2345),
        };

        let body = decode_body(event);
        match body.msg {
            Erc721Msg::ApprovalForAll(approval) => {
                assert_eq!(approval.owner, Felt::from(0xcu64));
                assert_eq!(approval.operator, Felt::from(0xdu64));
                assert!(approval.approved);
            }
            msg => panic!("unexpected message: {msg:?}"),
        }
    }
}
