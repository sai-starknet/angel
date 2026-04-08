//! ERC1155 event decoder (TransferSingle, TransferBatch, ApprovalForAll, URI)

use anyhow::Result;
use async_trait::async_trait;
use primitive_types::U256;
use starknet::core::codec::Decode;
use starknet::core::types::ByteArray;
use starknet::core::utils::parse_cairo_short_string;
use starknet_types_raw::event::EmittedEvent;
use starknet_types_raw::Felt;
use torii::etl::envelope::EventMsg;
use torii::etl::{Decoder, Envelope, EventBody, TypeId};
use torii_common::utils::{felt_pair_to_u256, felt_to_u256};
use torii_types::event::EventContext;

pub const ERC1155_TYPE_ID: TypeId = TypeId::new("erc1155");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferData {
    pub operator: Felt,
    pub from: Felt,
    pub to: Felt,
    pub token_id: U256,
    pub amount: U256,
    pub is_batch: bool,
    pub batch_index: u32,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorApproval {
    pub owner: Felt,
    pub operator: Felt,
    pub approved: bool,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriUpdate {
    pub token: Felt,
    pub token_id: U256,
    pub uri: String,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Erc1155Message {
    Transfer(TransferData),
    ApprovalForAll(OperatorApproval),
    Uri(UriUpdate),
}

pub type Erc1155Body = EventBody<Erc1155Message>;

impl EventMsg for Erc1155Message {
    fn event_id(&self) -> String {
        match self {
            Self::Transfer(transfer) => {
                if transfer.is_batch {
                    format!(
                        "erc1155_transfer_batch_{}_{}_{}",
                        transfer.block_number,
                        format!("{:#x}", transfer.transaction_hash),
                        transfer.batch_index
                    )
                } else {
                    format!(
                        "erc1155_transfer_single_{}_{}",
                        transfer.block_number,
                        format!("{:#x}", transfer.transaction_hash)
                    )
                }
            }
            Self::ApprovalForAll(approval) => {
                format!(
                    "erc1155_approval_for_all_{}_{}",
                    approval.block_number,
                    format!("{:#x}", approval.transaction_hash)
                )
            }
            Self::Uri(uri) => format!(
                "erc1155_uri_{}_{}",
                uri.block_number,
                format!("{:#x}", uri.transaction_hash)
            ),
        }
    }

    fn envelope_type_id(&self) -> TypeId {
        ERC1155_TYPE_ID
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransferPart {
    operator: Felt,
    from: Felt,
    to: Felt,
    token_id: U256,
    amount: U256,
    batch_index: u32,
    is_batch: bool,
}

/// ERC1155 event decoder.
pub struct Erc1155Decoder;

impl Erc1155Decoder {
    pub fn new() -> Self {
        Self
    }

    pub fn transfer_single_selector() -> Felt {
        Felt::selector("TransferSingle")
    }

    pub fn transfer_batch_selector() -> Felt {
        Felt::selector("TransferBatch")
    }

    pub fn approval_for_all_selector() -> Felt {
        Felt::selector("ApprovalForAll")
    }

    pub fn uri_selector() -> Felt {
        Felt::selector("URI")
    }

    fn decode_string_result(result: &[Felt]) -> Option<String> {
        if result.is_empty() {
            return None;
        }

        if result.len() == 1 {
            return parse_cairo_short_string(&to_starknet_felt(result[0]))
                .ok()
                .filter(|s| !s.is_empty());
        }

        let starknet_felts = result
            .iter()
            .copied()
            .map(to_starknet_felt)
            .collect::<Vec<_>>();
        if let Ok(byte_array) = ByteArray::decode(&starknet_felts) {
            if let Ok(s) = String::try_from(byte_array) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        let len = felt_to_usize(result[0]);
        if len > 0 && len < 1000 && result.len() > len {
            let mut out = String::new();
            for felt in &result[1..=len] {
                if let Ok(chunk) = parse_cairo_short_string(&to_starknet_felt(*felt)) {
                    out.push_str(&chunk);
                }
            }
            if !out.is_empty() {
                return Some(out);
            }
        }

        None
    }

    fn parse_transfer_single(&self, event: &EmittedEvent) -> Option<TransferPart> {
        if event.keys.len() == 4 && event.data.len() == 4 {
            return Some(TransferPart {
                operator: event.keys[1],
                from: event.keys[2],
                to: event.keys[3],
                token_id: felt_pair_to_u256(event.data[0], event.data[1]),
                amount: felt_pair_to_u256(event.data[2], event.data[3]),
                batch_index: 0,
                is_batch: false,
            });
        }

        if event.keys.len() == 1 && event.data.len() == 7 {
            return Some(TransferPart {
                operator: event.data[0],
                from: event.data[1],
                to: event.data[2],
                token_id: felt_pair_to_u256(event.data[3], event.data[4]),
                amount: felt_pair_to_u256(event.data[5], event.data[6]),
                batch_index: 0,
                is_batch: false,
            });
        }

        if event.keys.len() == 4 && event.data.len() == 2 {
            return Some(TransferPart {
                operator: event.keys[1],
                from: event.keys[2],
                to: event.keys[3],
                token_id: felt_to_u256(event.data[0]),
                amount: felt_to_u256(event.data[1]),
                batch_index: 0,
                is_batch: false,
            });
        }

        None
    }

    fn parse_u256_slice(items: &[Felt], as_pairs: bool) -> Vec<U256> {
        if as_pairs {
            return items
                .chunks_exact(2)
                .map(|chunk| felt_pair_to_u256(chunk[0], chunk[1]))
                .collect();
        }

        items.iter().copied().map(felt_to_u256).collect()
    }

    fn parse_transfer_batch(&self, event: &EmittedEvent) -> Option<Vec<TransferPart>> {
        let (operator, from, to, mut data_offset) = if event.keys.len() == 4 {
            (event.keys[1], event.keys[2], event.keys[3], 0usize)
        } else if event.keys.len() == 1 && event.data.len() >= 3 {
            (event.data[0], event.data[1], event.data[2], 3usize)
        } else {
            return None;
        };

        if event.data.len() <= data_offset {
            return Some(Vec::new());
        }

        let ids_len = felt_to_usize(event.data[data_offset]);
        data_offset += 1;

        let mut ids = Vec::new();
        let mut values = Vec::new();
        let mut parsed = false;

        for ids_as_pairs in [true, false] {
            let id_words = if ids_as_pairs { 2 } else { 1 };
            let ids_end = data_offset.saturating_add(ids_len.saturating_mul(id_words));
            if ids_end > event.data.len() || ids_end >= event.data.len() {
                continue;
            }

            let candidate_ids =
                Self::parse_u256_slice(&event.data[data_offset..ids_end], ids_as_pairs);
            let values_len = felt_to_usize(event.data[ids_end]);
            let values_start = ids_end + 1;

            for values_as_pairs in [true, false] {
                let value_words = if values_as_pairs { 2 } else { 1 };
                let values_end =
                    values_start.saturating_add(values_len.saturating_mul(value_words));
                if values_end > event.data.len() {
                    continue;
                }

                ids.clone_from(&candidate_ids);
                values =
                    Self::parse_u256_slice(&event.data[values_start..values_end], values_as_pairs);
                parsed = true;
                break;
            }

            if parsed {
                break;
            }
        }

        if !parsed || ids.len() != values.len() {
            return None;
        }

        Some(
            ids.into_iter()
                .zip(values)
                .enumerate()
                .map(|(batch_index, (token_id, amount))| TransferPart {
                    operator,
                    from,
                    to,
                    token_id,
                    amount,
                    batch_index: batch_index as u32,
                    is_batch: true,
                })
                .collect(),
        )
    }

    fn parse_approval_for_all(&self, event: &EmittedEvent) -> Option<OperatorApproval> {
        if event.keys.len() == 3 && event.data.len() == 1 {
            return Some(OperatorApproval {
                owner: event.keys[1],
                operator: event.keys[2],
                approved: event.data[0] != Felt::ZERO,
                token: event.from_address,
                block_number: event.block_number.unwrap_or(0),
                transaction_hash: event.transaction_hash,
            });
        }

        if event.keys.len() == 1 && event.data.len() == 3 {
            return Some(OperatorApproval {
                owner: event.data[0],
                operator: event.data[1],
                approved: event.data[2] != Felt::ZERO,
                token: event.from_address,
                block_number: event.block_number.unwrap_or(0),
                transaction_hash: event.transaction_hash,
            });
        }

        None
    }

    fn parse_uri(&self, event: &EmittedEvent) -> Option<UriUpdate> {
        if event.keys.len() < 2 || event.data.is_empty() {
            return None;
        }

        let uri = Self::decode_string_result(&event.data)?;
        Some(UriUpdate {
            token: event.from_address,
            token_id: felt_to_u256(event.keys[1]),
            uri,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        })
    }

    fn log_malformed(&self, event: &EmittedEvent, event_name: &str) {
        tracing::warn!(
            target: "torii_erc1155::decoder",
            token = %format!("{:#x}", event.from_address),
            tx_hash = %format!("{:#x}", event.transaction_hash),
            block_number = event.block_number.unwrap_or(0),
            keys_len = event.keys.len(),
            data_len = event.data.len(),
            "Malformed ERC1155 {event_name} event"
        );
    }

    fn transfer_envelope(
        &self,
        event: &EmittedEvent,
        context: EventContext,
        part: TransferPart,
    ) -> Envelope {
        Erc1155Message::Transfer(TransferData {
            operator: part.operator,
            from: part.from,
            to: part.to,
            token_id: part.token_id,
            amount: part.amount,
            is_batch: part.is_batch,
            batch_index: part.batch_index,
            token: event.from_address,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        })
        .to_envelope(context)
    }
}

impl Default for Erc1155Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Decoder for Erc1155Decoder {
    fn decoder_name(&self) -> &'static str {
        "erc1155"
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

        let Some(selector) = event.keys.first().copied() else {
            return Ok(Vec::new());
        };

        if selector == Self::transfer_single_selector() {
            return Ok(if let Some(part) = self.parse_transfer_single(&event) {
                vec![self.transfer_envelope(&event, context, part)]
            } else {
                self.log_malformed(&event, "TransferSingle");
                Vec::new()
            });
        }

        if selector == Self::transfer_batch_selector() {
            return Ok(if let Some(parts) = self.parse_transfer_batch(&event) {
                parts
                    .into_iter()
                    .map(|part| self.transfer_envelope(&event, context, part))
                    .collect()
            } else {
                self.log_malformed(&event, "TransferBatch");
                Vec::new()
            });
        }

        if selector == Self::approval_for_all_selector() {
            return Ok(if let Some(approval) = self.parse_approval_for_all(&event) {
                vec![Erc1155Message::ApprovalForAll(approval).to_envelope(context)]
            } else {
                self.log_malformed(&event, "ApprovalForAll");
                Vec::new()
            });
        }

        if selector == Self::uri_selector() {
            return Ok(match self.parse_uri(&event) {
                Some(uri) => vec![Erc1155Message::Uri(uri).to_envelope(context)],
                None => Vec::new(),
            });
        }

        tracing::trace!(
            target: "torii_erc1155::decoder",
            token = %format!("{:#x}", event.from_address),
            selector = %format!("{:#x}", selector),
            keys_len = event.keys.len(),
            data_len = event.data.len(),
            block_number = event.block_number.unwrap_or(0),
            tx_hash = %format!("{:#x}", event.transaction_hash),
            "Unhandled event selector"
        );

        Ok(Vec::new())
    }
}

fn to_starknet_felt(value: Felt) -> starknet::core::types::Felt {
    starknet::core::types::Felt::from_bytes_be(&value.to_be_bytes())
}

fn felt_to_usize(value: Felt) -> usize {
    usize::try_from(value.to_le_words()[0]).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_raw(decoder: &Erc1155Decoder, event: &EmittedEvent) -> Vec<Envelope> {
        futures::executor::block_on(decoder.decode(
            &event.keys,
            &event.data,
            EventContext {
                from_address: event.from_address,
                block_number: event.block_number.unwrap_or(0),
                transaction_hash: event.transaction_hash,
            },
        ))
        .unwrap()
    }

    fn body(envelope: &Envelope) -> &Erc1155Body {
        envelope
            .body
            .as_any()
            .downcast_ref::<Erc1155Body>()
            .unwrap()
    }

    #[tokio::test]
    async fn test_decode_transfer_single_modern() {
        let decoder = Erc1155Decoder::new();
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![
                Erc1155Decoder::transfer_single_selector(),
                Felt::from(0x1u64),
                Felt::from(0x2u64),
                Felt::from(0x3u64),
            ],
            data: vec![
                Felt::from(42u64),
                Felt::ZERO,
                Felt::from(100u64),
                Felt::ZERO,
            ],
            block_hash: None,
            block_number: Some(100),
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decode_raw(&decoder, &event);
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].type_id, ERC1155_TYPE_ID);

        match &body(&envelopes[0]).msg {
            Erc1155Message::Transfer(transfer) => {
                assert_eq!(transfer.operator, Felt::from(0x1u64));
                assert_eq!(transfer.from, Felt::from(0x2u64));
                assert_eq!(transfer.to, Felt::from(0x3u64));
                assert_eq!(transfer.token_id, U256::from(42u64));
                assert_eq!(transfer.amount, U256::from(100u64));
                assert!(!transfer.is_batch);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_decode_approval_for_all() {
        let decoder = Erc1155Decoder::new();
        let event = EmittedEvent {
            from_address: Felt::from(0x456u64),
            keys: vec![
                Erc1155Decoder::approval_for_all_selector(),
                Felt::from(0xau64),
                Felt::from(0xbu64),
            ],
            data: vec![Felt::from(1u64)],
            block_hash: None,
            block_number: Some(200),
            transaction_hash: Felt::from(0xef01u64),
        };

        let envelopes = decode_raw(&decoder, &event);
        assert_eq!(envelopes.len(), 1);

        match &body(&envelopes[0]).msg {
            Erc1155Message::ApprovalForAll(approval) => {
                assert_eq!(approval.owner, Felt::from(0xau64));
                assert_eq!(approval.operator, Felt::from(0xbu64));
                assert!(approval.approved);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_decode_transfer_single_single_felt_preserves_full_felt() {
        let decoder = Erc1155Decoder::new();
        let id_felt = Felt::from_hex("0x100000000000000000000000000000001").unwrap();
        let value_felt = Felt::from_hex("0x200000000000000000000000000000003").unwrap();
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![
                Erc1155Decoder::transfer_single_selector(),
                Felt::from(0x1u64),
                Felt::from(0x2u64),
                Felt::from(0x3u64),
            ],
            data: vec![id_felt, value_felt],
            block_hash: None,
            block_number: Some(101),
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decode_raw(&decoder, &event);
        match &body(&envelopes[0]).msg {
            Erc1155Message::Transfer(transfer) => {
                assert_eq!(transfer.token_id, felt_to_u256(id_felt));
                assert_eq!(transfer.amount, felt_to_u256(value_felt));
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_decode_transfer_batch_single_felt_arrays() {
        let decoder = Erc1155Decoder::new();
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![
                Erc1155Decoder::transfer_batch_selector(),
                Felt::from(0x1u64),
                Felt::from(0x2u64),
                Felt::from(0x3u64),
            ],
            data: vec![
                Felt::from(2u64),
                Felt::from(11u64),
                Felt::from(12u64),
                Felt::from(2u64),
                Felt::from(101u64),
                Felt::from(102u64),
            ],
            block_hash: None,
            block_number: Some(102),
            transaction_hash: Felt::from(0xabcfu64),
        };

        let envelopes = decode_raw(&decoder, &event);
        assert_eq!(envelopes.len(), 2);

        match (&body(&envelopes[0]).msg, &body(&envelopes[1]).msg) {
            (Erc1155Message::Transfer(first), Erc1155Message::Transfer(second)) => {
                assert_eq!(first.token_id, U256::from(11u64));
                assert_eq!(first.amount, U256::from(101u64));
                assert!(first.is_batch);
                assert_eq!(first.batch_index, 0);
                assert_eq!(second.token_id, U256::from(12u64));
                assert_eq!(second.amount, U256::from(102u64));
                assert_eq!(second.batch_index, 1);
            }
            other => panic!("unexpected messages: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_decode_uri_event() {
        let decoder = Erc1155Decoder::new();
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![Erc1155Decoder::uri_selector(), Felt::from(7u64)],
            data: vec![Felt::from(0x616263u64)],
            block_hash: None,
            block_number: Some(103),
            transaction_hash: Felt::from(0xabd0u64),
        };

        let envelopes = decode_raw(&decoder, &event);
        assert_eq!(envelopes.len(), 1);

        match &body(&envelopes[0]).msg {
            Erc1155Message::Uri(uri) => {
                assert_eq!(uri.token, Felt::from(0x123u64));
                assert_eq!(uri.token_id, U256::from(7u64));
                assert_eq!(uri.uri, "abc");
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
