//! ERC1155 event decoder (TransferSingle, TransferBatch, ApprovalForAll, URI)

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::codec::Decode;
use starknet::core::types::{ByteArray, EmittedEvent, Felt, U256};
use starknet::core::utils::parse_cairo_short_string;
use starknet::macros::selector;
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypedBody};
use torii_common::bytes_to_u256;

/// TransferSingle event from ERC1155 token
#[derive(Debug, Clone)]
pub struct TransferSingle {
    pub operator: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Token ID as U256 (256-bit)
    pub id: U256,
    /// Amount as U256 (256-bit)
    pub value: U256,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for TransferSingle {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc1155.transfer_single")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// TransferBatch event from ERC1155 token (denormalized into individual transfers)
#[derive(Debug, Clone)]
pub struct TransferBatch {
    pub operator: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Token ID for this specific transfer in the batch
    pub id: U256,
    /// Amount for this specific transfer in the batch
    pub value: U256,
    /// Index in the original batch
    pub batch_index: u32,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for TransferBatch {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc1155.transfer_batch")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ApprovalForAll event from ERC1155 token
#[derive(Debug, Clone)]
pub struct OperatorApproval {
    pub owner: Felt,
    pub operator: Felt,
    pub approved: bool,
    pub token: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for OperatorApproval {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc1155.approval_for_all")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// URI event from ERC1155 token
#[derive(Debug, Clone)]
pub struct UriUpdate {
    pub token: Felt,
    pub token_id: U256,
    pub uri: String,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for UriUpdate {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc1155.uri")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// ERC1155 event decoder
///
/// Decodes multiple ERC1155 events:
/// - TransferSingle(operator, from, to, id, value)
/// - TransferBatch(operator, from, to, ids, values)
/// - ApprovalForAll(owner, operator, approved)
///
/// Supports both modern (keys) and legacy (data-only) formats.
pub struct Erc1155Decoder;

impl Erc1155Decoder {
    pub fn new() -> Self {
        Self
    }

    fn felt_to_u256(felt: Felt) -> U256 {
        bytes_to_u256(&felt.to_bytes_be())
    }

    /// TransferSingle event selector: sn_keccak("TransferSingle")
    fn transfer_single_selector() -> Felt {
        selector!("TransferSingle")
    }

    /// TransferBatch event selector: sn_keccak("TransferBatch")
    fn transfer_batch_selector() -> Felt {
        selector!("TransferBatch")
    }

    /// ApprovalForAll event selector: sn_keccak("ApprovalForAll")
    fn approval_for_all_selector() -> Felt {
        selector!("ApprovalForAll")
    }

    /// URI event selector: sn_keccak("URI")
    fn uri_selector() -> Felt {
        selector!("URI")
    }

    fn decode_string_result(result: &[Felt]) -> Option<String> {
        if result.is_empty() {
            return None;
        }

        if result.len() == 1 {
            return parse_cairo_short_string(&result[0])
                .ok()
                .filter(|s| !s.is_empty());
        }

        if let Ok(byte_array) = ByteArray::decode(result) {
            if let Ok(s) = String::try_from(byte_array) {
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }

        let len: usize = result[0].try_into().unwrap_or(0usize);
        if len > 0 && len < 1000 && result.len() > len {
            let mut out = String::new();
            for felt in &result[1..=len] {
                if let Ok(chunk) = parse_cairo_short_string(felt) {
                    out.push_str(&chunk);
                }
            }
            if !out.is_empty() {
                return Some(out);
            }
        }

        None
    }

    /// Decode TransferSingle event into envelope
    ///
    /// TransferSingle event signatures:
    ///
    /// Modern ERC1155:
    /// - keys[0]: TransferSingle selector
    /// - keys[1]: operator address
    /// - keys[2]: from address
    /// - keys[3]: to address
    /// - data[0]: id_low (u128)
    /// - data[1]: id_high (u128)
    /// - data[2]: value_low (u128)
    /// - data[3]: value_high (u128)
    ///
    /// Legacy ERC1155:
    /// - keys[0]: TransferSingle selector
    /// - data[0]: operator address
    /// - data[1]: from address
    /// - data[2]: to address
    /// - data[3]: id_low (u128)
    /// - data[4]: id_high (u128)
    /// - data[5]: value_low (u128)
    /// - data[6]: value_high (u128)
    async fn decode_transfer_single(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let operator;
        let from;
        let to;
        let id: U256;
        let value: U256;

        if event.keys.len() == 4 && event.data.len() == 4 {
            // Modern format: operator, from, to in keys; id, value in data
            operator = event.keys[1];
            from = event.keys[2];
            to = event.keys[3];
            let id_low: u128 = event.data[0].try_into().unwrap_or(0);
            let id_high: u128 = event.data[1].try_into().unwrap_or(0);
            id = U256::from_words(id_low, id_high);
            let value_low: u128 = event.data[2].try_into().unwrap_or(0);
            let value_high: u128 = event.data[3].try_into().unwrap_or(0);
            value = U256::from_words(value_low, value_high);
        } else if event.keys.len() == 1 && event.data.len() == 7 {
            // Legacy format: all in data
            operator = event.data[0];
            from = event.data[1];
            to = event.data[2];
            let id_low: u128 = event.data[3].try_into().unwrap_or(0);
            let id_high: u128 = event.data[4].try_into().unwrap_or(0);
            id = U256::from_words(id_low, id_high);
            let value_low: u128 = event.data[5].try_into().unwrap_or(0);
            let value_high: u128 = event.data[6].try_into().unwrap_or(0);
            value = U256::from_words(value_low, value_high);
        } else if event.keys.len() == 4 && event.data.len() == 2 {
            // Alternative modern format with single felt id and value
            operator = event.keys[1];
            from = event.keys[2];
            to = event.keys[3];
            id = Self::felt_to_u256(event.data[0]);
            value = Self::felt_to_u256(event.data[1]);
        } else {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed ERC1155 TransferSingle event"
            );
            return Ok(None);
        }

        let transfer = TransferSingle {
            operator,
            from,
            to,
            id,
            value,
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
            "erc1155_transfer_single_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(transfer),
            metadata,
        )))
    }

    /// Decode TransferBatch event into multiple envelopes (one per id/value pair)
    ///
    /// TransferBatch event signatures:
    ///
    /// Modern ERC1155:
    /// - keys[0]: TransferBatch selector
    /// - keys[1]: operator address
    /// - keys[2]: from address
    /// - keys[3]: to address
    /// - data[0]: ids_len
    /// - data[1..1+ids_len*2]: ids (low, high pairs)
    /// - data[1+ids_len*2]: values_len
    /// - data[2+ids_len*2..]: values (low, high pairs)
    ///
    /// Legacy: all in data
    async fn decode_transfer_batch(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
        let operator;
        let from;
        let to;
        let mut data_offset = 0;

        if event.keys.len() == 4 {
            // Modern format: operator, from, to in keys
            operator = event.keys[1];
            from = event.keys[2];
            to = event.keys[3];
        } else if event.keys.len() == 1 && event.data.len() >= 3 {
            // Legacy format: operator, from, to at start of data
            operator = event.data[0];
            from = event.data[1];
            to = event.data[2];
            data_offset = 3;
        } else {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed ERC1155 TransferBatch event"
            );
            return Ok(vec![]);
        }

        // Parse ids array
        if event.data.len() <= data_offset {
            return Ok(vec![]);
        }

        let ids_len: usize = event.data[data_offset].try_into().unwrap_or(0);
        data_offset += 1;

        fn parse_u256_slice(items: &[Felt], as_pairs: bool) -> Vec<U256> {
            if as_pairs {
                let mut out = Vec::with_capacity(items.len() / 2);
                for chunk in items.chunks_exact(2) {
                    let low: u128 = chunk[0].try_into().unwrap_or(0);
                    let high: u128 = chunk[1].try_into().unwrap_or(0);
                    out.push(U256::from_words(low, high));
                }
                out
            } else {
                items
                    .iter()
                    .copied()
                    .map(Erc1155Decoder::felt_to_u256)
                    .collect()
            }
        }

        let mut ids: Vec<U256> = Vec::new();
        let mut values: Vec<U256> = Vec::new();
        let mut parsed = false;

        // Try both pair-based and single-felt array layouts for ids and values.
        // Standard Starknet ERC1155 uses U256 pairs, but some contracts emit felt arrays.
        for ids_as_pairs in [true, false] {
            let id_words = if ids_as_pairs { 2 } else { 1 };
            let ids_end = data_offset.saturating_add(ids_len.saturating_mul(id_words));
            if ids_end > event.data.len() || ids_end >= event.data.len() {
                continue;
            }

            let candidate_ids = parse_u256_slice(&event.data[data_offset..ids_end], ids_as_pairs);
            let values_len: usize = event.data[ids_end].try_into().unwrap_or(0);
            let values_start = ids_end + 1;

            for values_as_pairs in [true, false] {
                let value_words = if values_as_pairs { 2 } else { 1 };
                let values_end =
                    values_start.saturating_add(values_len.saturating_mul(value_words));
                if values_end > event.data.len() {
                    continue;
                }

                ids.clone_from(&candidate_ids);
                values = parse_u256_slice(&event.data[values_start..values_end], values_as_pairs);
                parsed = true;
                break;
            }

            if parsed {
                break;
            }
        }

        if !parsed {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                ids_len = ids_len,
                "Failed to parse ERC1155 TransferBatch ids/values arrays"
            );
            return Ok(vec![]);
        }

        // Create envelope for each id/value pair
        let mut envelopes = Vec::new();
        for (i, (id, value)) in ids.iter().zip(values.iter()).enumerate() {
            let transfer = TransferBatch {
                operator,
                from,
                to,
                id: *id,
                value: *value,
                batch_index: i as u32,
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
            metadata.insert("batch_index".to_string(), i.to_string());

            let envelope_id = format!(
                "erc1155_transfer_batch_{}_{}_{}",
                event.block_number.unwrap_or(0),
                format!("{:#x}", event.transaction_hash),
                i
            );

            envelopes.push(Envelope::new(envelope_id, Box::new(transfer), metadata));
        }

        Ok(envelopes)
    }

    /// Decode ApprovalForAll event into envelope
    async fn decode_approval_for_all(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let owner;
        let operator;
        let approved: bool;

        if event.keys.len() == 3 && event.data.len() == 1 {
            // Modern format: owner, operator in keys; approved in data
            owner = event.keys[1];
            operator = event.keys[2];
            approved = event.data[0] != Felt::ZERO;
        } else if event.keys.len() == 1 && event.data.len() == 3 {
            // Legacy format: all in data
            owner = event.data[0];
            operator = event.data[1];
            approved = event.data[2] != Felt::ZERO;
        } else {
            tracing::warn!(
                target: "torii_erc1155::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed ERC1155 ApprovalForAll event"
            );
            return Ok(None);
        }

        let approval = OperatorApproval {
            owner,
            operator,
            approved,
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
            "erc1155_approval_for_all_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(approval),
            metadata,
        )))
    }

    /// Decode URI event into envelope
    ///
    /// Common ERC1155 Starknet layout:
    /// - keys[0]: URI selector
    /// - keys[1]: token id (felt-encoded)
    /// - data: URI payload (short string or ByteArray)
    async fn decode_uri(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        if event.keys.len() < 2 || event.data.is_empty() {
            return Ok(None);
        }

        let token_id = Self::felt_to_u256(event.keys[1]);
        let Some(uri) = Self::decode_string_result(&event.data) else {
            return Ok(None);
        };

        let uri_update = UriUpdate {
            token: event.from_address,
            token_id,
            uri,
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
            "erc1155_uri_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(uri_update),
            metadata,
        )))
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

    async fn decode(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
        if event.keys.is_empty() {
            return Ok(Vec::new());
        }

        let selector = event.keys[0];

        if selector == Self::transfer_single_selector() {
            if let Some(envelope) = self.decode_transfer_single(event).await? {
                return Ok(vec![envelope]);
            }
        } else if selector == Self::transfer_batch_selector() {
            return self.decode_transfer_batch(event).await;
        } else if selector == Self::approval_for_all_selector() {
            if let Some(envelope) = self.decode_approval_for_all(event).await? {
                return Ok(vec![envelope]);
            }
        } else if selector == Self::uri_selector() {
            if let Some(envelope) = self.decode_uri(event).await? {
                return Ok(vec![envelope]);
            }
        } else {
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
        }

        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decode_transfer_single_modern() {
        let decoder = Erc1155Decoder::new();

        // Modern format: operator, from, to in keys; id, value in data
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![
                Erc1155Decoder::transfer_single_selector(),
                Felt::from(0x1u64), // operator
                Felt::from(0x2u64), // from
                Felt::from(0x3u64), // to
            ],
            data: vec![
                Felt::from(42u64),  // id_low
                Felt::ZERO,         // id_high
                Felt::from(100u64), // value_low
                Felt::ZERO,         // value_high
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
            .downcast_ref::<TransferSingle>()
            .unwrap();

        assert_eq!(transfer.operator, Felt::from(0x1u64));
        assert_eq!(transfer.from, Felt::from(0x2u64));
        assert_eq!(transfer.to, Felt::from(0x3u64));
        assert_eq!(transfer.id, U256::from(42u64));
        assert_eq!(transfer.value, U256::from(100u64));
    }

    #[tokio::test]
    async fn test_decode_approval_for_all() {
        let decoder = Erc1155Decoder::new();

        let event = EmittedEvent {
            from_address: Felt::from(0x456u64),
            keys: vec![
                Erc1155Decoder::approval_for_all_selector(),
                Felt::from(0xau64), // owner
                Felt::from(0xbu64), // operator
            ],
            data: vec![Felt::from(1u64)], // approved = true
            block_hash: None,
            block_number: Some(200),
            transaction_hash: Felt::from(0xef01u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<OperatorApproval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0xau64));
        assert_eq!(approval.operator, Felt::from(0xbu64));
        assert!(approval.approved);
    }

    #[tokio::test]
    async fn test_decode_transfer_single_single_felt_preserves_full_felt() {
        let decoder = Erc1155Decoder::new();

        let id_felt =
            Felt::from_hex("0x100000000000000000000000000000001").expect("invalid id felt");
        let value_felt =
            Felt::from_hex("0x200000000000000000000000000000003").expect("invalid value felt");

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

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<TransferSingle>()
            .unwrap();

        assert_eq!(transfer.id, Erc1155Decoder::felt_to_u256(id_felt));
        assert_eq!(transfer.value, Erc1155Decoder::felt_to_u256(value_felt));
    }

    #[tokio::test]
    async fn test_decode_transfer_batch_single_felt_arrays() {
        let decoder = Erc1155Decoder::new();

        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![
                Erc1155Decoder::transfer_batch_selector(),
                Felt::from(0x1u64), // operator
                Felt::from(0x2u64), // from
                Felt::from(0x3u64), // to
            ],
            data: vec![
                Felt::from(2u64),   // ids_len
                Felt::from(11u64),  // id[0]
                Felt::from(12u64),  // id[1]
                Felt::from(2u64),   // values_len
                Felt::from(101u64), // value[0]
                Felt::from(102u64), // value[1]
            ],
            block_hash: None,
            block_number: Some(102),
            transaction_hash: Felt::from(0xabcfu64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 2);

        let first = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<TransferBatch>()
            .unwrap();
        let second = envelopes[1]
            .body
            .as_any()
            .downcast_ref::<TransferBatch>()
            .unwrap();

        assert_eq!(first.id, U256::from(11u64));
        assert_eq!(first.value, U256::from(101u64));
        assert_eq!(second.id, U256::from(12u64));
        assert_eq!(second.value, U256::from(102u64));
    }

    #[tokio::test]
    async fn test_decode_uri_event() {
        let decoder = Erc1155Decoder::new();

        // "abc" as short string felt
        let uri_felt = Felt::from(0x616263u64);
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![
                Erc1155Decoder::uri_selector(),
                Felt::from(7u64), // token id
            ],
            data: vec![uri_felt],
            block_hash: None,
            block_number: Some(103),
            transaction_hash: Felt::from(0xabd0u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let uri = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<UriUpdate>()
            .unwrap();
        assert_eq!(uri.token, Felt::from(0x123u64));
        assert_eq!(uri.token_id, U256::from(7u64));
        assert_eq!(uri.uri, "abc".to_string());
    }
}
