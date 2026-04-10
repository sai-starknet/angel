//! ERC721 event decoder (Transfer, Approval, ApprovalForAll)

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt, U256};
use starknet::macros::selector;
use std::any::Any;
use std::collections::HashMap;
use torii::etl::{Decoder, Envelope, TypedBody};

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

impl TypedBody for NftTransfer {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.transfer")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
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

impl TypedBody for NftApproval {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.approval")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
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

impl TypedBody for OperatorApproval {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.approval_for_all")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// MetadataUpdate event (EIP-4906) — single token
#[derive(Debug, Clone)]
pub struct MetadataUpdate {
    pub token: Felt,
    pub token_id: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

impl TypedBody for MetadataUpdate {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.metadata_update")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
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

impl TypedBody for BatchMetadataUpdate {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("erc721.batch_metadata_update")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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

    /// Decode Transfer event into envelope
    ///
    /// Transfer event signatures (supports both modern and legacy):
    ///
    /// Modern ERC721:
    /// - keys[0]: Transfer selector
    /// - keys[1]: from address
    /// - keys[2]: to address
    /// - keys[3]: token_id_low (u128)
    /// - keys[4]: token_id_high (u128)
    ///
    /// Legacy ERC721 (all in data):
    /// - keys[0]: Transfer selector
    /// - data[0]: from address
    /// - data[1]: to address
    /// - data[2]: token_id_low (u128)
    /// - data[3]: token_id_high (u128)
    async fn decode_transfer(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let from;
        let to;
        let token_id: U256;

        if event.keys.len() == 5 && event.data.is_empty() {
            // Modern ERC721: from, to, token_id_low, token_id_high all in keys
            from = event.keys[1];
            to = event.keys[2];
            let low: u128 = event.keys[3].try_into().unwrap_or(0);
            let high: u128 = event.keys[4].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if event.keys.len() == 1 && event.data.len() == 4 {
            // Legacy ERC721: from, to, token_id_low, token_id_high in data
            from = event.data[0];
            to = event.data[1];
            let low: u128 = event.data[2].try_into().unwrap_or(0);
            let high: u128 = event.data[3].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if event.keys.len() == 4 && event.data.is_empty() {
            // Alternative modern format with single felt token_id
            from = event.keys[1];
            to = event.keys[2];
            let id_felt: u128 = event.keys[3].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else if event.keys.len() == 1 && event.data.len() == 3 {
            // Alternative legacy format with single felt token_id
            from = event.data[0];
            to = event.data[1];
            let id_felt: u128 = event.data[2].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed ERC721 Transfer event"
            );
            return Ok(None);
        }

        let transfer = NftTransfer {
            from,
            to,
            token_id,
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
            "erc721_transfer_{}_{}",
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
    /// Approval event signatures (supports both modern and legacy):
    ///
    /// Modern ERC721:
    /// - keys[0]: Approval selector
    /// - keys[1]: owner address
    /// - keys[2]: approved address
    /// - keys[3]: token_id_low (u128)
    /// - keys[4]: token_id_high (u128)
    ///
    /// Legacy ERC721:
    /// - keys[0]: Approval selector
    /// - data[0]: owner address
    /// - data[1]: approved address
    /// - data[2]: token_id_low (u128)
    /// - data[3]: token_id_high (u128)
    async fn decode_approval(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let owner;
        let approved;
        let token_id: U256;

        if event.keys.len() == 5 && event.data.is_empty() {
            // Modern ERC721: owner, approved, token_id_low, token_id_high all in keys
            owner = event.keys[1];
            approved = event.keys[2];
            let low: u128 = event.keys[3].try_into().unwrap_or(0);
            let high: u128 = event.keys[4].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if event.keys.len() == 1 && event.data.len() == 4 {
            // Legacy ERC721: owner, approved, token_id_low, token_id_high in data
            owner = event.data[0];
            approved = event.data[1];
            let low: u128 = event.data[2].try_into().unwrap_or(0);
            let high: u128 = event.data[3].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if event.keys.len() == 4 && event.data.is_empty() {
            // Alternative modern format with single felt token_id
            owner = event.keys[1];
            approved = event.keys[2];
            let id_felt: u128 = event.keys[3].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else if event.keys.len() == 1 && event.data.len() == 3 {
            // Alternative legacy format with single felt token_id
            owner = event.data[0];
            approved = event.data[1];
            let id_felt: u128 = event.data[2].try_into().unwrap_or(0);
            token_id = U256::from(id_felt);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed ERC721 Approval event"
            );
            return Ok(None);
        }

        let approval = NftApproval {
            owner,
            approved,
            token_id,
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
            "erc721_approval_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(approval),
            metadata,
        )))
    }

    /// Decode ApprovalForAll event into envelope
    ///
    /// ApprovalForAll event signatures (supports both modern and legacy):
    ///
    /// Modern ERC721:
    /// - keys[0]: ApprovalForAll selector
    /// - keys[1]: owner address
    /// - keys[2]: operator address
    /// - data[0]: approved (bool as felt)
    ///
    /// Legacy ERC721:
    /// - keys[0]: ApprovalForAll selector
    /// - data[0]: owner address
    /// - data[1]: operator address
    /// - data[2]: approved (bool as felt)
    async fn decode_approval_for_all(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let owner;
        let operator;
        let approved: bool;

        if event.keys.len() == 3 && event.data.len() == 1 {
            // Modern ERC721: owner, operator in keys; approved in data
            owner = event.keys[1];
            operator = event.keys[2];
            approved = event.data[0] != Felt::ZERO;
        } else if event.keys.len() == 1 && event.data.len() == 3 {
            // Legacy ERC721: owner, operator, approved all in data
            owner = event.data[0];
            operator = event.data[1];
            approved = event.data[2] != Felt::ZERO;
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", event.from_address),
                tx_hash = %format!("{:#x}", event.transaction_hash),
                block_number = event.block_number.unwrap_or(0),
                keys_len = event.keys.len(),
                data_len = event.data.len(),
                "Malformed ERC721 ApprovalForAll event"
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
            "erc721_approval_for_all_{}_{}",
            event.block_number.unwrap_or(0),
            format!("{:#x}", event.transaction_hash)
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(approval),
            metadata,
        )))
    }

    /// Decode MetadataUpdate event (EIP-4906)
    ///
    /// MetadataUpdate(uint256 tokenId):
    /// - keys[0]: selector
    /// - data[0]: token_id_low
    /// - data[1]: token_id_high
    /// OR:
    /// - keys[0]: selector
    /// - keys[1]: token_id_low
    /// - keys[2]: token_id_high
    async fn decode_metadata_update(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let token_id: U256;

        if event.data.len() >= 2 {
            let low: u128 = event.data[0].try_into().unwrap_or(0);
            let high: u128 = event.data[1].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if event.keys.len() >= 3 {
            let low: u128 = event.keys[1].try_into().unwrap_or(0);
            let high: u128 = event.keys[2].try_into().unwrap_or(0);
            token_id = U256::from_words(low, high);
        } else if event.data.len() == 1 {
            let id: u128 = event.data[0].try_into().unwrap_or(0);
            token_id = U256::from(id);
        } else if event.keys.len() == 2 {
            let id: u128 = event.keys[1].try_into().unwrap_or(0);
            token_id = U256::from(id);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", event.from_address),
                "Malformed MetadataUpdate event"
            );
            return Ok(None);
        }

        let update = MetadataUpdate {
            token: event.from_address,
            token_id,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        };

        let envelope_id = format!(
            "erc721_metadata_update_{}_{:#x}",
            event.block_number.unwrap_or(0),
            event.transaction_hash
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(update),
            HashMap::new(),
        )))
    }

    /// Decode BatchMetadataUpdate event (EIP-4906)
    ///
    /// BatchMetadataUpdate(uint256 fromTokenId, uint256 toTokenId)
    async fn decode_batch_metadata_update(&self, event: &EmittedEvent) -> Result<Option<Envelope>> {
        let from_token_id: U256;
        let to_token_id: U256;

        if event.data.len() >= 4 {
            let low: u128 = event.data[0].try_into().unwrap_or(0);
            let high: u128 = event.data[1].try_into().unwrap_or(0);
            from_token_id = U256::from_words(low, high);
            let low: u128 = event.data[2].try_into().unwrap_or(0);
            let high: u128 = event.data[3].try_into().unwrap_or(0);
            to_token_id = U256::from_words(low, high);
        } else if event.keys.len() >= 5 {
            let low: u128 = event.keys[1].try_into().unwrap_or(0);
            let high: u128 = event.keys[2].try_into().unwrap_or(0);
            from_token_id = U256::from_words(low, high);
            let low: u128 = event.keys[3].try_into().unwrap_or(0);
            let high: u128 = event.keys[4].try_into().unwrap_or(0);
            to_token_id = U256::from_words(low, high);
        } else {
            tracing::warn!(
                target: "torii_erc721::decoder",
                token = %format!("{:#x}", event.from_address),
                "Malformed BatchMetadataUpdate event"
            );
            return Ok(None);
        }

        let update = BatchMetadataUpdate {
            token: event.from_address,
            from_token_id,
            to_token_id,
            block_number: event.block_number.unwrap_or(0),
            transaction_hash: event.transaction_hash,
        };

        let envelope_id = format!(
            "erc721_batch_metadata_update_{}_{:#x}",
            event.block_number.unwrap_or(0),
            event.transaction_hash
        );

        Ok(Some(Envelope::new(
            envelope_id,
            Box::new(update),
            HashMap::new(),
        )))
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

    async fn decode(&self, event: &EmittedEvent) -> Result<Vec<Envelope>> {
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
        } else if selector == Self::approval_for_all_selector() {
            if let Some(envelope) = self.decode_approval_for_all(event).await? {
                return Ok(vec![envelope]);
            }
        } else if selector == Self::metadata_update_selector() {
            if let Some(envelope) = self.decode_metadata_update(event).await? {
                return Ok(vec![envelope]);
            }
        } else if selector == Self::batch_metadata_update_selector() {
            if let Some(envelope) = self.decode_batch_metadata_update(event).await? {
                return Ok(vec![envelope]);
            }
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
        }

        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_decode_modern_transfer() {
        let decoder = Erc721Decoder::new();

        // Modern format: all in keys
        let event = EmittedEvent {
            from_address: Felt::from(0x123u64),
            keys: vec![
                Erc721Decoder::transfer_selector(),
                Felt::from(0x1u64), // from
                Felt::from(0x2u64), // to
                Felt::from(42u64),  // token_id_low
                Felt::ZERO,         // token_id_high
            ],
            data: vec![],
            block_hash: None,
            block_number: Some(100),
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<NftTransfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0x1u64));
        assert_eq!(transfer.to, Felt::from(0x2u64));
        assert_eq!(transfer.token_id, U256::from(42u64));
        assert_eq!(transfer.token, Felt::from(0x123u64));
    }

    #[tokio::test]
    async fn test_decode_legacy_transfer() {
        let decoder = Erc721Decoder::new();

        // Legacy format: all in data
        let event = EmittedEvent {
            from_address: Felt::from(0x456u64),
            keys: vec![Erc721Decoder::transfer_selector()],
            data: vec![
                Felt::from(0xau64), // from
                Felt::from(0xbu64), // to
                Felt::from(100u64), // token_id_low
                Felt::ZERO,         // token_id_high
            ],
            block_hash: None,
            block_number: Some(200),
            transaction_hash: Felt::from(0xef01u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let transfer = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<NftTransfer>()
            .unwrap();

        assert_eq!(transfer.from, Felt::from(0xau64));
        assert_eq!(transfer.to, Felt::from(0xbu64));
        assert_eq!(transfer.token_id, U256::from(100u64));
    }

    #[tokio::test]
    async fn test_decode_approval_for_all() {
        let decoder = Erc721Decoder::new();

        // Modern format
        let event = EmittedEvent {
            from_address: Felt::from(0x789u64),
            keys: vec![
                Erc721Decoder::approval_for_all_selector(),
                Felt::from(0xcu64), // owner
                Felt::from(0xdu64), // operator
            ],
            data: vec![Felt::from(1u64)], // approved = true
            block_hash: None,
            block_number: Some(300),
            transaction_hash: Felt::from(0x2345u64),
        };

        let envelopes = decoder.decode(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);

        let approval = envelopes[0]
            .body
            .as_any()
            .downcast_ref::<OperatorApproval>()
            .unwrap();

        assert_eq!(approval.owner, Felt::from(0xcu64));
        assert_eq!(approval.operator, Felt::from(0xdu64));
        assert!(approval.approved);
    }
}
