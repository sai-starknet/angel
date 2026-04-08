//! Synthetic ERC721 extractor for deterministic workload generation.
//!
//! Generates canonical ERC721 Transfer, Approval, ApprovalForAll, and metadata events
//! with realistic block and transaction context for profiling the ingestion pipeline.

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet::core::types::U256;
use starknet::macros::selector;
use starknet_types_raw::Felt;
use torii::etl::extractor::{ExtractionBatch, SyntheticExtractor};
use torii::etl::StarknetEvent;

use crate::conversions::core_to_raw_felt;

const EXTRACTOR_NAME: &str = "synthetic_erc721";

fn raw_selector(name: &str) -> Felt {
    core_to_raw_felt(match name {
        "Transfer" => selector!("Transfer"),
        "Approval" => selector!("Approval"),
        "ApprovalForAll" => selector!("ApprovalForAll"),
        "MetadataUpdate" => selector!("MetadataUpdate"),
        "BatchMetadataUpdate" => selector!("BatchMetadataUpdate"),
        _ => unreachable!(),
    })
}

/// Configuration for the synthetic ERC721 workload.
#[derive(Debug, Clone)]
pub struct SyntheticErc721Config {
    /// Starting block number.
    pub from_block: u64,
    /// Number of blocks to generate.
    pub block_count: u64,
    /// Number of transactions per block.
    pub tx_per_block: usize,
    /// Number of blocks per extract() call.
    pub blocks_per_batch: u64,
    /// Ratio of approval events in basis points (0..=10_000).
    pub approval_ratio_bps: u16,
    /// Ratio of ApprovalForAll events in basis points (0..=10_000).
    pub approval_for_all_ratio_bps: u16,
    /// Ratio of MetadataUpdate events in basis points (0..=10_000).
    pub metadata_update_ratio_bps: u16,
    /// Ratio of BatchMetadataUpdate events in basis points (0..=10_000).
    pub batch_metadata_update_ratio_bps: u16,
    /// Number of synthetic token contracts used by the workload.
    pub token_count: usize,
    /// Number of synthetic wallets used by the workload.
    pub wallet_count: usize,
    /// Maximum token ID for NFTs.
    pub max_token_id: u64,
    /// Seed used to deterministically perturb generated values.
    pub seed: u64,
}

impl Default for SyntheticErc721Config {
    fn default() -> Self {
        Self {
            from_block: 1_000_000,
            block_count: 200,
            tx_per_block: 1_000,
            blocks_per_batch: 1,
            approval_ratio_bps: 500,
            approval_for_all_ratio_bps: 200,
            metadata_update_ratio_bps: 50,
            batch_metadata_update_ratio_bps: 20,
            token_count: 16,
            wallet_count: 20_000,
            max_token_id: 10_000,
            seed: 42,
        }
    }
}

impl SyntheticErc721Config {
    fn validate(&self) -> Result<()> {
        if self.block_count == 0 {
            anyhow::bail!("block_count must be > 0");
        }
        if self.tx_per_block == 0 {
            anyhow::bail!("tx_per_block must be > 0");
        }
        if self.blocks_per_batch == 0 {
            anyhow::bail!("blocks_per_batch must be > 0");
        }
        if self.approval_ratio_bps > 10_000 {
            anyhow::bail!("approval_ratio_bps must be <= 10_000");
        }
        if self.approval_for_all_ratio_bps > 10_000 {
            anyhow::bail!("approval_for_all_ratio_bps must be <= 10_000");
        }
        if self.metadata_update_ratio_bps > 10_000 {
            anyhow::bail!("metadata_update_ratio_bps must be <= 10_000");
        }
        if self.batch_metadata_update_ratio_bps > 10_000 {
            anyhow::bail!("batch_metadata_update_ratio_bps must be <= 10_000");
        }
        if self.token_count == 0 {
            anyhow::bail!("token_count must be > 0");
        }
        if self.wallet_count == 0 {
            anyhow::bail!("wallet_count must be > 0");
        }
        if self.max_token_id == 0 {
            anyhow::bail!("max_token_id must be > 0");
        }
        Ok(())
    }

    fn to_block_inclusive(&self) -> u64 {
        self.from_block + self.block_count - 1
    }
}

/// Deterministic synthetic extractor for ERC721 canonical events.
pub struct SyntheticErc721Extractor {
    config: SyntheticErc721Config,
    current_block: u64,
    finished: bool,
}

impl SyntheticErc721Extractor {
    fn to_block_inclusive(&self) -> u64 {
        self.config.to_block_inclusive()
    }

    fn parse_cursor(cursor: &str) -> Result<u64> {
        let block_str = cursor
            .strip_prefix("synthetic_erc721:block:")
            .context("invalid cursor format, expected synthetic_erc721:block:<n>")?;
        let block = block_str
            .parse::<u64>()
            .context("invalid cursor block number")?;
        Ok(block)
    }

    fn make_cursor(block: u64) -> String {
        format!("synthetic_erc721:block:{block}")
    }

    fn token_for(&self, block_number: u64, tx_index: usize) -> Felt {
        let idx = ((block_number as usize)
            .wrapping_add(tx_index)
            .wrapping_add(self.config.seed as usize))
            % self.config.token_count;
        Felt::from(0x0100_0000_u64 + idx as u64)
    }

    fn wallet_for(&self, value: usize) -> Felt {
        let idx = (value + self.config.seed as usize) % self.config.wallet_count;
        Felt::from(0x0200_0000_u64 + idx as u64)
    }

    fn tx_hash_for(&self, block_number: u64, tx_index: usize) -> Felt {
        Felt::from(
            block_number
                .saturating_mul(1_000_000)
                .saturating_add(tx_index as u64)
                .saturating_add(self.config.seed),
        )
    }

    fn token_id_for(&self, block_number: u64, tx_index: usize) -> U256 {
        let id = ((block_number as usize)
            .wrapping_add(tx_index)
            .wrapping_add(self.config.seed as usize))
            % self.config.max_token_id as usize;
        U256::from(id as u64)
    }

    fn event_type_for(&self, tx_index: usize) -> Erc721EventType {
        let scaled = (tx_index.saturating_mul(10_000)) / self.config.tx_per_block.max(1);

        if scaled < self.config.approval_ratio_bps as usize {
            Erc721EventType::Approval
        } else if scaled
            < self.config.approval_ratio_bps as usize
                + self.config.approval_for_all_ratio_bps as usize
        {
            Erc721EventType::ApprovalForAll
        } else if scaled
            < self.config.approval_ratio_bps as usize
                + self.config.approval_for_all_ratio_bps as usize
                + self.config.metadata_update_ratio_bps as usize
        {
            Erc721EventType::MetadataUpdate
        } else if scaled
            < self.config.approval_ratio_bps as usize
                + self.config.approval_for_all_ratio_bps as usize
                + self.config.metadata_update_ratio_bps as usize
                + self.config.batch_metadata_update_ratio_bps as usize
        {
            Erc721EventType::BatchMetadataUpdate
        } else {
            Erc721EventType::Transfer
        }
    }

    fn build_block_batch(&self, start_block: u64, end_block: u64) -> ExtractionBatch {
        let blocks_in_batch = (end_block - start_block + 1) as usize;
        let total_events = blocks_in_batch * self.config.tx_per_block;

        let mut batch =
            ExtractionBatch::with_capacities(total_events, blocks_in_batch, total_events, 0, 0);

        for block_number in start_block..=end_block {
            batch.add_block_context(
                block_number,
                Felt::from(0x0300_0000_u64 + block_number),
                if block_number > 0 {
                    Felt::from(0x0300_0000_u64 + block_number - 1)
                } else {
                    Felt::ZERO
                },
                1_700_000_000 + (block_number * 12),
            );

            for tx_index in 0..self.config.tx_per_block {
                let tx_hash = self.tx_hash_for(block_number, tx_index);
                let token = self.token_for(block_number, tx_index);
                let from = self.wallet_for(tx_index.wrapping_mul(13) + block_number as usize);
                let to = self.wallet_for(tx_index.wrapping_mul(17) + block_number as usize + 7);
                let token_id = self.token_id_for(block_number, tx_index);

                let event = match self.event_type_for(tx_index) {
                    Erc721EventType::Transfer => StarknetEvent::new(
                        token,
                        vec![
                            raw_selector("Transfer"),
                            from,
                            to,
                            Felt::from(token_id.low()),
                            Felt::from(token_id.high()),
                        ],
                        vec![],
                        block_number,
                        tx_hash,
                    ),
                    Erc721EventType::Approval => StarknetEvent::new(
                        token,
                        vec![
                            raw_selector("Approval"),
                            from,
                            to,
                            Felt::from(token_id.low()),
                            Felt::from(token_id.high()),
                        ],
                        vec![],
                        block_number,
                        tx_hash,
                    ),
                    Erc721EventType::ApprovalForAll => StarknetEvent::new(
                        token,
                        vec![raw_selector("ApprovalForAll"), from, to],
                        vec![Felt::from(1u64)],
                        block_number,
                        tx_hash,
                    ),
                    Erc721EventType::MetadataUpdate => StarknetEvent::new(
                        token,
                        vec![raw_selector("MetadataUpdate")],
                        vec![Felt::from(token_id.low()), Felt::from(token_id.high())],
                        block_number,
                        tx_hash,
                    ),
                    Erc721EventType::BatchMetadataUpdate => {
                        let to_id = token_id + U256::from(100u64);
                        StarknetEvent::new(
                            token,
                            vec![raw_selector("BatchMetadataUpdate")],
                            vec![
                                Felt::from(token_id.low()),
                                Felt::from(token_id.high()),
                                Felt::from(to_id.low()),
                                Felt::from(to_id.high()),
                            ],
                            block_number,
                            tx_hash,
                        )
                    }
                };

                batch.add_event_with_tx_context(event, Some(from), vec![token, from, to]);
            }
        }

        batch.set_cursor(Self::make_cursor(end_block));
        batch.set_chain_head(self.to_block_inclusive());
        batch
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Erc721EventType {
    Transfer,
    Approval,
    ApprovalForAll,
    MetadataUpdate,
    BatchMetadataUpdate,
}

#[async_trait]
impl SyntheticExtractor for SyntheticErc721Extractor {
    type Config = SyntheticErc721Config;

    fn new(config: Self::Config) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            current_block: config.from_block,
            config,
            finished: false,
        })
    }

    async fn extract(&mut self, cursor: Option<String>) -> Result<ExtractionBatch> {
        if let Some(cursor_str) = cursor {
            self.current_block = Self::parse_cursor(&cursor_str)?.saturating_add(1);
        }

        if self.current_block > self.to_block_inclusive() {
            self.finished = true;
        }

        if self.finished {
            return Ok(ExtractionBatch::empty());
        }

        let end_block =
            (self.current_block + self.config.blocks_per_batch - 1).min(self.to_block_inclusive());

        let batch = self.build_block_batch(self.current_block, end_block);
        self.current_block = end_block + 1;
        self.finished = self.current_block > self.to_block_inclusive();
        Ok(batch)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn extractor_name(&self) -> &'static str {
        EXTRACTOR_NAME
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use torii::etl::extractor::SyntheticExtractor;

    #[tokio::test]
    async fn synthetic_extractor_is_deterministic() {
        let cfg = SyntheticErc721Config {
            block_count: 2,
            tx_per_block: 4,
            blocks_per_batch: 1,
            ..Default::default()
        };

        let mut a = SyntheticErc721Extractor::new(cfg.clone()).unwrap();
        let mut b = SyntheticErc721Extractor::new(cfg).unwrap();

        let a1 = a.extract(None).await.unwrap();
        let b1 = b.extract(None).await.unwrap();

        assert_eq!(a1.events.len(), b1.events.len());
        assert_eq!(a1.events[0].transaction_hash, b1.events[0].transaction_hash);
        assert_eq!(a1.events[0].keys, b1.events[0].keys);
    }

    #[tokio::test]
    async fn synthetic_extractor_generates_all_event_types() {
        let cfg = SyntheticErc721Config {
            block_count: 1,
            tx_per_block: 100,
            blocks_per_batch: 1,
            approval_ratio_bps: 1_000,
            approval_for_all_ratio_bps: 1_000,
            metadata_update_ratio_bps: 1_000,
            batch_metadata_update_ratio_bps: 1_000,
            ..Default::default()
        };

        let mut extractor = SyntheticErc721Extractor::new(cfg).unwrap();
        let batch = extractor.extract(None).await.unwrap();

        let transfer_selector = raw_selector("Transfer");
        let approval_selector = raw_selector("Approval");
        let approval_for_all_selector = raw_selector("ApprovalForAll");
        let metadata_update_selector = raw_selector("MetadataUpdate");
        let batch_metadata_update_selector = raw_selector("BatchMetadataUpdate");

        let mut has_transfer = false;
        let mut has_approval = false;
        let mut has_approval_for_all = false;
        let mut has_metadata_update = false;
        let mut has_batch_metadata_update = false;

        for event in &batch.events {
            if event.keys[0] == transfer_selector {
                has_transfer = true;
            } else if event.keys[0] == approval_selector {
                has_approval = true;
            } else if event.keys[0] == approval_for_all_selector {
                has_approval_for_all = true;
            } else if event.keys[0] == metadata_update_selector {
                has_metadata_update = true;
            } else if event.keys[0] == batch_metadata_update_selector {
                has_batch_metadata_update = true;
            }
        }

        assert!(has_transfer, "Should have Transfer events");
        assert!(has_approval, "Should have Approval events");
        assert!(has_approval_for_all, "Should have ApprovalForAll events");
        assert!(has_metadata_update, "Should have MetadataUpdate events");
        assert!(
            has_batch_metadata_update,
            "Should have BatchMetadataUpdate events"
        );
    }

    #[tokio::test]
    async fn synthetic_extractor_returns_empty_after_final_cursor() {
        let cfg = SyntheticErc721Config {
            block_count: 1,
            tx_per_block: 2,
            blocks_per_batch: 1,
            from_block: 500,
            ..Default::default()
        };

        let mut extractor = SyntheticErc721Extractor::new(cfg).unwrap();

        let batch = extractor.extract(None).await.unwrap();
        let cursor = batch.cursor.clone().unwrap();

        let resumed = extractor.extract(Some(cursor)).await.unwrap();
        assert!(resumed.is_empty());
        assert!(extractor.is_finished());
    }
}
