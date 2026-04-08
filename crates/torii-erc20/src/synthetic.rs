//! Synthetic ERC20 extractor for deterministic workload generation.
//!
//! Generates canonical ERC20 Transfer/Approval events with realistic block and
//! transaction context for profiling the ingestion pipeline without external dependencies.

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet_types_raw::Felt;
use torii::etl::extractor::{ExtractionBatch, SyntheticExtractor};
use torii::etl::StarknetEvent;

use crate::decoder::{APPROVAL_SELECTOR, TRANSFER_SELECTOR};

const EXTRACTOR_NAME: &str = "synthetic_erc20";

/// Configuration for the synthetic ERC20 workload.
#[derive(Debug, Clone)]
pub struct SyntheticErc20Config {
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
    /// Number of synthetic token contracts used by the workload.
    pub token_count: usize,
    /// Number of synthetic wallets used by the workload.
    pub wallet_count: usize,
    /// Seed used to deterministically perturb generated values.
    pub seed: u64,
}

impl Default for SyntheticErc20Config {
    fn default() -> Self {
        Self {
            from_block: 1_000_000,
            block_count: 200,
            tx_per_block: 1_000,
            blocks_per_batch: 1,
            approval_ratio_bps: 2_000,
            token_count: 16,
            wallet_count: 20_000,
            seed: 42,
        }
    }
}

impl SyntheticErc20Config {
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
        if self.token_count == 0 {
            anyhow::bail!("token_count must be > 0");
        }
        if self.wallet_count == 0 {
            anyhow::bail!("wallet_count must be > 0");
        }
        Ok(())
    }

    fn to_block_inclusive(&self) -> u64 {
        self.from_block + self.block_count - 1
    }
}

/// Deterministic synthetic extractor for ERC20 canonical events.
pub struct SyntheticErc20Extractor {
    config: SyntheticErc20Config,
    current_block: u64,
    finished: bool,
}

impl SyntheticErc20Extractor {
    fn to_block_inclusive(&self) -> u64 {
        self.config.to_block_inclusive()
    }

    fn parse_cursor(cursor: &str) -> Result<u64> {
        let block_str = cursor
            .strip_prefix("synthetic_erc20:block:")
            .context("invalid cursor format, expected synthetic_erc20:block:<n>")?;
        let block = block_str
            .parse::<u64>()
            .context("invalid cursor block number")?;
        Ok(block)
    }

    fn make_cursor(block: u64) -> String {
        format!("synthetic_erc20:block:{block}")
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

    fn amount_low_for(&self, block_number: u64, tx_index: usize) -> Felt {
        let value = 1_000_u64 + ((block_number + tx_index as u64 + self.config.seed) % 10_000_u64);
        Felt::from(value)
    }

    fn is_approval(&self, tx_index: usize) -> bool {
        let scaled = (tx_index.saturating_mul(10_000)) / self.config.tx_per_block.max(1);
        scaled < self.config.approval_ratio_bps as usize
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
                let amount_low = self.amount_low_for(block_number, tx_index);

                let event = if self.is_approval(tx_index) {
                    StarknetEvent::new(
                        token,
                        vec![APPROVAL_SELECTOR, from, to],
                        vec![amount_low, Felt::ZERO],
                        block_number,
                        tx_hash,
                    )
                } else {
                    StarknetEvent::new(
                        token,
                        vec![TRANSFER_SELECTOR, from, to],
                        vec![amount_low, Felt::ZERO],
                        block_number,
                        tx_hash,
                    )
                };
                batch.add_event_with_tx_context(
                    event,
                    Some(from),
                    vec![token, from, to, amount_low],
                );
            }
        }
        batch.set_cursor(Self::make_cursor(end_block));
        batch.set_chain_head(self.to_block_inclusive());
        batch
    }
}

#[async_trait]
impl SyntheticExtractor for SyntheticErc20Extractor {
    type Config = SyntheticErc20Config;

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
        let cfg = SyntheticErc20Config {
            block_count: 2,
            tx_per_block: 4,
            blocks_per_batch: 1,
            ..Default::default()
        };

        let mut a = SyntheticErc20Extractor::new(cfg.clone()).unwrap();
        let mut b = SyntheticErc20Extractor::new(cfg).unwrap();

        let a1 = a.extract(None).await.unwrap();
        let b1 = b.extract(None).await.unwrap();

        assert_eq!(a1.events.len(), b1.events.len());
        assert_eq!(a1.events[0].transaction_hash, b1.events[0].transaction_hash);
        assert_eq!(a1.events[0].keys, b1.events[0].keys);
        assert_eq!(a1.events[0].data, b1.events[0].data);
    }

    #[tokio::test]
    async fn synthetic_extractor_respects_cursor() {
        let cfg = SyntheticErc20Config {
            block_count: 10,
            tx_per_block: 5,
            blocks_per_batch: 1,
            from_block: 100,
            ..Default::default()
        };

        let mut extractor = SyntheticErc20Extractor::new(cfg).unwrap();

        let batch1 = extractor.extract(None).await.unwrap();
        assert_eq!(batch1.events.len(), 5);

        let cursor = batch1.cursor.unwrap();
        assert!(cursor.starts_with("synthetic_erc20:block:"));

        let batch2 = extractor.extract(Some(cursor)).await.unwrap();
        assert_eq!(batch2.events.len(), 5);
    }

    #[tokio::test]
    async fn synthetic_extractor_finishes_correctly() {
        let cfg = SyntheticErc20Config {
            block_count: 3,
            tx_per_block: 2,
            blocks_per_batch: 1,
            ..Default::default()
        };

        let mut extractor = SyntheticErc20Extractor::new(cfg).unwrap();

        assert!(!extractor.is_finished());

        extractor.extract(None).await.unwrap();
        assert!(!extractor.is_finished());

        extractor.extract(None).await.unwrap();
        assert!(!extractor.is_finished());

        extractor.extract(None).await.unwrap();
        assert!(extractor.is_finished());

        let final_batch = extractor.extract(None).await.unwrap();
        assert!(final_batch.is_empty());
    }

    #[tokio::test]
    async fn synthetic_extractor_returns_empty_after_final_cursor() {
        let cfg = SyntheticErc20Config {
            block_count: 1,
            tx_per_block: 2,
            blocks_per_batch: 1,
            from_block: 500,
            ..Default::default()
        };

        let mut extractor = SyntheticErc20Extractor::new(cfg).unwrap();

        let batch = extractor.extract(None).await.unwrap();
        let cursor = batch.cursor.clone().unwrap();

        let resumed = extractor.extract(Some(cursor)).await.unwrap();
        assert!(resumed.is_empty());
        assert!(extractor.is_finished());
    }
}
