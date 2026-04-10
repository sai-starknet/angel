//! Synthetic ERC20 extractor for deterministic end-to-end workload generation.
//!
//! Generates canonical ERC20 Transfer/Approval events with realistic block and
//! transaction context so the full ingestion path (extract -> decode -> sink -> DB)
//! can be profiled locally without external RPC dependencies.

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet_types_raw::Felt;
use std::collections::HashMap;
use std::sync::Arc;

use crate::etl::engine_db::EngineDb;
use crate::etl::StarknetEvent;

use super::{BlockContext, ExtractionBatch, Extractor, TransactionContext};

const EXTRACTOR_TYPE: &str = "synthetic_erc20";
const STATE_KEY: &str = "last_block";

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
    initialized: bool,
}

impl SyntheticErc20Extractor {
    /// Create a new synthetic extractor.
    pub fn new(config: SyntheticErc20Config) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            current_block: config.from_block,
            config,
            finished: false,
            initialized: false,
        })
    }

    fn to_block_inclusive(&self) -> u64 {
        self.config.to_block_inclusive()
    }

    async fn initialize(&mut self, cursor: Option<String>, engine_db: &EngineDb) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        if let Some(cursor_str) = cursor {
            self.current_block = Self::parse_cursor(&cursor_str)?.saturating_add(1);
        } else if let Some(saved_state) = engine_db
            .get_extractor_state(EXTRACTOR_TYPE, STATE_KEY)
            .await?
        {
            let block = saved_state
                .parse::<u64>()
                .context("invalid synthetic extractor saved state")?;
            self.current_block = block.saturating_add(1);
        }

        if self.current_block > self.to_block_inclusive() {
            self.finished = true;
        }

        self.initialized = true;
        Ok(())
    }

    fn parse_cursor(cursor: &str) -> Result<u64> {
        let block_str = cursor
            .strip_prefix("synthetic:block:")
            .context("invalid cursor format, expected synthetic:block:<n>")?;
        let block = block_str
            .parse::<u64>()
            .context("invalid cursor block number")?;
        Ok(block)
    }

    fn make_cursor(block: u64) -> String {
        format!("synthetic:block:{block}")
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
        // Fits inside u64 ranges with current defaults and stays deterministic.
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

        let mut events = Vec::with_capacity(total_events);
        let mut blocks = HashMap::with_capacity(blocks_in_batch);
        let mut transactions = HashMap::with_capacity(total_events);

        for block_number in start_block..=end_block {
            blocks.insert(
                block_number,
                Arc::new(BlockContext {
                    number: block_number,
                    hash: Felt::from(0x0300_0000_u64 + block_number),
                    parent_hash: if block_number > 0 {
                        Felt::from(0x0300_0000_u64 + block_number - 1)
                    } else {
                        Felt::ZERO
                    },
                    // 12-second blocks anchored to a fixed epoch.
                    timestamp: 1_700_000_000 + (block_number * 12),
                }),
            );

            for tx_index in 0..self.config.tx_per_block {
                let tx_hash = self.tx_hash_for(block_number, tx_index);
                let token = self.token_for(block_number, tx_index);
                let from = self.wallet_for(tx_index.wrapping_mul(13) + block_number as usize);
                let to = self.wallet_for(tx_index.wrapping_mul(17) + block_number as usize + 7);
                let amount_low = self.amount_low_for(block_number, tx_index);

                let event = if self.is_approval(tx_index) {
                    StarknetEvent {
                        from_address: token,
                        keys: vec![Felt::selector("Approval"), from, to],
                        data: vec![amount_low, Felt::ZERO],
                        block_number: block_number,
                        transaction_hash: tx_hash,
                    }
                } else {
                    StarknetEvent {
                        from_address: token,
                        keys: vec![Felt::selector("Transfer"), from, to],
                        data: vec![amount_low, Felt::ZERO],
                        block_number: block_number,
                        transaction_hash: tx_hash,
                    }
                };

                transactions.insert(
                    tx_hash,
                    Arc::new(TransactionContext {
                        hash: tx_hash,
                        block_number,
                        sender_address: Some(from),
                        calldata: vec![token, from, to, amount_low],
                    }),
                );

                events.push(event);
            }
        }

        ExtractionBatch {
            events,
            blocks,
            transactions,
            declared_classes: Vec::new(),
            deployed_contracts: Vec::new(),
            cursor: Some(Self::make_cursor(end_block)),
            chain_head: Some(self.to_block_inclusive()),
        }
    }
}

#[async_trait]
impl Extractor for SyntheticErc20Extractor {
    fn set_start_block(&mut self, start_block: u64) {
        self.current_block = start_block.max(self.current_block);
    }
    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        self.initialize(cursor, engine_db).await?;
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

    async fn commit_cursor(&mut self, cursor: &str, engine_db: &EngineDb) -> Result<()> {
        let block_num = Self::parse_cursor(cursor)?;
        engine_db
            .set_extractor_state(EXTRACTOR_TYPE, STATE_KEY, &block_num.to_string())
            .await
            .context("failed to commit synthetic extractor cursor")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::extractor::Extractor;

    #[tokio::test]
    async fn synthetic_extractor_is_deterministic() {
        let cfg = SyntheticErc20Config {
            block_count: 2,
            tx_per_block: 4,
            blocks_per_batch: 1,
            ..Default::default()
        };

        let engine_db = EngineDb::new(crate::etl::engine_db::EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();

        let mut a = SyntheticErc20Extractor::new(cfg.clone()).unwrap();
        let mut b = SyntheticErc20Extractor::new(cfg).unwrap();

        let a1 = a.extract(None, &engine_db).await.unwrap();
        let b1 = b.extract(None, &engine_db).await.unwrap();

        assert_eq!(a1.events.len(), b1.events.len());
        assert_eq!(a1.events[0].transaction_hash, b1.events[0].transaction_hash);
        assert_eq!(a1.events[0].keys, b1.events[0].keys);
        assert_eq!(a1.events[0].data, b1.events[0].data);
    }
}
