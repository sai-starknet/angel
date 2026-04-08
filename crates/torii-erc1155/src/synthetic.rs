//! Synthetic ERC1155 extractor for deterministic workload generation.
//!
//! Generates canonical ERC1155 TransferSingle, TransferBatch, ApprovalForAll, and URI events
//! with realistic block and transaction context for profiling the ingestion pipeline.

use anyhow::{Context, Result};
use async_trait::async_trait;
use primitive_types::U256;
use starknet_types_raw::event::EmittedEvent;
use starknet_types_raw::Felt;
use torii::etl::extractor::{ExtractionBatch, SyntheticExtractor};
use torii_types::event::StarknetEvent;

const EXTRACTOR_NAME: &str = "synthetic_erc1155";

/// Configuration for the synthetic ERC1155 workload.
#[derive(Debug, Clone)]
pub struct SyntheticErc1155Config {
    /// Starting block number.
    pub from_block: u64,
    /// Number of blocks to generate.
    pub block_count: u64,
    /// Number of transactions per block.
    pub tx_per_block: usize,
    /// Number of blocks per extract() call.
    pub blocks_per_batch: u64,
    /// Ratio of TransferSingle events in basis points (0..=10_000).
    pub transfer_single_ratio_bps: u16,
    /// Ratio of TransferBatch events in basis points (0..=10_000).
    pub transfer_batch_ratio_bps: u16,
    /// Minimum batch size for TransferBatch events.
    pub min_batch_size: usize,
    /// Maximum batch size for TransferBatch events.
    pub max_batch_size: usize,
    /// Ratio of ApprovalForAll events in basis points (0..=10_000).
    pub approval_for_all_ratio_bps: u16,
    /// Ratio of URI events in basis points (0..=10_000).
    pub uri_ratio_bps: u16,
    /// Number of synthetic token contracts used by the workload.
    pub token_count: usize,
    /// Number of unique token IDs per contract.
    pub token_id_count: usize,
    /// Number of synthetic wallets used by the workload.
    pub wallet_count: usize,
    /// Seed used to deterministically perturb generated values.
    pub seed: u64,
}

impl Default for SyntheticErc1155Config {
    fn default() -> Self {
        Self {
            from_block: 1_000_000,
            block_count: 200,
            tx_per_block: 1_000,
            blocks_per_batch: 1,
            transfer_single_ratio_bps: 6_000,
            transfer_batch_ratio_bps: 2_000,
            min_batch_size: 1,
            max_batch_size: 5,
            approval_for_all_ratio_bps: 500,
            uri_ratio_bps: 100,
            token_count: 16,
            token_id_count: 100,
            wallet_count: 20_000,
            seed: 42,
        }
    }
}

impl SyntheticErc1155Config {
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
        if self.transfer_single_ratio_bps > 10_000 {
            anyhow::bail!("transfer_single_ratio_bps must be <= 10_000");
        }
        if self.transfer_batch_ratio_bps > 10_000 {
            anyhow::bail!("transfer_batch_ratio_bps must be <= 10_000");
        }
        if self.approval_for_all_ratio_bps > 10_000 {
            anyhow::bail!("approval_for_all_ratio_bps must be <= 10_000");
        }
        if self.uri_ratio_bps > 10_000 {
            anyhow::bail!("uri_ratio_bps must be <= 10_000");
        }
        if self.token_count == 0 {
            anyhow::bail!("token_count must be > 0");
        }
        if self.token_id_count == 0 {
            anyhow::bail!("token_id_count must be > 0");
        }
        if self.wallet_count == 0 {
            anyhow::bail!("wallet_count must be > 0");
        }
        if self.min_batch_size == 0 {
            anyhow::bail!("min_batch_size must be > 0");
        }
        if self.max_batch_size < self.min_batch_size {
            anyhow::bail!("max_batch_size must be >= min_batch_size");
        }
        Ok(())
    }

    fn to_block_inclusive(&self) -> u64 {
        self.from_block + self.block_count - 1
    }
}

/// Deterministic synthetic extractor for ERC1155 canonical events.
pub struct SyntheticErc1155Extractor {
    config: SyntheticErc1155Config,
    current_block: u64,
    finished: bool,
}

impl SyntheticErc1155Extractor {
    fn to_block_inclusive(&self) -> u64 {
        self.config.to_block_inclusive()
    }

    fn parse_cursor(cursor: &str) -> Result<u64> {
        let block_str = cursor
            .strip_prefix("synthetic_erc1155:block:")
            .context("invalid cursor format, expected synthetic_erc1155:block:<n>")?;
        let block = block_str
            .parse::<u64>()
            .context("invalid cursor block number")?;
        Ok(block)
    }

    fn make_cursor(block: u64) -> String {
        format!("synthetic_erc1155:block:{block}")
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

    fn token_id_for(&self, block_number: u64, tx_index: usize, id_offset: usize) -> U256 {
        let id = ((block_number as usize)
            .wrapping_add(tx_index)
            .wrapping_add(id_offset)
            .wrapping_add(self.config.seed as usize))
            % self.config.token_id_count;
        U256::from(id as u64)
    }

    fn value_for(&self, block_number: u64, tx_index: usize, value_offset: usize) -> U256 {
        let value = 1_000_u64
            + ((block_number + tx_index as u64 + value_offset as u64 + self.config.seed)
                % 10_000_u64);
        U256::from(value)
    }

    fn batch_size_for(&self, tx_index: usize) -> usize {
        let range = self.config.max_batch_size - self.config.min_batch_size + 1;
        let offset = (tx_index.wrapping_add(self.config.seed as usize)) % range;
        self.config.min_batch_size + offset
    }

    fn event_type_for(&self, tx_index: usize) -> Erc1155EventType {
        let scaled = (tx_index.saturating_mul(10_000)) / self.config.tx_per_block.max(1);

        if scaled < self.config.transfer_single_ratio_bps as usize {
            Erc1155EventType::TransferSingle
        } else if scaled
            < self.config.transfer_single_ratio_bps as usize
                + self.config.transfer_batch_ratio_bps as usize
        {
            Erc1155EventType::TransferBatch
        } else if scaled
            < self.config.transfer_single_ratio_bps as usize
                + self.config.transfer_batch_ratio_bps as usize
                + self.config.approval_for_all_ratio_bps as usize
        {
            Erc1155EventType::ApprovalForAll
        } else if scaled
            < self.config.transfer_single_ratio_bps as usize
                + self.config.transfer_batch_ratio_bps as usize
                + self.config.approval_for_all_ratio_bps as usize
                + self.config.uri_ratio_bps as usize
        {
            Erc1155EventType::Uri
        } else {
            Erc1155EventType::TransferSingle
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
                let operator = self.wallet_for(tx_index.wrapping_mul(7) + block_number as usize);
                let from = self.wallet_for(tx_index.wrapping_mul(13) + block_number as usize);
                let to = self.wallet_for(tx_index.wrapping_mul(17) + block_number as usize + 7);

                let event = match self.event_type_for(tx_index) {
                    Erc1155EventType::TransferSingle => {
                        let id = self.token_id_for(block_number, tx_index, 0);
                        let value = self.value_for(block_number, tx_index, 0);
                        let (id_low, id_high) = u256_low_high(id);
                        let (value_low, value_high) = u256_low_high(value);

                        EmittedEvent {
                            from_address: token,
                            keys: vec![Felt::selector("TransferSingle"), operator, from, to],
                            data: vec![id_low, id_high, value_low, value_high],
                            block_hash: Some(Felt::from(0x0300_0000_u64 + block_number)),
                            block_number: Some(block_number),
                            transaction_hash: tx_hash,
                        }
                    }
                    Erc1155EventType::TransferBatch => {
                        let batch_size = self.batch_size_for(tx_index);
                        let mut ids_data = vec![Felt::from(batch_size as u64)];
                        let mut values_data = Vec::new();

                        for i in 0..batch_size {
                            let id = self.token_id_for(block_number, tx_index, i);
                            let (low, high) = u256_low_high(id);
                            ids_data.push(low);
                            ids_data.push(high);
                        }

                        values_data.push(Felt::from(batch_size as u64));
                        for i in 0..batch_size {
                            let value = self.value_for(block_number, tx_index, i);
                            let (low, high) = u256_low_high(value);
                            values_data.push(low);
                            values_data.push(high);
                        }

                        ids_data.extend(values_data);

                        EmittedEvent {
                            from_address: token,
                            keys: vec![Felt::selector("TransferBatch"), operator, from, to],
                            data: ids_data,
                            block_hash: Some(Felt::from(0x0300_0000_u64 + block_number)),
                            block_number: Some(block_number),
                            transaction_hash: tx_hash,
                        }
                    }
                    Erc1155EventType::ApprovalForAll => EmittedEvent {
                        from_address: token,
                        keys: vec![Felt::selector("ApprovalForAll"), from, to],
                        data: vec![Felt::from(1u64)],
                        block_hash: Some(Felt::from(0x0300_0000_u64 + block_number)),
                        block_number: Some(block_number),
                        transaction_hash: tx_hash,
                    },
                    Erc1155EventType::Uri => {
                        let id = self.token_id_for(block_number, tx_index, 0);
                        let uri_felt = Felt::from(0x69706673u64);
                        let (id_low, _) = u256_low_high(id);
                        EmittedEvent {
                            from_address: token,
                            keys: vec![Felt::selector("URI"), id_low],
                            data: vec![uri_felt],
                            block_hash: Some(Felt::from(0x0300_0000_u64 + block_number)),
                            block_number: Some(block_number),
                            transaction_hash: tx_hash,
                        }
                    }
                };
                batch.add_event_with_tx_context(
                    StarknetEvent::new(
                        event.from_address,
                        event.keys,
                        event.data,
                        event.block_number.unwrap_or(0),
                        event.transaction_hash,
                    ),
                    Some(operator),
                    vec![token, from, to],
                );
            }
        }

        batch.set_cursor(Self::make_cursor(end_block));
        batch.set_chain_head(self.to_block_inclusive());
        batch
    }
}

fn u256_low_high(value: U256) -> (Felt, Felt) {
    let [l0, l1, h0, h1] = value.0;
    (
        Felt::from_le_words([l0, l1, 0, 0]),
        Felt::from_le_words([h0, h1, 0, 0]),
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Erc1155EventType {
    TransferSingle,
    TransferBatch,
    ApprovalForAll,
    Uri,
}

#[async_trait]
impl SyntheticExtractor for SyntheticErc1155Extractor {
    type Config = SyntheticErc1155Config;

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
        let cfg = SyntheticErc1155Config {
            block_count: 2,
            tx_per_block: 4,
            blocks_per_batch: 1,
            ..Default::default()
        };

        let mut a = SyntheticErc1155Extractor::new(cfg.clone()).unwrap();
        let mut b = SyntheticErc1155Extractor::new(cfg).unwrap();

        let a1 = a.extract(None).await.unwrap();
        let b1 = b.extract(None).await.unwrap();

        assert_eq!(a1.events.len(), b1.events.len());
        assert_eq!(a1.events[0].transaction_hash, b1.events[0].transaction_hash);
        assert_eq!(a1.events[0].keys, b1.events[0].keys);
    }

    #[tokio::test]
    async fn synthetic_extractor_generates_all_event_types() {
        let cfg = SyntheticErc1155Config {
            block_count: 1,
            tx_per_block: 100,
            blocks_per_batch: 1,
            transfer_single_ratio_bps: 3_000,
            transfer_batch_ratio_bps: 3_000,
            approval_for_all_ratio_bps: 2_000,
            uri_ratio_bps: 2_000,
            ..Default::default()
        };

        let mut extractor = SyntheticErc1155Extractor::new(cfg).unwrap();
        let batch = extractor.extract(None).await.unwrap();

        let transfer_single_selector = Felt::selector("TransferSingle");
        let transfer_batch_selector = Felt::selector("TransferBatch");
        let approval_for_all_selector = Felt::selector("ApprovalForAll");
        let uri_selector = Felt::selector("URI");

        let mut has_transfer_single = false;
        let mut has_transfer_batch = false;
        let mut has_approval_for_all = false;
        let mut has_uri = false;

        for event in &batch.events {
            if event.keys[0] == transfer_single_selector {
                has_transfer_single = true;
            } else if event.keys[0] == transfer_batch_selector {
                has_transfer_batch = true;
            } else if event.keys[0] == approval_for_all_selector {
                has_approval_for_all = true;
            } else if event.keys[0] == uri_selector {
                has_uri = true;
            }
        }

        assert!(has_transfer_single, "Should have TransferSingle events");
        assert!(has_transfer_batch, "Should have TransferBatch events");
        assert!(has_approval_for_all, "Should have ApprovalForAll events");
        assert!(has_uri, "Should have URI events");
    }

    #[tokio::test]
    async fn synthetic_extractor_batch_size_varies() {
        let cfg = SyntheticErc1155Config {
            block_count: 1,
            tx_per_block: 50,
            blocks_per_batch: 1,
            transfer_single_ratio_bps: 0,
            transfer_batch_ratio_bps: 10_000,
            min_batch_size: 2,
            max_batch_size: 5,
            ..Default::default()
        };

        let mut extractor = SyntheticErc1155Extractor::new(cfg).unwrap();
        let batch = extractor.extract(None).await.unwrap();

        let transfer_batch_selector = Felt::selector("TransferBatch");
        let mut batch_sizes = Vec::new();

        for event in &batch.events {
            if event.keys[0] == transfer_batch_selector {
                let batch_size = usize::try_from(event.data[0].to_le_words()[0]).unwrap_or(0);
                batch_sizes.push(batch_size);
            }
        }

        assert!(!batch_sizes.is_empty(), "Should have TransferBatch events");

        let min_found = *batch_sizes.iter().min().unwrap();
        let max_found = *batch_sizes.iter().max().unwrap();

        assert!(min_found >= 2, "Min batch size should be >= 2");
        assert!(max_found <= 5, "Max batch size should be <= 5");
    }

    #[tokio::test]
    async fn synthetic_extractor_returns_empty_after_final_cursor() {
        let cfg = SyntheticErc1155Config {
            block_count: 1,
            tx_per_block: 2,
            blocks_per_batch: 1,
            from_block: 500,
            ..Default::default()
        };

        let mut extractor = SyntheticErc1155Extractor::new(cfg).unwrap();

        let batch = extractor.extract(None).await.unwrap();
        let cursor = batch.cursor.clone().unwrap();

        let resumed = extractor.extract(Some(cursor)).await.unwrap();
        assert!(resumed.is_empty());
        assert!(extractor.is_finished());
    }
}
