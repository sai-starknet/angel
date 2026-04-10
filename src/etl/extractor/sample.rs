//! Simple extractor that cycles through predefined sample events
//!
//! This extractor is designed for demos and testing. Events are defined
//! in main.rs and passed to the extractor at initialization.

use anyhow::Result;
use async_trait::async_trait;
use starknet_types_raw::Felt;
use std::collections::HashMap;
use std::sync::Arc;

use crate::etl::engine_db::EngineDb;
use crate::etl::StarknetEvent;

use super::{BlockContext, ExtractionBatch, Extractor, TransactionContext};

/// Simple extractor that cycles through predefined events
pub struct SampleExtractor {
    /// Sample events to cycle through
    events: Vec<StarknetEvent>,
    /// Current index in the events array
    current_index: usize,
    /// Number of events to return per extraction
    batch_size: usize,
    /// Current block number for generated blocks
    current_block: u64,
}

impl SampleExtractor {
    /// Create a new sample extractor
    ///
    /// # Arguments
    /// * `events` - Predefined events to cycle through
    /// * `batch_size` - Number of events to return per extraction
    pub fn new(events: Vec<StarknetEvent>, batch_size: usize) -> Self {
        Self {
            events,
            current_index: 0,
            batch_size,
            current_block: 1000,
        }
    }

    /// Generate the next batch of events (cycling through the predefined list)
    fn next_batch(&mut self) -> Vec<StarknetEvent> {
        if self.events.is_empty() {
            return Vec::new();
        }

        let mut batch = Vec::new();

        for _ in 0..self.batch_size {
            // Get the next event from the cycle
            let mut event = self.events[self.current_index].clone();

            // Update block number to current block
            event.block_number = self.current_block;

            // Generate unique transaction hash based on position
            event.transaction_hash = Felt::from(2000 + self.current_index as u64);

            batch.push(event);

            // Move to next event (cycle)
            self.current_index = (self.current_index + 1) % self.events.len();

            // Advance block every 3 events
            if self.current_index.is_multiple_of(3) {
                self.current_block += 1;
            }
        }

        batch
    }
}

#[async_trait]
impl Extractor for SampleExtractor {
    fn set_start_block(&mut self, start_block: u64) {
        self.current_block = start_block.max(self.current_block);
    }
    fn is_finished(&self) -> bool {
        false // Sample extractor cycles infinitely, never finishes
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn extract(
        &mut self,
        _cursor: Option<String>,
        _engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        // Generate next batch of events
        let events = self.next_batch();

        if events.is_empty() {
            return Ok(ExtractionBatch::empty());
        }

        // Create block context (deduplicated)
        let mut blocks = HashMap::new();
        for event in &events {
            blocks.entry(event.block_number).or_insert_with(|| {
                Arc::new(BlockContext {
                    number: event.block_number,
                    hash: event.block_number.into(),
                    parent_hash: if event.block_number > 0 {
                        Felt::from(event.block_number - 1)
                    } else {
                        Felt::ZERO
                    },
                    timestamp: 1700000000 + event.block_number, // Realistic timestamp
                })
            });
        }

        // Create transaction context (deduplicated)
        let mut transactions = HashMap::new();
        for event in &events {
            transactions
                .entry(event.transaction_hash)
                .or_insert_with(|| {
                    Arc::new(TransactionContext {
                        hash: event.transaction_hash,
                        block_number: event.block_number,
                        sender_address: Some(
                            Felt::from_hex(
                                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd",
                            )
                            .unwrap(),
                        ),
                        calldata: vec![
                            Felt::from(0),                  // selector
                            Felt::from(self.current_block), // param1
                            Felt::from(42),                 // param2
                        ],
                    })
                });
        }

        tracing::debug!(
            target: "torii::etl::sample_extractor",
            "Generated {} sample events across {} blocks ({} transactions)",
            events.len(),
            blocks.len(),
            transactions.len()
        );

        Ok(ExtractionBatch {
            events,
            blocks,
            transactions,
            declared_classes: Vec::new(), // Sample extractor doesn't generate these
            deployed_contracts: Vec::new(), // Sample extractor doesn't generate these
            cursor: None,
            chain_head: None, // Sample extractor doesn't track chain head
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sample_extractor_cycling() {
        // Create sample events
        let events = vec![
            StarknetEvent {
                from_address: Felt::from(1u64),
                keys: vec![Felt::from(100u64)],
                data: vec![Felt::from(1000u64)],
                block_number: 0,
                transaction_hash: Felt::ZERO,
            },
            StarknetEvent {
                from_address: Felt::from(2u64),
                keys: vec![Felt::from(200u64)],
                data: vec![Felt::from(2000u64)],
                block_number: 0,
                transaction_hash: Felt::ZERO,
            },
        ];

        let mut extractor = SampleExtractor::new(events.clone(), 3);

        let engine_db_config = crate::etl::engine_db::EngineDbConfig {
            path: ":memory:".to_string(),
        };
        let engine_db = EngineDb::new(engine_db_config).await.unwrap();

        // First extraction (3 events from 2 samples = should cycle)
        let batch = extractor.extract(None, &engine_db).await.unwrap();
        assert_eq!(batch.len(), 3);

        // Verify cycling: should be [event1, event2, event1]
        assert_eq!(batch.events[0].from_address, Felt::from(1u64));
        assert_eq!(batch.events[1].from_address, Felt::from(2u64));
        assert_eq!(batch.events[2].from_address, Felt::from(1u64));
    }
}
