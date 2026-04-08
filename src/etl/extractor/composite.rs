//! Composite extractor that wraps multiple extractors.
//!
//! The `CompositeExtractor` allows running multiple extractors within a single ETL pipeline,
//! enabling scenarios like:
//! - Following chain head while backfilling historical data
//! - Combining block-range and event-based extraction
//! - Running multiple independent extraction strategies
//!
//! # Scheduling Strategy
//!
//! The composite extractor uses round-robin scheduling with priority for non-empty batches:
//! 1. Try each extractor in sequence
//! 2. Skip finished extractors
//! 3. Return the first non-empty batch
//! 4. If all return empty, return empty batch
//!
//! # Cursor Management
//!
//! Each child extractor manages its own cursor independently in EngineDb.
//! The composite extractor's `commit_cursor()` delegates to all children.
//!
//! # Example
//!
//! ```rust,ignore
//! use torii::etl::extractor::{
//!     CompositeExtractor, BlockRangeExtractor, EventExtractor,
//!     BlockRangeConfig, EventExtractorConfig,
//! };
//!
//! // Create extractors
//! let block_extractor = Box::new(BlockRangeExtractor::new(provider.clone(), block_config));
//! let event_extractor = Box::new(EventExtractor::new(provider.clone(), event_config));
//!
//! // Combine them - event extractor first for priority backfill
//! let composite = CompositeExtractor::new(vec![event_extractor, block_extractor]);
//!
//! // Use with ToriiConfig
//! let config = ToriiConfig::builder()
//!     .with_extractor(Box::new(composite))
//!     .build();
//! ```

use anyhow::Result;
use async_trait::async_trait;

use crate::etl::engine_db::EngineDb;
use crate::etl::extractor::{ExtractionBatch, Extractor};

/// Composite extractor that wraps multiple extractors.
///
/// Combines multiple extractors into a single extractor that can be used with
/// the standard Torii ETL pipeline. Each child extractor maintains its own
/// cursor and state independently.
pub struct CompositeExtractor {
    /// Child extractors in priority order (first = highest priority).
    extractors: Vec<Box<dyn Extractor>>,

    /// Current extractor index for round-robin scheduling.
    current_index: usize,
}

impl CompositeExtractor {
    /// Create a new composite extractor.
    ///
    /// # Arguments
    ///
    /// * `extractors` - Child extractors in priority order. The first extractor
    ///   that returns a non-empty batch will be used each iteration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Event extractor has priority (for backfill completion)
    /// let composite = CompositeExtractor::new(vec![
    ///     event_extractor,  // Priority: backfill
    ///     block_extractor,  // Fallback: chain head
    /// ]);
    /// ```
    pub fn new(extractors: Vec<Box<dyn Extractor>>) -> Self {
        Self {
            extractors,
            current_index: 0,
        }
    }

    /// Returns number of active (non-finished) extractors.
    pub fn active_count(&self) -> usize {
        self.extractors.iter().filter(|e| !e.is_finished()).count()
    }

    /// Returns total number of extractors.
    pub fn total_count(&self) -> usize {
        self.extractors.len()
    }
}

#[async_trait]
impl Extractor for CompositeExtractor {
    fn set_start_block(&mut self, start_block: u64) {
        for extractor in &mut self.extractors {
            extractor.set_start_block(start_block);
        }
    }
    async fn extract(
        &mut self,
        _cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        if self.extractors.is_empty() {
            return Ok(ExtractionBatch::empty());
        }

        // Try each extractor starting from current_index
        let num_extractors = self.extractors.len();
        let start_index = self.current_index;

        for i in 0..num_extractors {
            let index = (start_index + i) % num_extractors;
            let extractor = &mut self.extractors[index];

            // Skip finished extractors
            if extractor.is_finished() {
                continue;
            }

            // Try to extract
            let batch = extractor.extract(None, engine_db).await?;

            if !batch.is_empty() {
                // Move to next extractor for fairness on next call
                self.current_index = (index + 1) % num_extractors;

                tracing::debug!(
                    target: "torii::etl::composite",
                    extractor_index = index,
                    events = batch.len(),
                    active_extractors = self.active_count(),
                    "Extracted batch from child extractor"
                );

                return Ok(batch);
            }
        }

        // All extractors returned empty batches
        Ok(ExtractionBatch::empty())
    }

    fn is_finished(&self) -> bool {
        self.extractors.iter().all(|e| e.is_finished())
    }

    async fn commit_cursor(&mut self, _cursor: &str, engine_db: &EngineDb) -> Result<()> {
        // Delegate to all extractors - each manages its own state
        for extractor in &mut self.extractors {
            extractor.commit_cursor("", engine_db).await?;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::extractor::SampleExtractor;
    use starknet_types_raw::Felt;
    use torii_types::event::StarknetEvent;

    fn make_sample_extractor() -> SampleExtractor {
        let events = vec![StarknetEvent {
            from_address: Felt::from(1u64),
            keys: vec![Felt::from(100u64)],
            data: vec![Felt::from(1000u64)],
            block_number: 0,
            transaction_hash: Felt::ZERO,
        }];
        SampleExtractor::new(events, 1)
    }

    #[tokio::test]
    async fn test_composite_empty() {
        let composite = CompositeExtractor::new(vec![]);
        assert!(composite.is_finished());
        assert_eq!(composite.active_count(), 0);
        assert_eq!(composite.total_count(), 0);
    }

    #[tokio::test]
    async fn test_composite_active_count() {
        // SampleExtractor cycles through predefined events and never finishes
        let extractor1 = Box::new(make_sample_extractor()) as Box<dyn Extractor>;
        let extractor2 = Box::new(make_sample_extractor()) as Box<dyn Extractor>;

        let composite = CompositeExtractor::new(vec![extractor1, extractor2]);

        assert_eq!(composite.total_count(), 2);
        assert_eq!(composite.active_count(), 2);
        assert!(!composite.is_finished());
    }
}
