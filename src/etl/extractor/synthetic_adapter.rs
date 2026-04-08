//! Adapter that exposes a [`SyntheticExtractor`] as a standard [`Extractor`].

use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;

use crate::etl::engine_db::EngineDb;

use super::{ExtractionBatch, Extractor, SyntheticExtractor};

const STATE_KEY: &str = "cursor";

/// Bridges deterministic synthetic generators into the main ETL extractor interface.
///
/// This lets tests and local runs exercise the regular Torii ingestion pipeline without
/// needing a live Starknet provider. Cursor persistence is handled generically by storing
/// the synthetic extractor's opaque cursor string in the engine database.
pub struct SyntheticExtractorAdapter<T>
where
    T: SyntheticExtractor + Send + Sync + 'static,
{
    inner: T,
    initialized: bool,
}

impl<T> SyntheticExtractorAdapter<T>
where
    T: SyntheticExtractor + Send + Sync + 'static,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            initialized: false,
        }
    }
}

#[async_trait]
impl<T> Extractor for SyntheticExtractorAdapter<T>
where
    T: SyntheticExtractor + Send + Sync + 'static,
{
    fn set_start_block(&mut self, _start_block: u64) {}
    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        let cursor = if self.initialized {
            cursor
        } else {
            self.initialized = true;
            match cursor {
                Some(cursor) => Some(cursor),
                None => {
                    engine_db
                        .get_extractor_state(self.inner.extractor_name(), STATE_KEY)
                        .await?
                }
            }
        };

        self.inner.extract(cursor).await
    }

    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    async fn commit_cursor(&mut self, cursor: &str, engine_db: &EngineDb) -> Result<()> {
        engine_db
            .set_extractor_state(self.inner.extractor_name(), STATE_KEY, cursor)
            .await
            .with_context(|| {
                format!(
                    "failed to commit synthetic cursor for {}",
                    self.inner.extractor_name()
                )
            })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::etl::engine_db::{EngineDb, EngineDbConfig};
    use crate::etl::extractor::{Extractor, SyntheticExtractor};
    use anyhow::{Context, Result};
    use async_trait::async_trait;

    use super::SyntheticExtractorAdapter;

    #[derive(Clone)]
    struct TestSyntheticConfig {
        from_block: u64,
        block_count: u64,
    }

    struct TestSyntheticExtractor {
        current_block: u64,
        final_block: u64,
        finished: bool,
    }

    impl TestSyntheticExtractor {
        fn parse_cursor(cursor: &str) -> Result<u64> {
            cursor
                .strip_prefix("test:block:")
                .context("invalid test cursor format")?
                .parse::<u64>()
                .context("invalid test cursor block number")
        }

        fn make_cursor(block: u64) -> String {
            format!("test:block:{block}")
        }
    }

    #[async_trait]
    impl SyntheticExtractor for TestSyntheticExtractor {
        type Config = TestSyntheticConfig;

        fn new(config: Self::Config) -> Result<Self> {
            Ok(Self {
                current_block: config.from_block,
                final_block: config.from_block + config.block_count - 1,
                finished: false,
            })
        }

        async fn extract(
            &mut self,
            cursor: Option<String>,
        ) -> Result<crate::etl::extractor::ExtractionBatch> {
            if let Some(cursor) = cursor {
                self.current_block = Self::parse_cursor(&cursor)?.saturating_add(1);
            }

            if self.current_block > self.final_block {
                self.finished = true;
            }

            if self.finished {
                return Ok(crate::etl::extractor::ExtractionBatch::empty());
            }

            let block = self.current_block;
            self.current_block += 1;
            self.finished = self.current_block > self.final_block;

            let mut batch = crate::etl::extractor::ExtractionBatch::empty();
            batch.set_cursor(Self::make_cursor(block));
            batch.add_block_context(
                block,
                starknet::core::types::Felt::ZERO.into(),
                starknet::core::types::Felt::ZERO.into(),
                0,
            );
            Ok(batch)
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn extractor_name(&self) -> &'static str {
            "test_synthetic"
        }
    }

    #[tokio::test]
    async fn adapter_resumes_from_committed_cursor() {
        let engine_db = EngineDb::new(EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();

        let cfg = TestSyntheticConfig {
            from_block: 100,
            block_count: 3,
        };

        let mut first =
            SyntheticExtractorAdapter::new(TestSyntheticExtractor::new(cfg.clone()).unwrap());
        let batch1 = first.extract(None, &engine_db).await.unwrap();
        assert_eq!(batch1.max_block(), Some(100));

        let cursor = batch1.cursor.clone().unwrap();
        first.commit_cursor(&cursor, &engine_db).await.unwrap();

        let mut restarted =
            SyntheticExtractorAdapter::new(TestSyntheticExtractor::new(cfg).unwrap());
        let batch2 = restarted.extract(None, &engine_db).await.unwrap();

        assert_eq!(batch2.max_block(), Some(101));
        assert_eq!(batch2.events.len(), 0);
    }

    #[tokio::test]
    async fn adapter_returns_empty_batch_after_persisted_final_cursor() {
        let engine_db = EngineDb::new(EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .unwrap();

        let cfg = TestSyntheticConfig {
            from_block: 50,
            block_count: 1,
        };

        let mut first =
            SyntheticExtractorAdapter::new(TestSyntheticExtractor::new(cfg.clone()).unwrap());
        let batch = first.extract(None, &engine_db).await.unwrap();
        let cursor = batch.cursor.clone().unwrap();
        first.commit_cursor(&cursor, &engine_db).await.unwrap();

        let mut restarted =
            SyntheticExtractorAdapter::new(TestSyntheticExtractor::new(cfg).unwrap());
        let resumed = restarted.extract(None, &engine_db).await.unwrap();

        assert!(resumed.is_empty());
        assert!(restarted.is_finished());
    }
}
