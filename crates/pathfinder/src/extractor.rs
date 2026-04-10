use crate::{connect, EventFetcher, PFResult};
use rusqlite::Connection;
use std::path::Path;
use std::sync::Mutex;
use torii_types::block::BlockContext;
use torii_types::event::StarknetEvent;
#[derive(Debug)]
pub struct PathfinderExtractor {
    pub conn: Mutex<Connection>,
    pub batch: u64,
    pub current: u64,
    pub end: u64,
    pub finished: bool,
}

#[derive(Debug)]
pub struct PathfinderCombinedExtractor<T> {
    pub pathfinder: PathfinderExtractor,
    pub head: T,
    pub on_head: bool,
}

impl PathfinderExtractor {
    pub fn new<P: AsRef<Path>>(path: P, batch: u64, start: u64, end: u64) -> PFResult<Self> {
        Ok(Self {
            conn: connect(path)?.into(),
            batch,
            current: start,
            end,
            finished: false,
        })
    }

    pub fn next_batch(&mut self) -> PFResult<(Vec<BlockContext>, Vec<StarknetEvent>)> {
        let mut last = self.current + self.batch;
        if last >= self.end {
            last = self.end;
            self.finished = true;
        }
        let (blocks, events) = self
            .conn
            .lock()?
            .get_events_with_context(self.current, last)?;
        let last_block = blocks.last().map_or(self.current, |b| b.number);
        if last_block < last {
            self.finished = true;
        }
        self.current = last_block + 1;
        Ok((blocks, events.into_iter().map(Into::into).collect()))
    }
}

impl<T> PathfinderCombinedExtractor<T> {
    pub fn new<P: AsRef<Path>>(
        path: P,
        batch: u64,
        start: u64,
        end: u64,
        head: T,
    ) -> PFResult<Self> {
        Ok(Self {
            pathfinder: PathfinderExtractor::new(path, batch, start, end)?,
            head,
            on_head: false,
        })
    }
}

#[cfg(feature = "etl")]
mod etl {
    use super::{PathfinderCombinedExtractor, PathfinderExtractor};
    use anyhow::Result as AnyResult;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use torii::etl::{EngineDb, ExtractionBatch, Extractor};

    #[async_trait]
    impl Extractor for PathfinderExtractor {
        fn set_start_block(&mut self, start_block: u64) {
            self.current = start_block.max(self.current);
        }
        async fn extract(
            &mut self,
            _cursor: Option<String>,
            _engine_db: &EngineDb,
        ) -> AnyResult<ExtractionBatch> {
            let (blocks, events) = self.next_batch()?;
            let blocks = blocks
                .into_iter()
                .map(|b| (b.number, Arc::new(b)))
                .collect();
            Ok(ExtractionBatch {
                events,
                blocks,
                transactions: HashMap::new(),
                declared_classes: Vec::new(),
                deployed_contracts: Vec::new(),
                cursor: None,
                chain_head: None,
            })
        }
        fn is_finished(&self) -> bool {
            self.current >= self.end
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[async_trait]
    impl<T: Extractor + Send + Sync + 'static> Extractor for PathfinderCombinedExtractor<T> {
        fn set_start_block(&mut self, start_block: u64) {
            self.pathfinder.set_start_block(start_block);
            self.head.set_start_block(start_block);
        }
        async fn extract(
            &mut self,
            cursor: Option<String>,
            engine_db: &EngineDb,
        ) -> AnyResult<ExtractionBatch> {
            if self.on_head {
                self.head.extract(cursor, engine_db).await
            } else if self.pathfinder.is_finished() {
                self.on_head = true;
                self.head.set_start_block(self.pathfinder.current);
                self.head.extract(cursor, engine_db).await
            } else {
                self.pathfinder.extract(cursor, engine_db).await
            }
        }
        fn is_finished(&self) -> bool {
            self.pathfinder.is_finished() && self.head.is_finished()
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
}
