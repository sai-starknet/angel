use crate::decoding::BlockEvents;
use crate::sqlite::{BlockContextRow, SqliteExt};
use crate::{PFError, PFResult};
use rusqlite::Connection;
use torii_types::block::BlockContext;
use torii_types::event::StarknetEvent;

impl From<BlockContextRow> for BlockContext {
    fn from(value: BlockContextRow) -> Self {
        BlockContext {
            number: value.number,
            hash: value.hash,
            parent_hash: value.parent_hash,
            timestamp: value.timestamp,
        }
    }
}

pub trait EventFetcher {
    fn get_events(&self, from_block: u64, to_block: u64) -> PFResult<Vec<StarknetEvent>>;
    fn get_events_with_context(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<(Vec<BlockContext>, Vec<StarknetEvent>)>;
}
impl EventFetcher for Connection {
    fn get_events(&self, from_block: u64, to_block: u64) -> PFResult<Vec<StarknetEvent>> {
        let total_events = self.get_number_of_events_for_blocks(from_block, to_block)?;
        let mut tx_hashes = self
            .get_block_tx_hash_rows(from_block, to_block)?
            .into_iter();
        let mut events = Vec::with_capacity(total_events as usize);

        for row in self.get_block_events_rows(from_block, to_block)? {
            let block: BlockEvents = row.try_into()?;
            for transaction in block.transactions {
                let transaction_hash = tx_hashes
                    .next()
                    .map(|r| r.1)
                    .ok_or_else(|| PFError::tx_hash_mismatch(block.block_number))?;
                for event in transaction {
                    events.push(StarknetEvent {
                        block_number: block.block_number,
                        data: event.data,
                        from_address: event.from_address,
                        keys: event.keys,
                        transaction_hash,
                    });
                }
            }
        }
        Ok(events)
    }

    fn get_events_with_context(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> PFResult<(Vec<BlockContext>, Vec<StarknetEvent>)> {
        let context_rows = self.get_block_context_rows(from_block, to_block)?;
        let total_events: u64 = context_rows.iter().map(|r| r.event_count).sum();
        let mut tx_hashes = self
            .get_block_tx_hash_rows(from_block, to_block)?
            .into_iter();
        let mut emitted_events = Vec::with_capacity(total_events as usize);
        let mut contexts = Vec::with_capacity(context_rows.len());
        let mut ctx_iter = context_rows.into_iter();

        for row in self.get_block_events_rows(from_block, to_block)? {
            let block = BlockEvents::try_from(row)?;
            let ctx = loop {
                match ctx_iter.next() {
                    Some(ctx) => match ctx.number.cmp(&block.block_number) {
                        std::cmp::Ordering::Equal => break ctx,
                        std::cmp::Ordering::Less => contexts.push(ctx.into()),
                        std::cmp::Ordering::Greater => {
                            return Err(PFError::block_context_missing(block.block_number))
                        }
                    },
                    None => return Err(PFError::block_context_missing(block.block_number)),
                }
            };
            for transaction in block.transactions {
                let transaction_hash = tx_hashes
                    .next()
                    .map(|r| r.1)
                    .ok_or_else(|| PFError::tx_hash_mismatch(block.block_number))?;
                for event in transaction {
                    emitted_events.push(StarknetEvent {
                        block_number: block.block_number,
                        data: event.data,
                        from_address: event.from_address,
                        keys: event.keys,
                        transaction_hash,
                    });
                }
            }
            contexts.push(ctx.into());
        }

        Ok((contexts, emitted_events))
    }
}
