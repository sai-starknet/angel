use async_trait::async_trait;

use crate::events::{ContractEvents, ExtractedEvents};

#[async_trait]
pub trait Extractor {
    type Error;
    async fn next_batch(&mut self) -> Result<ExtractedEvents, Self::Error>;
    fn set_start_block(&mut self, block_number: u64);
    fn next_block_number(&self) -> u64;
    fn finished(&self) -> bool;
}

#[async_trait]
pub trait ContractsExtractor {
    type Error: Send;
    async fn next_events(&mut self) -> Vec<Result<ContractEvents, Self::Error>>;
    fn at_end_block(&self) -> bool;
}

#[async_trait]
pub trait BlockExtractor: Sync {
    type Error: Send;
    async fn fetch_block_with_receipts(
        &self,
        block_number: u64,
    ) -> Result<BlockWithReceipts, Self::Error>;
    async fn wait_for_block_with_receipts(
        &self,
        block_number: u64,
    ) -> Result<BlockWithReceipts, Self::Error>;

    async fn fetch_head(&self) -> Result<u64, Self::Error>;
    fn set_head(&mut self, block_number: u64);
    fn current_head(&self) -> u64;

    fn next_block_to_fetch(&self) -> u64;
    fn set_next_block(&mut self, next_block: u64);

    async fn fetch_blocks_with_receipts(
        &self,
        blocks: Vec<u64>,
    ) -> Result<Vec<BlockWithReceipts>, Self::Error> {
        let fetches = blocks
            .into_iter()
            .map(|block| self.fetch_block_with_receipts(block));
        futures::future::try_join_all(fetches).await
    }
    async fn wait_for_block_events(
        &mut self,
        block_number: u64,
    ) -> Result<BlockEvents, Self::Error> {
        let block = self
            .wait_for_block_with_receipts(block_number)
            .await?
            .into();
        self.set_next_block(block_number + 1);
        self.update_current_head(block_number);
        Ok(block)
    }
    async fn get_block_events(&self, block_number: u64) -> Result<BlockEvents, Self::Error> {
        self.fetch_block_with_receipts(block_number)
            .await
            .map(Into::into)
    }
    async fn get_blocks_events(
        &self,
        block_numbers: Vec<u64>,
    ) -> Result<Vec<BlockEvents>, Self::Error> {
        let fetches = block_numbers
            .into_iter()
            .map(|block| self.get_block_events(block));
        futures::future::try_join_all(fetches).await
    }
    async fn next_block_events(&mut self) -> Result<BlockEvents, Self::Error> {
        let next_block = self.next_block_to_fetch();
        let block: BlockEvents = self.get_block_events(next_block).await?;
        self.update_current_head(block.block.block_number);
        self.set_next_block(block.block.block_number + 1);
        Ok(block)
    }

    async fn next_blocks_events(
        &mut self,
        batch_size: u64,
    ) -> Result<Vec<BlockEvents>, Self::Error> {
        let (start, end) = self.next_batch_range(batch_size).await?;
        let blocks = self.get_blocks_events((start..end).collect()).await?;
        self.set_next_block(end);
        Ok(blocks)
    }

    async fn update_latest_block_number(&mut self) -> Result<u64, Self::Error> {
        let head = self.fetch_head().await?;
        self.set_head(head);
        Ok(head)
    }

    fn update_current_head(&mut self, block_number: u64) {
        if block_number > self.current_head() {
            self.set_head(block_number);
        }
    }

    async fn next_batch_range(&mut self, batch_size: u64) -> Result<(u64, u64), Self::Error> {
        let start = self.next_block_to_fetch();
        let end = start + batch_size;
        if end <= self.current_head() + 1 {
            Ok((start, end))
        } else {
            Ok((start, min(end, self.update_latest_block_number().await?)))
        }
    }
    async fn wait_for_next_batch(
        &mut self,
        batch_size: u64,
    ) -> Result<Vec<BlockEvents>, Self::Error> {
        let (start, end) = self.next_batch_range(batch_size).await?;
        if start < end {
            let blocks = self.get_blocks_events((start..=end).collect()).await?;
            self.set_next_block(end);
            Ok(blocks)
        } else {
            Ok(vec![self.wait_for_block_events(start).await?])
        }
    }
}
