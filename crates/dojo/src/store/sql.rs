use crate::store::DojoStoreTrait;
use crate::{DojoTable, DojoToriiResult};
use async_trait::async_trait;
use starknet_types_raw::Felt;
use torii_sql::DbPool;

#[async_trait]
impl DojoStoreTrait for DbPool {
    async fn initialize(&self) -> DojoToriiResult {
        match self {
            DbPool::Postgres(pool) => pool.initialize().await,
            DbPool::Sqlite(pool) => pool.initialize().await,
        }
    }

    async fn save_table(
        &self,
        owner: Felt,
        table: &DojoTable,
        tx_hash: Felt,
        block_number: u64,
    ) -> DojoToriiResult {
        match self {
            DbPool::Postgres(pool) => pool.save_table(owner, table, tx_hash, block_number).await,
            DbPool::Sqlite(pool) => pool.save_table(owner, table, tx_hash, block_number).await,
        }
    }

    async fn read_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        match self {
            DbPool::Postgres(pool) => pool.read_tables(owners).await,
            DbPool::Sqlite(pool) => pool.read_tables(owners).await,
        }
    }
}
