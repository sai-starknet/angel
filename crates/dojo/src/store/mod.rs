pub mod json;

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "postgres")]
#[cfg(feature = "sqlite")]
pub mod sql;

use crate::table::DojoTableInfo;
use crate::{DojoTable, DojoToriiResult};
use async_trait::async_trait;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;

#[async_trait]
pub trait DojoStoreTrait {
    async fn initialize(&self) -> DojoToriiResult;
    async fn save_table(
        &self,
        owner: Felt,
        table: &DojoTable,
        tx_hash: Felt,
        block_number: u64,
    ) -> DojoToriiResult;
    async fn read_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>>;
    async fn read_table_map(
        &self,
        owners: &[Felt],
    ) -> DojoToriiResult<HashMap<Felt, DojoTableInfo>> {
        Ok(self
            .read_tables(owners)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}
