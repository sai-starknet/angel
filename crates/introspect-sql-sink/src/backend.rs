use crate::processor::{messages_to_queries, DbColumn, DbDeadField, DbTable, COMMIT_CMD};
use crate::table::Table;
use crate::tables::Tables;
use crate::{DbResult, NamespaceMode, RecordResult, TableResult};
use async_trait::async_trait;
use introspect_types::{ColumnDef, PrimaryDef};
use sqlx::{Database, Pool};
use starknet_types_core::felt::Felt;
use std::fmt::Debug;
use torii_introspect::events::IntrospectBody;
use torii_introspect::Record;
use torii_sql::{Executable, FlexQuery, PoolExt};

#[async_trait]
pub trait IntrospectProcessor {
    async fn process_msgs(
        &self,
        namespaces: &NamespaceMode,
        tables: &Tables,
        msgs: Vec<&IntrospectBody>,
    ) -> DbResult<Vec<DbResult<()>>>;
}

#[async_trait]
pub trait IntrospectInitialize {
    async fn initialize(&self) -> DbResult<()>;
    async fn load_tables(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbTable>>;
    async fn load_columns(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>>;
    async fn load_dead_fields(
        &self,
        namespaces: &Option<Vec<String>>,
    ) -> DbResult<Vec<DbDeadField>>;
}

#[allow(clippy::too_many_arguments)]
pub trait IntrospectQueryMaker: Database {
    fn create_table_queries(
        namespace: &str,
        id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        append_only: bool,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Self>>,
    ) -> TableResult<()>;
    fn update_table_queries(
        table: &mut Table,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Self>>,
    ) -> TableResult<()>;
    fn insert_record_queries(
        table: &Table,
        columns: &[Felt],
        records: &[Record],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<Self>>,
    ) -> RecordResult<()>;
}

#[async_trait]
pub trait IntrospectPool<DB: IntrospectQueryMaker> {
    async fn commit_queries(&self, queries: Vec<FlexQuery<DB>>) -> DbResult<()>;
    async fn execute_msgs(
        &self,
        namespaces: &NamespaceMode,
        tables: &Tables,
        msgs: Vec<&IntrospectBody>,
    ) -> DbResult<Vec<DbResult<()>>> {
        let mut queries = Vec::new();
        let results = messages_to_queries::<DB>(namespaces, tables, msgs, &mut queries)?;
        self.commit_queries(queries).await?;
        Ok(results)
    }
}

#[async_trait]
impl<DB: IntrospectQueryMaker> IntrospectPool<DB> for Pool<DB>
where
    Vec<FlexQuery<DB>>: Executable<DB>,
    FlexQuery<DB>: Debug + Clone,
{
    async fn commit_queries(&self, queries: Vec<FlexQuery<DB>>) -> DbResult<()> {
        let mut batch = Vec::new();
        for query in queries {
            if query == *COMMIT_CMD {
                self.execute_queries(std::mem::take(&mut batch)).await?;
            } else {
                batch.push(query);
            }
        }
        if !batch.is_empty() {
            match self.execute_queries(batch.clone()).await {
                Ok(_) => (),
                Err(e) => {
                    for query in batch {
                        eprintln!("Failed query: {query:?}");
                    }
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
}
