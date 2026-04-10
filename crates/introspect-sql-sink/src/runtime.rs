use crate::tables::Tables;
use crate::{
    DbColumn, DbDeadField, DbResult, DbTable, IntrospectInitialize, IntrospectProcessor,
    IntrospectSqlSink, NamespaceMode,
};
use async_trait::async_trait;
use torii_introspect::events::IntrospectBody;
use torii_sql::DbPool;

impl IntrospectSqlSink for DbPool {
    const NAME: &'static str = "introspect-sql";
}

#[async_trait]
impl IntrospectInitialize for DbPool {
    async fn initialize(&self) -> DbResult {
        match self {
            DbPool::Postgres(pg) => pg.initialize().await,
            DbPool::Sqlite(site) => site.initialize().await,
        }
    }
    async fn load_tables(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbTable>> {
        match self {
            DbPool::Postgres(pg) => pg.load_tables(namespaces).await,
            DbPool::Sqlite(site) => site.load_tables(namespaces).await,
        }
    }
    async fn load_columns(&self, namespaces: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>> {
        match self {
            DbPool::Postgres(pg) => pg.load_columns(namespaces).await,
            DbPool::Sqlite(site) => site.load_columns(namespaces).await,
        }
    }
    async fn load_dead_fields(
        &self,
        namespaces: &Option<Vec<String>>,
    ) -> DbResult<Vec<DbDeadField>> {
        match self {
            DbPool::Postgres(pg) => pg.load_dead_fields(namespaces).await,
            DbPool::Sqlite(site) => site.load_dead_fields(namespaces).await,
        }
    }
}

#[async_trait]
impl IntrospectProcessor for DbPool {
    async fn process_msgs(
        &self,
        namespaces: &NamespaceMode,
        tables: &Tables,
        msgs: Vec<&IntrospectBody>,
    ) -> DbResult<Vec<DbResult<()>>> {
        match self {
            DbPool::Postgres(pg) => pg.process_msgs(namespaces, tables, msgs).await,
            DbPool::Sqlite(site) => site.process_msgs(namespaces, tables, msgs).await,
        }
    }
}
