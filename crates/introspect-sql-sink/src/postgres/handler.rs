use super::query::{fetch_columns, fetch_dead_fields, fetch_tables};
use crate::backend::IntrospectInitialize;
use crate::{DbColumn, DbDeadField, DbResult, DbTable};
use async_trait::async_trait;
use introspect_types::ResultInto;
use sqlx::PgPool;
use torii_sql::PoolExt;

pub const INTROSPECT_PG_SINK_MIGRATIONS: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/postgres");

#[async_trait]
impl IntrospectInitialize for PgPool {
    async fn load_tables(&self, schemas: &Option<Vec<String>>) -> DbResult<Vec<DbTable>> {
        fetch_tables(self.pool(), schemas).await.err_into()
    }
    async fn load_columns(&self, schemas: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>> {
        fetch_columns(self.pool(), schemas).await.err_into()
    }
    async fn load_dead_fields(&self, schemas: &Option<Vec<String>>) -> DbResult<Vec<DbDeadField>> {
        fetch_dead_fields(self.pool(), schemas).await.err_into()
    }
    async fn initialize(&self) -> DbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_PG_SINK_MIGRATIONS)
            .await
            .err_into()
    }
}
