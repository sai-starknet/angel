use crate::sqlite::append_only::append_only_record_queries;
use crate::sqlite::record::insert_record_queries;
use crate::sqlite::table::{
    create_table_query, persist_table_state_query, qualified_table_name, update_column,
    update_columns, FETCH_TABLES_QUERY,
};
use crate::{
    DbColumn, DbDeadField, DbResult, DbTable, IntrospectDb, IntrospectInitialize,
    IntrospectQueryMaker, IntrospectSqlSink, RecordResult, Table, TableResult, UpgradeResultExt,
};
use async_trait::async_trait;
use introspect_types::{ColumnDef, ColumnInfo, PrimaryDef, ResultInto};
use itertools::Itertools;
use sqlx::prelude::FromRow;
use sqlx::types::Json;
use starknet_types_core::felt::{Felt, FromStrError};
use std::collections::HashMap;
use torii_introspect::Record;
use torii_sql::{PoolExt, Queries, Sqlite, SqlitePool, SqliteQuery};

pub const INTROSPECT_SQLITE_SINK_MIGRATIONS: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/sqlite");

pub type IntrospectSqliteDb = IntrospectDb<SqlitePool>;

#[derive(FromRow)]
pub struct SqliteTableRow {
    namespace: String,
    id: String,
    owner: String,
    name: String,
    primary: Json<PrimaryDef>,
    columns: Json<HashMap<Felt, ColumnInfo>>,
    append_only: bool,
    alive: bool,
}

impl TryFrom<SqliteTableRow> for DbTable {
    type Error = FromStrError;
    fn try_from(value: SqliteTableRow) -> Result<Self, FromStrError> {
        Ok(DbTable {
            namespace: value.namespace,
            id: Felt::from_hex(&value.id)?,
            owner: Felt::from_hex(&value.owner)?,
            name: value.name,
            primary: value.primary.0,
            columns: value.columns.0.into_iter().map_into().collect(),
            dead: HashMap::new(),
            append_only: value.append_only,
            alive: value.alive,
        })
    }
}

#[async_trait]
impl IntrospectQueryMaker for Sqlite {
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
        queries: &mut Vec<SqliteQuery>,
    ) -> TableResult<()> {
        queries.add(create_table_query(
            namespace,
            name,
            primary,
            columns,
            append_only,
        )?);
        persist_table_state_query(
            namespace,
            id,
            name,
            primary,
            columns,
            append_only,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }
    fn update_table_queries(
        table: &mut Table,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<SqliteQuery>,
    ) -> TableResult<()> {
        let table_name = qualified_table_name(&table.namespace, name);
        if table.name != name {
            queries.add(format!(
                r#"ALTER TABLE "{}" RENAME TO "{table_name}""#,
                qualified_table_name(&table.namespace, &table.name),
            ));
            table.name = name.to_string();
        }
        update_column(
            &table_name,
            &mut table.primary,
            &primary.name,
            &((&primary.type_def).into()),
            queries,
        )
        .to_table_result(&table_name, "primary")?;
        update_columns(&mut table.columns, &table_name, columns, queries)?;
        persist_table_state_query(
            &table.namespace,
            &table.id,
            &table.name,
            primary,
            &table.columns.iter().collect_vec(),
            table.append_only,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
    }
    fn insert_record_queries(
        table: &Table,
        columns: &[Felt],
        records: &[Record],
        _from_address: &Felt,
        _block_number: u64,
        _transaction_hash: &Felt,
        queries: &mut Vec<SqliteQuery>,
    ) -> RecordResult<()> {
        if table.append_only {
            append_only_record_queries(
                table,
                columns,
                records,
                _from_address,
                _block_number,
                _transaction_hash,
                queries,
            )
        } else {
            insert_record_queries(
                table,
                columns,
                records,
                _from_address,
                _block_number,
                _transaction_hash,
                queries,
            )
        }
    }
}

impl IntrospectSqlSink for SqlitePool {
    const NAME: &'static str = "Introspect Sqlite";
}

#[async_trait]
impl IntrospectInitialize for SqlitePool {
    async fn load_tables(&self, _schemas: &Option<Vec<String>>) -> DbResult<Vec<DbTable>> {
        let rows: Vec<SqliteTableRow> = sqlx::query_as(FETCH_TABLES_QUERY)
            .fetch_all(self.pool())
            .await?;

        let tables: Vec<DbTable> = rows
            .into_iter()
            .map(|row| row.try_into())
            .collect::<Result<_, _>>()?;
        Ok(tables)
    }
    async fn load_columns(&self, _schemas: &Option<Vec<String>>) -> DbResult<Vec<DbColumn>> {
        Ok(Vec::new())
    }
    async fn load_dead_fields(&self, _schemas: &Option<Vec<String>>) -> DbResult<Vec<DbDeadField>> {
        Ok(Vec::new())
    }
    async fn initialize(&self) -> DbResult<()> {
        self.migrate(Some("introspect"), INTROSPECT_SQLITE_SINK_MIGRATIONS)
            .await
            .err_into()
    }
}
