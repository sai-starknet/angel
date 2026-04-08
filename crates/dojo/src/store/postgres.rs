use super::DojoStoreTrait;
use crate::decoder::primary_field_def;
use crate::table::DojoTableInfo;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use async_trait::async_trait;
use introspect_types::{Attribute, ColumnInfo, TypeDef};
use itertools::Itertools;
use sqlx::migrate::Migrator;
use sqlx::query::Query;
use sqlx::types::Json;
use sqlx::FromRow;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii_introspect::postgres::owned::PgTypeDef;
use torii_introspect::postgres::PgFelt;
use torii_introspect::schema::ColumnKeyTrait;
use torii_sql::{PgArguments, PgPool, PoolExt, Postgres, SqlxResult};

pub const FETCH_TABLES_QUERY: &str = r#"
    SELECT DISTINCT ON (owner, id)
        id,
        name,
        attributes,
        keys,
        "values",
        legacy
    FROM dojo.tables
    WHERE $1::felt252[] = '{}' OR owner = ANY($1)
    ORDER BY owner, id, block_number DESC"#;

pub const FETCH_COLUMNS_QUERY: &str = r#"
    SELECT DISTINCT ON (owner, "table", id)
        "table",
        id,
        name,
        attributes,
        type_def
    FROM dojo.columns
    WHERE $1::felt252[] = '{}' OR owner = ANY($1)
    ORDER BY owner, "table", id, block_number DESC"#;

pub const FETCH_TABLES_BEFORE_QUERY: &str = r#"
    SELECT DISTINCT ON (owner, id)
        id,
        name,
        attributes,
        keys,
        "values",
        legacy
    FROM dojo.tables
    WHERE ($1::felt252[] = '{}' OR owner = ANY($1)) AND block_number < $2::uint64
    ORDER BY owner, id, block_number DESC"#;

pub const FETCH_COLUMNS_BEFORE_QUERY: &str = r#"
    SELECT DISTINCT ON (owner, "table", id)
        "table",
        id,
        name,
        attributes,
        type_def
    FROM dojo.columns
    WHERE ($1::felt252[] = '{}' OR owner = ANY($1)) AND block_number < $2::uint64
    ORDER BY owner, "table", id, block_number DESC"#;

pub const INSERT_TABLE_QUERY: &str = r#"
    INSERT INTO dojo.tables (owner, id, block_number, name, attributes, keys, "values", legacy, updated_at, created_tx, updated_tx)
        VALUES ($1, $2, $3::uint64, $4, $5, $6, $7, $8, NOW(), $9, $9)
        ON CONFLICT (owner, id, block_number) DO UPDATE SET
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes,
        keys = EXCLUDED.keys,
        "values" = EXCLUDED."values",
        legacy = EXCLUDED.legacy,
        updated_at = NOW(),
        updated_tx = EXCLUDED.updated_tx"#;

pub const INSERT_COLUMN_QUERY: &str = r#"
    INSERT INTO dojo.columns (owner, "table", id, block_number, name, attributes, type_def,  updated_at, created_tx, updated_tx)
        VALUES ($1, $2, $3, $4::uint64, $5, $6, $7, NOW(), $8, $8)
        ON CONFLICT (owner, "table", id, block_number) DO UPDATE SET
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes,
        type_def = EXCLUDED.type_def,
        updated_at = NOW(),
        updated_tx = EXCLUDED.updated_tx"#;

pub const DOJO_STORE_MIGRATIONS: Migrator = sqlx::migrate!("./migrations/postgres");

#[derive(Debug, thiserror::Error)]
pub enum DojoPgStoreError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error("historical schema bootstrap is not supported from dojo.table")]
    UnsupportedHistoricalLoad,
    #[error("Column not found for table {name} with id {table_id} and column {column_id}")]
    ColumnNotFound {
        name: String,
        table_id: Felt,
        column_id: Felt,
    },
}

impl DojoPgStoreError {
    pub fn column_not_found<K: ColumnKeyTrait>(name: String, key: &K) -> Self {
        let (table_id, column_id) = key.as_parts();
        Self::ColumnNotFound {
            name,
            table_id: *table_id,
            column_id: *column_id,
        }
    }
}

#[derive(FromRow)]
pub struct DojoTableRow {
    id: PgFelt,
    name: String,
    attributes: Vec<String>,
    keys: Vec<PgFelt>,
    #[sqlx(rename = "values")]
    values: Vec<PgFelt>,
    legacy: bool,
}

#[derive(FromRow)]
pub struct DojoColumnRow {
    table: PgFelt,
    id: PgFelt,
    name: String,
    attributes: Vec<String>,
    type_def: Json<TypeDef>,
}

impl From<DojoTableRow> for ((), DojoTable) {
    fn from(value: DojoTableRow) -> Self {
        (
            (),
            DojoTable {
                id: value.id.into(),
                name: value.name,
                attributes: value.attributes,
                primary: primary_field_def(),
                columns: HashMap::new(),
                key_fields: value.keys.into_iter().map_into().collect(),
                value_fields: value.values.into_iter().map_into().collect(),
                legacy: value.legacy,
            },
        )
    }
}

impl From<DojoTableRow> for (Felt, DojoTableInfo) {
    fn from(value: DojoTableRow) -> Self {
        (
            value.id.into(),
            DojoTableInfo {
                name: value.name,
                attributes: value.attributes,
                primary: primary_field_def(),
                columns: HashMap::new(),
                key_fields: value.keys.into_iter().map_into().collect(),
                value_fields: value.values.into_iter().map_into().collect(),
                legacy: value.legacy,
            },
        )
    }
}

impl<K> From<DojoColumnRow> for (K, ColumnInfo)
where
    K: ColumnKeyTrait,
{
    fn from(value: DojoColumnRow) -> Self {
        (
            K::from_parts(value.table.into(), value.id.into()),
            ColumnInfo {
                name: value.name,
                attributes: value
                    .attributes
                    .into_iter()
                    .map(Attribute::new_empty)
                    .collect(),
                type_def: value.type_def.0,
            },
        )
    }
}

pub fn table_insert_query(
    owner: Felt,
    id: Felt,
    block_number: u64,
    name: &str,
    attributes: &[String],
    keys: &[Felt],
    values: &[Felt],
    legacy: bool,
    created_tx: Felt,
) -> Query<'static, Postgres, PgArguments> {
    sqlx::query::<Postgres>(INSERT_TABLE_QUERY)
        .bind(PgFelt::from(owner))
        .bind(PgFelt::from(id))
        .bind(block_number.to_string())
        .bind(name.to_owned())
        .bind(attributes.to_owned())
        .bind(keys.iter().copied().map(PgFelt::from).collect_vec())
        .bind(values.iter().copied().map(PgFelt::from).collect_vec())
        .bind(legacy)
        .bind(PgFelt::from(created_tx))
}

pub fn column_info_insert_query(
    query: &'static str,
    owner: Felt,
    table: Felt,
    id: Felt,
    block_number: u64,
    info: &ColumnInfo,
    created_tx: Felt,
) -> Query<'static, Postgres, PgArguments> {
    column_insert_query(
        query,
        owner,
        table,
        id,
        block_number,
        &info.name,
        info.attributes.iter().map(|s| s.name.clone()).collect_vec(),
        &info.type_def,
        created_tx,
    )
}

pub fn column_insert_query(
    query: &'static str,
    owner: Felt,
    table: Felt,
    id: Felt,
    block_number: u64,
    name: &str,
    attributes: Vec<String>,
    type_def: &TypeDef,
    created_tx: Felt,
) -> Query<'static, Postgres, PgArguments> {
    sqlx::query::<Postgres>(query)
        .bind(PgFelt::from(owner))
        .bind(PgFelt::from(table))
        .bind(PgFelt::from(id))
        .bind(block_number.to_string())
        .bind(name.to_owned())
        .bind(attributes)
        .bind(Json(type_def.clone()))
        .bind(PgFelt::from(created_tx))
}

impl DojoTable {
    pub fn insert_query(
        &self,
        owner: Felt,
        tx_hash: Felt,
        block_number: u64,
    ) -> Query<'static, Postgres, PgArguments> {
        table_insert_query(
            owner,
            self.id,
            block_number,
            &self.name,
            &self.attributes,
            &self.key_fields.iter().copied().collect_vec(),
            &self.value_fields.iter().copied().collect_vec(),
            self.legacy,
            tx_hash,
        )
    }
}

pub struct PgStore<T>(pub T);

impl<T: PoolExt<Postgres>> PoolExt<Postgres> for PgStore<T> {
    fn pool(&self) -> &PgPool {
        self.0.pool()
    }
}

impl<T: PoolExt<Postgres> + Send + Sync> PgStore<T> {
    pub async fn initialize(&self) -> SqlxResult<()> {
        self.migrate(Some("dojo"), DOJO_STORE_MIGRATIONS).await
    }
}

impl<T: PoolExt<Postgres>> From<T> for PgStore<T> {
    fn from(pool: T) -> Self {
        PgStore(pool)
    }
}

#[async_trait]
impl DojoStoreTrait for PgPool {
    async fn initialize(&self) -> DojoToriiResult {
        self.migrate(Some("dojo"), DOJO_STORE_MIGRATIONS)
            .await
            .map_err(DojoToriiError::store_error)
    }
    async fn save_table(
        &self,
        owner: Felt,
        table: &DojoTable,
        tx_hash: Felt,
        block_number: u64,
    ) -> DojoToriiResult {
        let mut transaction = self.begin().await.map_err(DojoToriiError::store_error)?;
        table
            .insert_query(owner, tx_hash, block_number)
            .execute(&mut *transaction)
            .await
            .map_err(DojoToriiError::store_error)?;
        for (&id, info) in &table.columns {
            column_info_insert_query(
                INSERT_COLUMN_QUERY,
                owner,
                table.id,
                id,
                block_number,
                info,
                tx_hash,
            )
            .execute(&mut *transaction)
            .await
            .map_err(DojoToriiError::store_error)?;
        }

        transaction
            .commit()
            .await
            .map_err(DojoToriiError::store_error)
    }

    async fn read_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        let mut tables =
            PgTypeDef::get_rows::<DojoTableRow>(self.pool(), FETCH_TABLES_QUERY, owners)
                .await
                .map_err(DojoToriiError::store_error)?
                .into_iter()
                .map(|row: ((), DojoTable)| row.1)
                .collect_vec();
        let mut columns: HashMap<(Felt, Felt), _> =
            ColumnInfo::get_hash_map::<DojoColumnRow>(self.pool(), FETCH_COLUMNS_QUERY, owners)
                .await
                .map_err(DojoToriiError::store_error)?;
        for table in &mut tables {
            for key in table.key_fields.iter().chain(table.value_fields.iter()) {
                let column = columns
                    .remove(&(table.id, *key))
                    .ok_or_else(|| DojoToriiError::ColumnNotFound(table.name.clone(), *key))?;
                table.columns.insert(*key, column);
            }
        }
        Ok(tables)
    }
}
