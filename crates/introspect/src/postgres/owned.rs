use crate::postgres::types::PgPrimary;
use crate::postgres::{PgAttribute, PgFelt};
use crate::schema::{ColumnKeyTrait, Table};
use async_trait::async_trait;
use introspect_types::{Attribute, ColumnDef, ColumnInfo, PrimaryDef, TypeDef};
use itertools::Itertools;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool, Postgres};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii_sql::SqlxResult;

pub const TABLE_INSERT_QUERY: &str = "
    INSERT INTO introspect.tables (owner, id, name, attributes, primary_def, column_ids, updated_at, created_block, updated_block, created_tx, updated_tx)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $7, $8, $8)
        ON CONFLICT (owner, id) DO UPDATE SET
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes,
        primary_def = EXCLUDED.primary_def,
        column_ids = EXCLUDED.column_ids,
        updated_at = NOW(),
        updated_block = EXCLUDED.updated_block,
        updated_tx = EXCLUDED.updated_tx
";

pub const COLUMN_INSERT_QUERY: &str = r#"
    INSERT INTO introspect.columns (owner, "table", id, name, attributes, type_def)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (owner, "table", id) DO UPDATE SET
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes,
        type_def = EXCLUDED.type_def
"#;

pub const FETCH_TABLES_QUERY: &str = "
    SELECT
        id::felt252 AS id,
        name,
        attributes,
        primary_def,
        column_ids::felt252[] AS column_ids,
        alive
    FROM introspect.tables
    WHERE $1::felt252[] = '{}' OR owner = ANY($1)
";

pub const FETCH_COLUMNS_QUERY: &str = r#"
    SELECT
        "table"::felt252 AS "table",
        id::felt252 AS id,
        name,
        attributes,
        type_def
    FROM introspect.columns
    WHERE $1::felt252[] = '{}' OR owner = ANY($1)
"#;

#[derive(FromRow)]
pub struct ColumnRow {
    table: PgFelt,
    id: PgFelt,
    name: String,
    attributes: Vec<PgAttribute>,
    type_def: Json<TypeDef>,
}

#[derive(FromRow)]
pub struct TableRow {
    id: PgFelt,
    name: String,
    attributes: Vec<PgAttribute>,
    primary_def: PgPrimary,
    column_ids: Vec<PgFelt>,
    alive: bool,
}

impl<K> From<ColumnRow> for (K, ColumnInfo)
where
    K: ColumnKeyTrait,
{
    fn from(value: ColumnRow) -> Self {
        (
            K::from_parts(value.table.into(), value.id.into()),
            ColumnInfo {
                name: value.name,
                attributes: value.attributes.into_iter().map(Into::into).collect(),
                type_def: value.type_def.0,
            },
        )
    }
}

impl From<ColumnRow> for (Felt, ColumnDef) {
    fn from(value: ColumnRow) -> Self {
        (
            value.table.into(),
            ColumnDef {
                id: value.id.into(),
                name: value.name,
                attributes: value.attributes.into_iter().map(Into::into).collect(),
                type_def: value.type_def.0,
            },
        )
    }
}

impl From<TableRow> for (Felt, Table) {
    fn from(value: TableRow) -> Self {
        (
            value.id.into(),
            Table {
                name: value.name,
                attributes: value.attributes.into_iter().map_into().collect(),
                primary: value.primary_def.into(),
                columns: HashMap::with_capacity(value.column_ids.len()),
                order: value.column_ids.into_iter().map_into().collect(),
                alive: value.alive,
            },
        )
    }
}

#[async_trait]
pub trait PgTypeRow
where
    Self: Sized,
{
    async fn get_pg_rows(
        pool: &PgPool,
        query: &'static str,
        owners: &[Felt],
    ) -> SqlxResult<Vec<Self>>;
}

#[async_trait]
impl<T> PgTypeRow for T
where
    Self: for<'r> FromRow<'r, PgRow> + Send + Unpin + Sized,
{
    async fn get_pg_rows(
        pool: &PgPool,
        query: &'static str,
        owners: &[Felt],
    ) -> SqlxResult<Vec<Self>> {
        sqlx::query_as(query)
            .bind(owners.iter().copied().map(PgFelt::from).collect_vec())
            .fetch_all(pool)
            .await
    }
}

#[async_trait]
pub trait PgTypeDef<Key> {
    async fn get_rows<Row>(
        pool: &PgPool,
        query: &'static str,
        owners: &[Felt],
    ) -> SqlxResult<Vec<(Key, Self)>>
    where
        Self: Sized,
        Row: PgTypeRow + Into<(Key, Self)>,
    {
        let pg_rows: Vec<Row> = <Row as PgTypeRow>::get_pg_rows(pool, query, owners).await?;
        Ok(pg_rows.into_iter().map_into().collect_vec())
    }
    async fn get_hash_map<Row>(
        pool: &PgPool,
        query: &'static str,
        owners: &[Felt],
    ) -> SqlxResult<HashMap<Key, Self>>
    where
        Self: Sized,
        Key: std::hash::Hash + Eq,
        Row: PgTypeRow + Into<(Key, Self)>,
    {
        Self::get_rows::<Row>(pool, query, owners)
            .await
            .map(|rows| rows.into_iter().collect())
    }
}

#[async_trait]
impl<Key, T> PgTypeDef<Key> for T {}

pub trait PgColumnTrait {
    fn insert_query(
        &self,
        query: &'static str,
        owner: &Felt,
        table: &Felt,
    ) -> Query<'static, Postgres, PgArguments>;
}

pub fn column_info_insert_query(
    query: &'static str,
    owner: &Felt,
    table: &Felt,
    id: &Felt,
    info: &ColumnInfo,
) -> Query<'static, Postgres, PgArguments> {
    column_insert_query(
        query,
        owner,
        table,
        id,
        &info.name,
        &info.attributes,
        &info.type_def,
    )
}

pub fn column_insert_query(
    query: &'static str,
    owner: &Felt,
    table: &Felt,
    id: &Felt,
    name: &str,
    attributes: &[Attribute],
    type_def: &TypeDef,
) -> Query<'static, Postgres, PgArguments> {
    sqlx::query::<Postgres>(query)
        .bind(PgFelt::from(*owner))
        .bind(PgFelt::from(*table))
        .bind(PgFelt::from(*id))
        .bind(name.to_owned())
        .bind(attributes.iter().map_into::<PgAttribute>().collect_vec())
        .bind(Json(type_def.clone()))
}

pub fn table_insert_query(
    query: &'static str,
    owner: &Felt,
    id: &Felt,
    name: &str,
    attributes: &[Attribute],
    primary: &PrimaryDef,
    columns: &[Felt],
    block_number: u64,
    tx_hash: &Felt,
) -> Query<'static, Postgres, PgArguments> {
    sqlx::query::<Postgres>(query)
        .bind(PgFelt::from(*owner))
        .bind(PgFelt::from(*id))
        .bind(name.to_owned())
        .bind(attributes.iter().map_into::<PgAttribute>().collect_vec())
        .bind(PgPrimary::from(primary.clone()))
        .bind(columns.iter().copied().map(PgFelt::from).collect_vec())
        .bind(block_number.to_string())
        .bind(PgFelt::from(*tx_hash))
}
