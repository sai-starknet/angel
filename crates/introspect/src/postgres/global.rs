use std::collections::HashMap;

use async_trait::async_trait;
use introspect_types::{Attribute, ColumnDef, ColumnInfo, TypeDef};
use itertools::Itertools;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};
use starknet_types_raw::Felt;
use torii_sql::SqlxResult;

use crate::postgres::{attribute_type, felt252_type, string_type, PgAttribute, PgFelt};

#[derive(FromRow)]
pub struct ColumnRow {
    table: PgFelt,
    id: PgFelt,
    name: String,
    attributes: Vec<PgAttribute>,
    type_def: Json<TypeDef>,
}

#[derive(PartialEq, Eq, Hash)]
pub struct ColumnKey {
    pub table: Felt,
    pub id: Felt,
}

impl From<ColumnRow> for (ColumnKey, ColumnInfo) {
    fn from(value: ColumnRow) -> Self {
        (
            ColumnKey {
                table: value.table.into(),
                id: value.id.into(),
            },
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

#[async_trait]
pub trait PgTypeDef<Key> {
    type Row;
    fn insert_query(&self, key: &Key) -> String;
    async fn get_rows(pool: &PgPool, pg_table: &str) -> SqlxResult<Vec<(Key, Self)>>
    where
        Self: Sized;
    async fn get_hash_map(pool: &PgPool, pg_table: &str) -> SqlxResult<HashMap<Key, Self>>
    where
        Self: Sized + std::hash::Hash + Eq,
        Key: std::hash::Hash + Eq,
    {
        Self::get_rows(pool, pg_table)
            .await
            .map(|rows| rows.into_iter().collect())
    }
}

#[async_trait]
impl PgTypeDef<ColumnKey> for ColumnInfo {
    type Row = ColumnRow;
    fn insert_query(&self, key: &ColumnKey) -> String {
        column_insert_query(
            &key.table,
            &key.id,
            &self.name,
            &self.attributes,
            &self.type_def,
        )
    }

    async fn get_rows(pool: &PgPool, pg_table: &str) -> SqlxResult<Vec<(ColumnKey, Self)>>
    where
        Self: Sized,
    {
        get_column_rows(pool, pg_table)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

#[async_trait]
impl PgTypeDef<Felt> for ColumnDef {
    type Row = ColumnRow;
    fn insert_query(&self, key: &Felt) -> String {
        column_insert_query(key, &self.id, &self.name, &self.attributes, &self.type_def)
    }
    async fn get_rows(pool: &PgPool, pg_table: &str) -> SqlxResult<Vec<(Felt, Self)>> {
        get_column_rows(pool, pg_table)
            .await
            .map(|rows| rows.into_iter().map_into().collect_vec())
    }
}

fn column_insert_query(
    table: &Felt,
    id: &Felt,
    name: &str,
    attributes: &[Attribute],
    type_def: &TypeDef,
) -> String {
    format!(
        r#"
            INSERT INTO dojo.columns ("table", id, name, attributes, type_def)
                VALUES ({table}, {id}, {name}, ARRAY[{attributes}]::introspect.attribute[], {type_def}::jsonb)
                ON CONFLICT ("table", id) DO UPDATE SET
                name = EXCLUDED.name,
                attributes = EXCLUDED.attributes,
                type_def = EXCLUDED.type_def
            "#,
        table = felt252_type(table),
        id = felt252_type(id),
        name = string_type(name),
        attributes = attributes.iter().map(attribute_type).join(","),
        type_def = string_type(&serde_json::to_string(&type_def).unwrap()),
    )
}

async fn get_column_rows(pool: &PgPool, pg_table: &str) -> SqlxResult<Vec<ColumnRow>> {
    sqlx::query_as(&get_rows_query(pg_table))
        .fetch_all(pool)
        .await
}

fn get_rows_query(table: &str) -> String {
    format!(
        r#"
            SELECT "table", id, name, attributes, type_def
            FROM {table}
        "#,
    )
}
