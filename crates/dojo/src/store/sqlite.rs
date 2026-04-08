use super::DojoStoreTrait;
use crate::decoder::primary_field_def;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use async_trait::async_trait;
use introspect_types::ColumnInfo;
use serde::{Deserialize, Serialize};
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Arguments, FromRow, Sqlite, SqlitePool};
use starknet_types_core::felt::Felt;
use starknet_types_raw::{error::OverflowError, Felt as RawFelt};
use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use torii_introspect::schema::ColumnKeyTrait;
use torii_sql::sqlite::SqliteDbConnection;
use torii_sql::{PoolExt, SqlxResult};

pub const DOJO_SQLITE_STORE_MIGRATIONS: Migrator = sqlx::migrate!("./migrations/sqlite");

#[derive(Debug, thiserror::Error)]
pub enum DojoSqliteStoreError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    #[error("Column not found for table {name} with id {table_id} and column {column_id}")]
    ColumnNotFound {
        name: String,
        table_id: Felt,
        column_id: Felt,
    },
    #[error("Felt overflow error: {0}")]
    FeltOverflow(#[from] OverflowError),
}

impl DojoSqliteStoreError {
    pub fn column_not_found<K: ColumnKeyTrait>(name: String, key: &K) -> Self {
        let (table_id, column_id) = key.as_parts();
        Self::ColumnNotFound {
            name,
            table_id: *table_id,
            column_id: *column_id,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredColumnInfo {
    name: String,
    attributes: Vec<introspect_types::Attribute>,
    type_def: introspect_types::TypeDef,
}

#[derive(FromRow)]
struct TableRow {
    id: Vec<u8>,
    name: String,
    attributes: String,
    keys_json: String,
    values_json: String,
    legacy: i64,
}

#[derive(FromRow)]
struct ColumnRow {
    table_id: Vec<u8>,
    id: Vec<u8>,
    payload: String,
}

fn serialize_felt_json_array(values: &[Felt]) -> Result<String, serde_json::Error> {
    serde_json::to_string(
        &values
            .iter()
            .map(|felt| format!("{felt:#x}"))
            .collect::<Vec<_>>(),
    )
}

fn parse_felt_json_array(value: &str) -> Result<Vec<Felt>, DojoSqliteStoreError> {
    let values = serde_json::from_str::<Vec<String>>(value)?;
    values
        .into_iter()
        .map(|felt| {
            Felt::from_hex(&felt)
                .map_err(|err| serde_json::Error::io(io::Error::other(err.to_string())).into())
        })
        .collect()
}

fn felt_from_bytes(value: &[u8]) -> Result<Felt, DojoSqliteStoreError> {
    Ok(RawFelt::from_be_bytes_slice(value)?.into())
}

fn table_row_into_table(value: TableRow) -> Result<DojoTable, DojoSqliteStoreError> {
    Ok(DojoTable {
        id: felt_from_bytes(&value.id)?,
        name: value.name,
        attributes: serde_json::from_str(&value.attributes)?,
        primary: primary_field_def(),
        columns: HashMap::new(),
        key_fields: parse_felt_json_array(&value.keys_json)?,
        value_fields: parse_felt_json_array(&value.values_json)?,
        legacy: value.legacy != 0,
    })
}

fn column_row_into_entry<K>(value: ColumnRow) -> Result<(K, ColumnInfo), DojoSqliteStoreError>
where
    K: ColumnKeyTrait,
{
    let payload: StoredColumnInfo = serde_json::from_str(&value.payload)?;
    Ok((
        K::from_parts(
            felt_from_bytes(&value.table_id)?,
            felt_from_bytes(&value.id)?,
        ),
        ColumnInfo {
            name: payload.name,
            attributes: payload.attributes,
            type_def: payload.type_def,
        },
    ))
}

fn select_table_query(owners: &[Felt]) -> (String, SqliteArguments<'_>) {
    let mut query = String::from(
        "SELECT id, name, attributes, keys_json, values_json, legacy FROM dojo_tables",
    );
    let mut args = SqliteArguments::default();
    if !owners.is_empty() {
        query.push_str(" WHERE owner IN (");
        for (index, owner) in owners.iter().enumerate() {
            if index > 0 {
                query.push_str(", ");
            }
            query.push('?');
            let _ = args.add(owner.to_bytes_be().to_vec());
        }
        query.push(')');
    }
    (query, args)
}

fn select_column_query(owners: &[Felt]) -> (String, SqliteArguments<'_>) {
    let mut query = String::from("SELECT table_id, id, payload FROM dojo_columns");
    let mut args = SqliteArguments::default();
    if !owners.is_empty() {
        query.push_str(" WHERE owner IN (");
        for (index, owner) in owners.iter().enumerate() {
            if index > 0 {
                query.push_str(", ");
            }
            query.push('?');
            let _ = args.add(owner.to_bytes_be().to_vec());
        }
        query.push(')');
    }
    (query, args)
}

pub struct SqliteStore<T>(pub T);

impl<T> Deref for SqliteStore<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: SqliteDbConnection + Send + Sync> SqliteStore<T> {
    pub async fn initialize(&self) -> SqlxResult<()> {
        self.migrate(Some("dojo"), DOJO_SQLITE_STORE_MIGRATIONS)
            .await
    }
}

impl<T: SqliteDbConnection> From<T> for SqliteStore<T> {
    fn from(pool: T) -> Self {
        Self(pool)
    }
}

#[async_trait]
impl DojoStoreTrait for SqlitePool {
    async fn initialize(&self) -> DojoToriiResult {
        self.migrate(Some("dojo"), DOJO_SQLITE_STORE_MIGRATIONS)
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

        sqlx::query(
            r"
            INSERT INTO dojo_tables (
                owner,
                id,
                name,
                attributes,
                keys_json,
                values_json,
                legacy,
                created_block,
                updated_block,
                created_tx,
                updated_tx
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner, id) DO UPDATE SET
                name = excluded.name,
                attributes = excluded.attributes,
                keys_json = excluded.keys_json,
                values_json = excluded.values_json,
                legacy = excluded.legacy,
                updated_at = unixepoch(),
                updated_block = excluded.updated_block,
                updated_tx = excluded.updated_tx
            ",
        )
        .bind(owner.to_bytes_be().to_vec())
        .bind(table.id.to_bytes_be().to_vec())
        .bind(&table.name)
        .bind(serde_json::to_string(&table.attributes)?)
        .bind(serialize_felt_json_array(&table.key_fields)?)
        .bind(serialize_felt_json_array(&table.value_fields)?)
        .bind(table.legacy)
        .bind(block_number as i64)
        .bind(block_number as i64)
        .bind(tx_hash.to_bytes_be().to_vec())
        .bind(tx_hash.to_bytes_be().to_vec())
        .execute(&mut *transaction)
        .await
        .map_err(DojoToriiError::store_error)?;

        for (id, info) in &table.columns {
            let payload = StoredColumnInfo {
                name: info.name.clone(),
                attributes: info.attributes.clone(),
                type_def: info.type_def.clone(),
            };
            sqlx::query(
                r"
                INSERT INTO dojo_columns (owner, table_id, id, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(owner, table_id, id) DO UPDATE SET
                    payload = excluded.payload
                ",
            )
            .bind(owner.to_bytes_be().to_vec())
            .bind(table.id.to_bytes_be().to_vec())
            .bind(id.to_bytes_be().to_vec())
            .bind(serde_json::to_string(&payload)?)
            .execute(&mut *transaction)
            .await
            .map_err(DojoToriiError::store_error)?;
        }

        transaction
            .commit()
            .await
            .map_err(DojoToriiError::store_error)?;
        Ok(())
    }

    async fn read_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        let (table_query, table_args) = select_table_query(owners);
        let rows = sqlx::query_as_with::<Sqlite, TableRow, _>(&table_query, table_args)
            .fetch_all(self.pool())
            .await
            .map_err(DojoToriiError::store_error)?;
        let mut tables = rows
            .into_iter()
            .map(table_row_into_table)
            .collect::<Result<Vec<_>, _>>()
            .map_err(DojoToriiError::store_error)?;

        let (column_query, column_args) = select_column_query(owners);
        let mut columns: HashMap<(Felt, Felt), ColumnInfo> =
            sqlx::query_as_with::<Sqlite, ColumnRow, _>(&column_query, column_args)
                .fetch_all(self.pool())
                .await
                .map_err(DojoToriiError::store_error)?
                .into_iter()
                .map(column_row_into_entry::<(Felt, Felt)>)
                .collect::<Result<HashMap<_, _>, _>>()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn felt_json_roundtrip() {
        let values = vec![Felt::ONE, Felt::TWO];
        let encoded = serialize_felt_json_array(&values).unwrap();
        let decoded = parse_felt_json_array(&encoded).unwrap();
        assert_eq!(decoded, values);
    }
}
