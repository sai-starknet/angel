use crate::sqlite::json::SqliteJsonSerializer;
use crate::sqlite::table::qualified_table_name;
use crate::sqlite::types::{SqliteColumn, SqliteType};
use crate::{RecordResult, Table, TypeResult};
use introspect_types::bytes::IntoByteSource;
use introspect_types::schema::{Names, TypeDefs};
use introspect_types::serialize::CairoSeFrom;
use introspect_types::{CairoDeserializer, DecodeError, EthAddress, ResultInto, TypeDef};
use itertools::Itertools;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::sqlite::SqliteArgumentValue;
use sqlx::Error::Encode as EncodeError;
use sqlx::{Arguments, Encode, Sqlite, Type};
use starknet_types_core::felt::Felt;
use std::sync::Arc;
use torii_introspect::Record;
use torii_sql::{Queries, SqliteArguments, SqliteQuery};

pub fn coalesce_sql<'a>(table_name: &str, column: &SqliteColumn<'a>) -> String {
    let column_name = column.name;
    match column.sql_type {
        SqliteType::Json => {
            format!(
                r#""{column_name}" = COALESCE(jsonb(excluded."{column_name}"), "{table_name}"."{column_name}")"#
            )
        }
        _ => format!(
            r#""{column_name}" = COALESCE(excluded."{column_name}", "{table_name}"."{column_name}")"#
        ),
    }
}

pub enum SqliteValue {
    Null,
    Integer(i64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqliteValue {
    fn integer(n: impl Into<i64>) -> Self {
        SqliteValue::Integer(n.into())
    }
    fn text(s: impl ToString) -> Self {
        SqliteValue::Text(s.to_string())
    }
}

impl From<String> for SqliteValue {
    fn from(value: String) -> Self {
        SqliteValue::Text(value)
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(value: Vec<u8>) -> Self {
        SqliteValue::Blob(value)
    }
}

impl<const N: usize> From<[u8; N]> for SqliteValue {
    fn from(value: [u8; N]) -> Self {
        SqliteValue::Blob(value.to_vec())
    }
}

impl From<Felt> for SqliteValue {
    fn from(value: Felt) -> Self {
        SqliteValue::Text(format!("{value:#064x}"))
    }
}

impl From<EthAddress> for SqliteValue {
    fn from(value: EthAddress) -> Self {
        SqliteValue::Text(format!("0x{}", hex::encode(value.0)))
    }
}

impl From<bool> for SqliteValue {
    fn from(value: bool) -> Self {
        SqliteValue::Integer(if value { 1 } else { 0 })
    }
}

pub trait SqliteDeserializer {
    fn deserialize_column(&self, data: &mut impl CairoDeserializer) -> RecordResult<SqliteValue>;
    fn deserialize_json(&self, data: &mut impl CairoDeserializer) -> RecordResult<SqliteValue>;
}

impl SqliteDeserializer for TypeDef {
    fn deserialize_column(&self, data: &mut impl CairoDeserializer) -> RecordResult<SqliteValue> {
        match self {
            TypeDef::None => Ok(SqliteValue::Null),
            TypeDef::Felt252
            | TypeDef::ClassHash
            | TypeDef::ContractAddress
            | TypeDef::StorageAddress
            | TypeDef::StorageBaseAddress => data.next_felt().result_into(),
            TypeDef::ShortUtf8 => data.next_short_string().result_into(),
            TypeDef::Bytes31 | TypeDef::Bytes31Encoded(_) => data.next_bytes::<31>().result_into(),
            TypeDef::Bool => data.next_bool().result_into(),
            TypeDef::U8 => data.next_u8().map(SqliteValue::integer).err_into(),
            TypeDef::U16 => data.next_u16().map(SqliteValue::integer).err_into(),
            TypeDef::U32 => data.next_u32().map(SqliteValue::integer).err_into(),
            TypeDef::U64 => data.next_u64().map(SqliteValue::text).err_into(),
            TypeDef::U128 => data.next_u128().map(SqliteValue::text).err_into(),
            TypeDef::U256 => data.next_u256().map(SqliteValue::text).err_into(),
            TypeDef::U512 => data.next_u512().map(SqliteValue::text).err_into(),
            TypeDef::I8 => data.next_i8().map(SqliteValue::integer).err_into(),
            TypeDef::I16 => data.next_i16().map(SqliteValue::integer).err_into(),
            TypeDef::I32 => data.next_i32().map(SqliteValue::integer).err_into(),
            TypeDef::I64 => data.next_i64().map(SqliteValue::integer).err_into(),
            TypeDef::I128 => data.next_i128().map(SqliteValue::text).err_into(),
            TypeDef::EthAddress => data.next_eth_address().result_into(),
            TypeDef::Utf8String => data.next_string().result_into(),
            TypeDef::ByteArray | TypeDef::ByteArrayEncoded(_) | TypeDef::Custom(_) => {
                data.next_byte_array_bytes().result_into()
            }
            TypeDef::Tuple(_)
            | TypeDef::Array(_)
            | TypeDef::FixedArray(_)
            | TypeDef::Felt252Dict(_)
            | TypeDef::Struct(_)
            | TypeDef::Enum(_)
            | TypeDef::Option(_)
            | TypeDef::Result(_)
            | TypeDef::Nullable(_) => self.deserialize_json(data),
            TypeDef::Ref(_) => Err(DecodeError::message(
                "TypeDef Ref needs to be expanded before transoding",
            ))
            .err_into(),
        }
    }
    fn deserialize_json(&self, data: &mut impl CairoDeserializer) -> RecordResult<SqliteValue> {
        let se = CairoSeFrom::new(self, data, &SqliteJsonSerializer);
        serde_json::to_string(&se).result_into()
    }
}

impl From<SqliteValue> for SqliteArgumentValue<'_> {
    fn from(value: SqliteValue) -> Self {
        match value {
            SqliteValue::Null => SqliteArgumentValue::Null,
            SqliteValue::Integer(n) => SqliteArgumentValue::Int64(n),
            SqliteValue::Text(s) => SqliteArgumentValue::Text(s.into()),
            SqliteValue::Blob(b) => SqliteArgumentValue::Blob(b.into()),
        }
    }
}

impl Type<Sqlite> for SqliteValue {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        // SqliteValue is dynamically typed; report as Text since SQLite is flexible with types.
        <String as Type<Sqlite>>::type_info()
    }

    fn compatible(ty: &<Sqlite as sqlx::Database>::TypeInfo) -> bool {
        <i64 as Type<Sqlite>>::compatible(ty)
            || <String as Type<Sqlite>>::compatible(ty)
            || <Vec<u8> as Type<Sqlite>>::compatible(ty)
    }
}

impl<'q> Encode<'q, Sqlite> for SqliteValue {
    fn encode_by_ref(&self, buf: &mut Vec<SqliteArgumentValue<'q>>) -> Result<IsNull, BoxDynError> {
        match self {
            SqliteValue::Null => Ok(IsNull::Yes),
            SqliteValue::Integer(n) => <i64 as Encode<Sqlite>>::encode_by_ref(n, buf),
            SqliteValue::Text(s) => <String as Encode<Sqlite>>::encode_by_ref(s, buf),
            SqliteValue::Blob(b) => <Vec<u8> as Encode<Sqlite>>::encode_by_ref(b, buf),
        }
    }
}

pub fn insert_record_queries(
    table: &Table,
    columns: &[Felt],
    records: &[Record],
    _from_address: &Felt,
    _block_number: u64,
    _transaction_hash: &Felt,
    queries: &mut Vec<SqliteQuery>,
) -> RecordResult<()> {
    let schema = table.get_record_schema(columns)?;
    let table_name = qualified_table_name(&table.namespace, &table.name);
    let all_columns = schema.all_columns();
    let sql_columns = all_columns
        .iter()
        .map(|c| (*c).try_into())
        .collect::<TypeResult<Vec<SqliteColumn>>>()?;
    let column_names = all_columns.names();
    let placeholders = sql_columns.iter().map(SqliteColumn::placeholder).join(", ");
    let coalesce = sql_columns[1..]
        .iter()
        .map(|col| coalesce_sql(&table_name, col))
        .join(", ");
    let sql: Arc<str> = format!(
        r#"INSERT INTO "{table_name}" ({}) VALUES ({}) ON CONFLICT("{}") DO UPDATE SET {}"#,
        column_names
            .iter()
            .map(|name| format!(r#""{name}""#))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders,
        schema.primary_name(),
        coalesce
    )
    .into();

    for record in records {
        let mut arguments: SqliteArguments<'static> = SqliteArguments::default();
        let mut primary_data = record.id.as_slice().into_source();
        let mut data = record.values.as_slice().into_source();
        arguments
            .add(
                schema
                    .primary_type_def()
                    .deserialize_column(&mut primary_data)?,
            )
            .map_err(EncodeError)?;
        for type_def in schema.columns().type_defs() {
            arguments
                .add(type_def.deserialize_column(&mut data)?)
                .map_err(EncodeError)?;
        }
        queries.add((sql.clone(), arguments));
    }
    Ok(())
}
