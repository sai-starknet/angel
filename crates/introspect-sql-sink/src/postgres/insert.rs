use super::json::PostgresJsonSerializer;
use crate::{RecordResult, Table};
use introspect_types::ColumnInfo;
use serde::ser::SerializeMap;
use serde_json::Serializer as JsonSerializer;
use starknet_types_raw::Felt;
use std::io::Write;
use torii_introspect::tables::SerializeEntries;
use torii_introspect::Record;
use torii_sql::postgres::PgQuery;
use torii_sql::Queries;

pub const METADATA_CONFLICTS: &str = "__updated_at = NOW(), __updated_block = EXCLUDED.__updated_block, __updated_tx = EXCLUDED.__updated_tx";

struct MetaData<'a> {
    pub block_number: u64,
    pub transaction_hash: &'a Felt,
}

impl<'a> MetaData<'a> {
    pub fn new(block_number: u64, transaction_hash: &'a Felt) -> Self {
        Self {
            block_number,
            transaction_hash,
        }
    }
}

pub fn pg_json_felt252(value: &Felt) -> String {
    format!("\\x{}", hex::encode(value.as_be_bytes()))
}

impl SerializeEntries for MetaData<'_> {
    fn entry_count(&self) -> usize {
        4
    }
    fn serialize_entries<S: serde::Serializer>(
        &self,
        map: &mut <S as serde::Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        let tx_hash = pg_json_felt252(self.transaction_hash);
        map.serialize_entry("__created_block", &self.block_number)?;
        map.serialize_entry("__updated_block", &self.block_number)?;
        map.serialize_entry("__created_tx", &tx_hash)?;
        map.serialize_entry("__updated_tx", &tx_hash)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn insert_record_queries(
    table: &Table,
    columns: &[Felt],
    records: &[Record],
    _from_address: &Felt,
    block_number: u64,
    transaction_hash: &Felt,
    queries: &mut Vec<PgQuery>,
) -> RecordResult<()> {
    let schema = table.get_record_schema(columns)?;
    let namespace = &table.namespace;
    let table_name = &table.name;
    let mut writer = Vec::new();
    let metadata = MetaData::new(block_number, transaction_hash);
    write!(
            writer,
            r#"INSERT INTO "{namespace}"."{table_name}" SELECT * FROM jsonb_populate_recordset(NULL::"{namespace}"."{table_name}", $$"#
        )
        .unwrap();
    schema.parse_records_with_metadata(
        records,
        &metadata,
        &mut JsonSerializer::new(&mut writer),
        &PostgresJsonSerializer,
    )?;
    write!(
        writer,
        r#"$$) ON CONFLICT ("{}") DO UPDATE SET {METADATA_CONFLICTS}"#,
        schema.primary().name
    )
    .unwrap();
    for ColumnInfo { name, .. } in schema.columns() {
        write!(
            writer,
            r#", "{name}" = COALESCE(EXCLUDED."{name}", "{table_name}"."{name}")"#,
            name = name
        )
        .unwrap();
    }
    let string = unsafe { String::from_utf8_unchecked(writer) };
    queries.add(string);
    Ok(())
}
