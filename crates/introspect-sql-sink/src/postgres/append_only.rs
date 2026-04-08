use super::json::PostgresJsonSerializer;
use crate::postgres::insert::pg_json_felt252;
use crate::{RecordResult, Table};
use introspect_types::ColumnInfo;
use serde::ser::SerializeMap;
use serde_json::Serializer as JsonSerializer;
use starknet_types_core::felt::Felt;
use std::io::Write;
use torii_introspect::tables::SerializeEntries;
use torii_introspect::Record;
use torii_sql::postgres::PgQuery;
use torii_sql::Queries;

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

impl SerializeEntries for MetaData<'_> {
    fn entry_count(&self) -> usize {
        2
    }
    fn serialize_entries<S: serde::Serializer>(
        &self,
        map: &mut <S as serde::Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        let tx_hash = pg_json_felt252(self.transaction_hash);
        map.serialize_entry("__created_block", &self.block_number)?;
        map.serialize_entry("__created_tx", &tx_hash)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn append_only_record_queries(
    table: &Table,
    column_ids: &[Felt],
    records: &[Record],
    _from_address: &Felt,
    block_number: u64,
    transaction_hash: &Felt,
    queries: &mut Vec<PgQuery>,
) -> RecordResult<()> {
    let schema = table.get_record_schema(column_ids)?;
    let namespace = &table.namespace;
    let table_name = &table.name;
    let mut writer = Vec::new();
    let metadata = MetaData::new(block_number, transaction_hash);
    let primary = schema.primary_name();
    let columns: Vec<(&Felt, &ColumnInfo)> = table.columns.iter().collect();

    write!(
        writer,
        r#"WITH latest AS (SELECT DISTINCT ON ("{primary}") * FROM "{namespace}"."{table_name}" ORDER BY "{primary}", "__revision" DESC),
        input AS ( SELECT * FROM jsonb_populate_recordset(NULL::"{namespace}"."{table_name}", $$"#
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
        r#"$$)) INSERT INTO "{namespace}"."{table_name}" ("{primary}", "__revision", "__created_block", "__created_tx""#
    )
    .unwrap();

    for (_, ColumnInfo { name, .. }) in &columns {
        write!(writer, r#", "{name}""#).unwrap();
    }

    write!(
        writer,
        r#") SELECT i."{primary}", (SELECT COALESCE(MAX("__revision"), 0) + 1 FROM "{namespace}"."{table_name}" WHERE "{primary}" = i."{primary}") AS "__revision", i."__created_block", i."__created_tx""#,
    ).unwrap();

    for (id, ColumnInfo { name, .. }) in &columns {
        if column_ids.contains(id) {
            write!(writer, r#", i."{name}""#).unwrap();
        } else {
            write!(writer, r#", COALESCE(i."{name}", l."{name}")"#).unwrap();
        }
    }

    write!(
        writer,
        r#" FROM input i LEFT JOIN latest l USING ("{primary}")"#
    )
    .unwrap();

    let string = unsafe { String::from_utf8_unchecked(writer) };
    queries.add(string);
    Ok(())
}
