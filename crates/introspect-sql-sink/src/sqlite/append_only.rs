use crate::sqlite::record::SqliteDeserializer;
use crate::sqlite::table::qualified_table_name;
use crate::sqlite::types::{SqliteType, TypeDefSqliteExt};
use crate::{RecordResult, Table};
use introspect_types::bytes::IntoByteSource;
use introspect_types::schema::{Names, TypeDefs};
use introspect_types::ColumnInfo;
use itertools::Itertools;
use sqlx::Arguments;
use sqlx::Error::Encode as EncodeError;
use starknet_types_core::felt::Felt;
use std::fmt::Write as FmtWrite;
use std::sync::Arc;
use torii_introspect::Record;
use torii_sql::{Queries, SqliteArguments, SqliteQuery};

pub fn append_only_record_queries(
    table: &Table,
    column_ids: &[Felt],
    records: &[Record],
    _from_address: &Felt,
    _block_number: u64,
    _transaction_hash: &Felt,
    queries: &mut Vec<SqliteQuery>,
) -> RecordResult<()> {
    let table_name = qualified_table_name(&table.namespace, &table.name);
    let primary = &table.primary.name;
    let columns = table.columns.iter().collect_vec();
    let mut sql = format!(r#"INSERT INTO "{table_name}" ("{primary}", "__revision""#,);
    for name in columns.names() {
        write!(sql, r#", "{name}""#).unwrap();
    }
    write!(sql, r#") VALUES (?, (SELECT COALESCE(MAX("__revision"), 0) + 1 FROM "{table_name}" WHERE "{primary}" = ?1)"#).unwrap();
    for (id, ColumnInfo { name, type_def, .. }) in &columns {
        match column_ids.iter().position(|c| &c == id) {
           Some(index) => write!(sql, r#", {}"#, type_def.index_placeholder(index + 2)?).unwrap(),
            None =>  match type_def.try_into()? {
                SqliteType::Json => write!(
                    sql,
                    r#", (SELECT jsonb("{name}") FROM "{table_name}" WHERE "{primary}" = ?1 ORDER BY "__revision" DESC LIMIT 1)"#
                ).unwrap(),
                _ => write!(
                    sql,
                    r#", (SELECT "{name}" FROM "{table_name}" WHERE "{primary}" = ?1 ORDER BY "__revision" DESC LIMIT 1)"#
                ).unwrap(),
            }
        }
    }
    let schema = table.get_record_schema(column_ids)?;
    sql.push(')');
    let sql: Arc<str> = sql.into();
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
