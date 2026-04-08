use super::insert::insert_record_queries;
use super::query::{insert_columns_query, insert_table_query, CreatePgTable};
use super::upgrade::PgTableUpgrade;
use crate::postgres::append_only::append_only_record_queries;
use crate::processor::IntrospectDb;
use crate::{
    IntrospectQueryMaker, IntrospectSqlSink, RecordResult, Table, TableError, TableResult,
};
use async_trait::async_trait;
use introspect_types::schema::{Names, TypeDefs};
use introspect_types::{ColumnDef, FeltIds, PrimaryDef};
use starknet_types_core::felt::Felt;
use torii_introspect::Record;
use torii_sql::{PgPool, PgQuery, Postgres};

pub type IntrospectPgDb = IntrospectDb<PgPool>;

#[async_trait]
impl IntrospectQueryMaker for Postgres {
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
        queries: &mut Vec<PgQuery>,
    ) -> TableResult<()> {
        let ns = namespace.into();
        CreatePgTable::new(&ns, id, name, primary, columns, append_only)?.make_queries(queries);
        store_table_queries(
            namespace,
            id,
            name,
            primary,
            append_only,
            columns,
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
        queries: &mut Vec<PgQuery>,
    ) -> TableResult<()> {
        let upgrades = table.upgrade_table(name, primary, columns)?;
        upgrades.to_queries(block_number, transaction_hash, queries)?;
        let columns: Vec<(&Felt, &introspect_types::ColumnInfo)> = table
            .columns_with_ids(&upgrades.columns_upgraded)
            .map_err(|e| e.to_table_error(&table.name))?;
        store_table_queries(
            &table.namespace,
            &table.id,
            name,
            primary,
            table.append_only,
            columns,
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
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<PgQuery>,
    ) -> RecordResult<()> {
        if table.append_only {
            append_only_record_queries(
                table,
                columns,
                records,
                from_address,
                block_number,
                transaction_hash,
                queries,
            )
        } else {
            insert_record_queries(
                table,
                columns,
                records,
                from_address,
                block_number,
                transaction_hash,
                queries,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn store_table_queries<CS>(
    schema: &str,
    id: &Felt,
    name: &str,
    primary: &PrimaryDef,
    append_only: bool,
    columns: CS,
    from_address: &Felt,
    block_number: u64,
    transaction_hash: &Felt,
    queries: &mut Vec<PgQuery>,
) -> TableResult<()>
where
    CS: Names + FeltIds + TypeDefs,
{
    queries.push(
        insert_table_query(
            schema,
            id,
            name,
            primary,
            from_address,
            append_only,
            block_number,
            transaction_hash,
        )
        .map_err(TableError::Encode)?,
    );

    queries.push(
        insert_columns_query(schema, id, columns, block_number, transaction_hash)
            .map_err(TableError::Encode)?,
    );
    Ok(())
}

impl IntrospectSqlSink for PgPool {
    const NAME: &'static str = "Introspect Postgres";
}
