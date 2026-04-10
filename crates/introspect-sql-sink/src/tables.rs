use crate::backend::IntrospectQueryMaker;
use crate::error::RecordResultExt;
use crate::namespace::{NamespaceKey, TableKey};
use crate::table::Table;
use crate::{DbError, DbResult};
use introspect_types::{ColumnDef, PrimaryDef, ResultInto};
use starknet_types_raw::Felt;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::RwLock;
use torii_introspect::InsertsFields;
use torii_sql::FlexQuery;

#[derive(Debug, Default)]
pub struct Tables(pub RwLock<HashMap<TableKey, Table>>);

impl Deref for Tables {
    type Target = RwLock<HashMap<TableKey, Table>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::too_many_arguments)]
impl Tables {
    pub fn create_table<DB: IntrospectQueryMaker>(
        &self,
        namespace_key: NamespaceKey,
        id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        append_only: bool,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()> {
        let namespace = namespace_key.to_string();

        let key = TableKey::new(namespace_key, *id);
        self.assert_table_not_exists(&key, name)?;
        DB::create_table_queries(
            &namespace,
            id,
            name,
            primary,
            columns,
            append_only,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )?;
        let mut tables = self.write()?;
        tables.insert(
            key,
            Table::new(
                namespace,
                *id,
                *from_address,
                name.to_string(),
                primary.clone(),
                columns,
                None,
                append_only,
            ),
        );
        Ok(())
    }

    pub fn update_table<DB: IntrospectQueryMaker>(
        &self,
        namespace_key: NamespaceKey,
        id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()> {
        let mut tables = self.write()?;
        let key = TableKey::new(namespace_key, *id);
        let table = tables
            .get_mut(&key)
            .ok_or_else(|| DbError::TableNotFound(key.clone()))?;
        DB::update_table_queries(
            table,
            name,
            primary,
            columns,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
        .err_into()
    }

    pub fn assert_table_not_exists(&self, id: &TableKey, name: &str) -> DbResult<()> {
        match self.read()?.get(id) {
            Some(existing) => Err(DbError::TableAlreadyExists(
                id.clone(),
                name.to_string(),
                existing.name.to_string(),
            )),
            None => Ok(()),
        }
    }

    pub fn set_table_dead(&self, namespace: NamespaceKey, id: Felt) -> DbResult<()> {
        let mut tables = self.write()?;
        let key = TableKey::new(namespace, id);
        match tables.get_mut(&key) {
            Some(table) => {
                table.alive = false;
                Ok(())
            }
            None => Err(DbError::TableNotFound(key)),
        }
    }

    pub fn insert_fields<DB: IntrospectQueryMaker>(
        &self,
        namespace: NamespaceKey,
        event: &InsertsFields,
        from_address: &Felt,
        block_number: u64,
        transaction_hash: &Felt,
        queries: &mut Vec<FlexQuery<DB>>,
    ) -> DbResult<()> {
        let tables = self.read().unwrap();
        let key = TableKey::new(namespace, event.table);
        let table = match tables.get(&key) {
            Some(table) => Ok(table),
            None => Err(DbError::TableNotFound(key)),
        }?;
        if !table.alive {
            return Ok(());
        }
        DB::insert_record_queries(
            table,
            &event.columns,
            &event.records,
            from_address,
            block_number,
            transaction_hash,
            queries,
        )
        .to_db_result(&table.name)
    }
}
