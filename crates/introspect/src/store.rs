use crate::schema::Table;
use async_trait::async_trait;
use introspect_types::{Attribute, ColumnDef, PrimaryTypeDef, TypeDef};
use starknet_types_raw::Felt;

#[async_trait]
pub trait TableStore {
    type Error;
    async fn save_table(&self, owner: &Felt, id: &Felt, table: &Table) -> Result<(), Self::Error>;
    async fn load_tables(&self, owners: &[Felt]) -> Result<Table, Self::Error>;
    async fn add_columns(
        &self,
        owner: &Felt,
        table: &Felt,
        columns: ColumnDef,
        order: &[Felt],
    ) -> Result<(), Self::Error>;
    async fn update_table_name(
        &self,
        owner: &Felt,
        id: &Felt,
        name: &str,
    ) -> Result<(), Self::Error>;
    async fn update_primary_name(
        &self,
        owner: &Felt,
        id: &Felt,
        name: &str,
    ) -> Result<(), Self::Error>;
    async fn update_primary_type(
        &self,
        owner: &Felt,
        id: &Felt,
        attributes: &[Attribute],
        primary: &PrimaryTypeDef,
    ) -> Result<(), Self::Error>;
    async fn update_column_name(
        &self,
        owner: &Felt,
        table: &Felt,
        column: &Felt,
        name: &str,
    ) -> Result<(), Self::Error>;
    async fn update_column_type(
        &self,
        owner: &Felt,
        table: &Felt,
        column: &Felt,
        attributes: &[Attribute],
        type_def: &TypeDef,
    ) -> Result<(), Self::Error>;
}

pub struct TableStoreRO<T: TableStore>(pub T);

#[async_trait]
impl<T: TableStore + Send + Sync> TableStore for TableStoreRO<T> {
    type Error = T::Error;
    async fn save_table(
        &self,
        _owner: &Felt,
        _id: &Felt,
        _table: &Table,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn load_tables(&self, owners: &[Felt]) -> Result<Table, Self::Error> {
        self.0.load_tables(owners).await
    }
    async fn add_columns(
        &self,
        _owner: &Felt,
        _table: &Felt,
        _columns: ColumnDef,
        _order: &[Felt],
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn update_table_name(
        &self,
        _owner: &Felt,
        _id: &Felt,
        _name: &str,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn update_primary_name(
        &self,
        _owner: &Felt,
        _id: &Felt,
        _name: &str,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn update_primary_type(
        &self,
        _owner: &Felt,
        _id: &Felt,
        _attributes: &[Attribute],
        _primary: &PrimaryTypeDef,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn update_column_name(
        &self,
        _owner: &Felt,
        _table: &Felt,
        _column: &Felt,
        _name: &str,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn update_column_type(
        &self,
        _owner: &Felt,
        _table: &Felt,
        _column: &Felt,
        _attributes: &[Attribute],
        _type_def: &TypeDef,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
