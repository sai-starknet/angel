use crate::error::{CollectColumnResults, ColumnNotFoundError, ColumnsNotFoundError};
use introspect_types::{ColumnDef, ColumnInfo, MemberDef, PrimaryDef, TypeDef};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::rc::Rc;
use torii_introspect::tables::RecordSchema;

#[derive(Debug)]
pub struct Table {
    pub id: Felt,
    pub namespace: String,
    pub name: String,
    pub owner: Felt,
    pub primary: ColumnInfo,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub dead: HashMap<u128, DeadField>,
    pub append_only: bool,
    pub alive: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeadField {
    pub name: String,
    pub type_def: TypeDef,
}

#[derive(Debug)]
pub struct DeadFieldDef {
    pub id: u128,
    pub name: String,
    pub type_def: TypeDef,
}

impl From<MemberDef> for DeadField {
    fn from(value: MemberDef) -> Self {
        DeadField {
            name: value.name,
            type_def: value.type_def,
        }
    }
}

impl From<DeadField> for MemberDef {
    fn from(value: DeadField) -> Self {
        MemberDef {
            name: value.name,
            attributes: Vec::new(),
            type_def: value.type_def,
        }
    }
}

impl From<DeadFieldDef> for (u128, DeadField) {
    fn from(value: DeadFieldDef) -> Self {
        (
            value.id,
            DeadField {
                name: value.name,
                type_def: value.type_def,
            },
        )
    }
}

impl From<(u128, DeadField)> for DeadFieldDef {
    fn from(value: (u128, DeadField)) -> Self {
        DeadFieldDef {
            id: value.0,
            name: value.1.name,
            type_def: value.1.type_def,
        }
    }
}

impl Table {
    pub fn column(&self, id: &Felt) -> Result<&ColumnInfo, ColumnNotFoundError> {
        self.columns.get(id).ok_or(ColumnNotFoundError(*id))
    }

    pub fn namespace(&self) -> Rc<str> {
        self.namespace.as_str().into()
    }

    pub fn columns(&self, ids: &[Felt]) -> Result<Vec<&ColumnInfo>, ColumnsNotFoundError> {
        ids.iter().map(|id| self.column(id)).collect_columns()
    }

    pub fn all_columns(&self) -> Vec<&ColumnInfo> {
        self.columns.values().collect()
    }

    pub fn columns_with_ids<'a>(
        &'a self,
        ids: &'a [Felt],
    ) -> Result<Vec<(&'a Felt, &'a ColumnInfo)>, ColumnsNotFoundError> {
        ids.iter()
            .map(|id| self.column(id).map(|col| (id, col)))
            .collect_columns()
    }

    pub fn all_columns_with_ids(&self) -> Vec<(&Felt, &ColumnInfo)> {
        self.columns.iter().collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace: String,
        id: Felt,
        owner: Felt,
        name: String,
        primary: PrimaryDef,
        columns: &[ColumnDef],
        dead: Option<Vec<(u128, DeadField)>>,
        append_only: bool,
    ) -> Self {
        Table {
            id,
            namespace,
            owner,
            name,
            primary: primary.into(),
            columns: columns.iter().cloned().map_into().collect(),
            dead: dead.unwrap_or_default().into_iter().collect(),
            append_only,
            alive: true,
        }
    }

    pub fn get_record_schema(
        &self,
        columns: &[Felt],
    ) -> Result<RecordSchema<'_>, ColumnsNotFoundError> {
        Ok(RecordSchema::new(&self.primary, self.columns(columns)?))
    }
}
