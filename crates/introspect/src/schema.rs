use introspect_types::{Attribute, ColumnDef, PrimaryDef};
use starknet_types_raw::Felt;
use std::collections::HashMap;

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct ColumnKey {
    pub table: Felt,
    pub id: Felt,
}

impl From<ColumnKey> for (Felt, Felt) {
    fn from(value: ColumnKey) -> Self {
        (value.table, value.id)
    }
}

impl From<(Felt, Felt)> for ColumnKey {
    fn from(value: (Felt, Felt)) -> Self {
        let (table, id) = value;
        ColumnKey { table, id }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

impl From<TableSchema> for (Felt, TableInfo) {
    fn from(value: TableSchema) -> Self {
        (
            value.id,
            TableInfo {
                name: value.name,
                attributes: value.attributes,
                primary: value.primary,
                columns: value.columns,
            },
        )
    }
}

impl From<(Felt, TableInfo)> for TableSchema {
    fn from(value: (Felt, TableInfo)) -> Self {
        let (id, info) = value;
        TableSchema {
            id,
            name: info.name,
            attributes: info.attributes,
            primary: info.primary,
            columns: info.columns,
        }
    }
}

pub struct TableInfo {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnDef>,
    pub order: Vec<Felt>,
    pub alive: bool,
}
// impl From<(Felt, TableInfo)> for TableSchema {
//     fn from(value: (Felt, TableInfo)) -> Self {
//         let (id, info) = value;
//         TableSchema {
//             id,
//             name: info.name,
//             attributes: info.attributes,
//             primary: info.primary,
//             columns: HashMap::with_capacity(info.order.len()),
//             order: info.order,
//         }
//     }
// }

// impl From<TableSchema> for (Felt, TableInfo) {
//     fn from(value: TableSchema) -> Self {
//         (
//             value.id,
//             TableInfo {
//                 name: value.name,
//                 attributes: value.attributes,
//                 primary: value.primary,
//                 columns: HashMap::with_capacity(info.order.len()),
//                 order: info.order,
//             },
//         )
//     }
// }

pub trait ColumnKeyTrait {
    fn as_parts(&self) -> (&Felt, &Felt);
    fn from_parts(table: Felt, id: Felt) -> Self;
}

impl ColumnKeyTrait for ColumnKey {
    fn as_parts(&self) -> (&Felt, &Felt) {
        (&self.table, &self.id)
    }
    fn from_parts(table: Felt, id: Felt) -> Self {
        ColumnKey { table, id }
    }
}

impl ColumnKeyTrait for (Felt, Felt) {
    fn as_parts(&self) -> (&Felt, &Felt) {
        (&self.0, &self.1)
    }
    fn from_parts(table: Felt, id: Felt) -> Self {
        (table, id)
    }
}
