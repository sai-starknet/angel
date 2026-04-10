//! Local event types for the introspect pipeline.

pub use introspect_events::database::{IdName, IdTypeDef};
use introspect_rust_macros::EnumFrom;
use introspect_types::{
    Attribute, ColumnDef, FeltId, FeltIds, PrimaryDef, PrimaryTypeDef, PrimaryValue, TypeDef,
};
use serde::{Deserialize, Serialize};
use starknet_types_raw::Felt;
use torii::etl::envelope::EventMsg;
use torii::etl::{EventBody, TypeId};

use crate::schema::TableSchema;

#[derive(EnumFrom, Debug)]
pub enum IntrospectMsg {
    CreateTable(CreateTable),
    UpdateTable(UpdateTable),
    RenameTable(RenameTable),
    RenamePrimary(RenamePrimary),
    RetypePrimary(RetypePrimary),
    RenameColumns(RenameColumns),
    RetypeColumns(RetypeColumns),
    AddColumns(AddColumns),
    DropTable(DropTable),
    DropColumns(DropColumns),
    InsertsFields(InsertsFields),
    DeleteRecords(DeleteRecords),
    DeletesFields(DeletesFields),
}

pub type IntrospectBody = EventBody<IntrospectMsg>;

pub trait EventId {
    fn event_id(&self) -> String;
}

impl EventMsg for IntrospectMsg {
    fn event_id(&self) -> String {
        match self {
            IntrospectMsg::CreateTable(e) => e.event_id(),
            IntrospectMsg::UpdateTable(e) => e.event_id(),
            IntrospectMsg::RenameTable(e) => e.event_id(),
            IntrospectMsg::RenamePrimary(e) => e.event_id(),
            IntrospectMsg::RetypePrimary(e) => e.event_id(),
            IntrospectMsg::RenameColumns(e) => e.event_id(),
            IntrospectMsg::RetypeColumns(e) => e.event_id(),
            IntrospectMsg::AddColumns(e) => e.event_id(),
            IntrospectMsg::DropTable(e) => e.event_id(),
            IntrospectMsg::DropColumns(e) => e.event_id(),
            IntrospectMsg::InsertsFields(e) => e.event_id(),
            IntrospectMsg::DeleteRecords(e) => e.event_id(),
            IntrospectMsg::DeletesFields(e) => e.event_id(),
        }
    }

    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("introspect")
    }
}

impl EventId for IntrospectBody {
    fn event_id(&self) -> String {
        self.msg.event_id()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
    pub append_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub primary: PrimaryDef,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameTable {
    pub id: Felt,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenamePrimary {
    pub table: Felt,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetypePrimary {
    pub table: Felt,
    pub attributes: Vec<Attribute>,
    pub type_def: PrimaryTypeDef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameColumns {
    pub table: Felt,
    pub columns: Vec<IdName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetypeColumns {
    pub table: Felt,
    pub columns: Vec<IdTypeDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddColumns {
    pub table: Felt,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTable {
    pub owner: Option<Felt>,
    pub id: Felt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropColumns {
    pub owner: Option<Felt>,
    pub table: Felt,
    pub columns: Vec<Felt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: [u8; 32],
    pub values: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertsFields {
    pub table: Felt,
    pub columns: Vec<Felt>,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRecords {
    pub table: Felt,
    pub rows: Vec<PrimaryValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletesFields {
    pub table: Felt,
    pub rows: Vec<PrimaryValue>,
    pub columns: Vec<Felt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFieldGroup {
    pub id: Felt,
    pub columns: Vec<Felt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTypeDef {
    pub owner: Option<Felt>,
    pub id: Felt,
    pub type_def: TypeDef,
}

impl EventId for CreateTable {
    fn event_id(&self) -> String {
        format!("introspect.create_table.{:064x}", self.id)
    }
}
impl EventId for UpdateTable {
    fn event_id(&self) -> String {
        format!("introspect.update_table.{:064x}", self.id)
    }
}
impl EventId for RenameTable {
    fn event_id(&self) -> String {
        format!("introspect.rename_table.{:064x}", self.id)
    }
}
impl EventId for RenamePrimary {
    fn event_id(&self) -> String {
        format!("introspect.rename_primary.{:064x}", self.table)
    }
}
impl EventId for RetypePrimary {
    fn event_id(&self) -> String {
        format!("introspect.retype_primary.{:064x}", self.table)
    }
}
impl EventId for RenameColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.rename_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}
impl EventId for RetypeColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.retype_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}
impl EventId for AddColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.add_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}
impl EventId for DropTable {
    fn event_id(&self) -> String {
        format!("introspect.drop_table.{:064x}", self.id)
    }
}
impl EventId for DropColumns {
    fn event_id(&self) -> String {
        format!(
            "introspect.drop_columns.{:064x}.{}",
            self.table,
            self.columns.hash()
        )
    }
}

impl EventId for InsertsFields {
    fn event_id(&self) -> String {
        format!(
            "introspect.insert_fields.{:064x}.{}.{}",
            self.table,
            self.records.hash(),
            self.columns.hash()
        )
    }
}

impl EventId for DeleteRecords {
    fn event_id(&self) -> String {
        format!(
            "introspect.delete_records.{:064x}.{}",
            self.table,
            self.rows.hash()
        )
    }
}
impl EventId for DeletesFields {
    fn event_id(&self) -> String {
        format!(
            "introspect.deletes_fields.{:064x}.{}.{}",
            self.table,
            self.rows.hash(),
            self.columns.hash()
        )
    }
}
impl EventId for CreateFieldGroup {
    fn event_id(&self) -> String {
        format!("introspect.create_field_group.{:064x}", self.id)
    }
}
impl EventId for CreateTypeDef {
    fn event_id(&self) -> String {
        format!("introspect.create_type_def.{:064x}", self.id)
    }
}

impl From<TableSchema> for CreateTable {
    fn from(schema: TableSchema) -> Self {
        Self {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
            append_only: false,
        }
    }
}

impl From<CreateTable> for TableSchema {
    fn from(schema: CreateTable) -> Self {
        TableSchema {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

impl From<TableSchema> for UpdateTable {
    fn from(schema: TableSchema) -> Self {
        Self {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

impl From<UpdateTable> for TableSchema {
    fn from(schema: UpdateTable) -> Self {
        TableSchema {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
        }
    }
}

impl InsertsFields {
    #[allow(private_bounds)]
    pub fn new_single<K: ToKeyBytes>(
        table: Felt,
        columns: Vec<Felt>,
        key: K,
        values: Vec<u8>,
    ) -> Self {
        Self {
            table,
            columns,
            records: vec![Record::new(key, values)],
        }
    }
    pub fn new(table: Felt, columns: Vec<Felt>, records: Vec<Record>) -> Self {
        Self {
            table,
            columns,
            records,
        }
    }
}

impl CreateTable {
    pub fn from_schema(schema: TableSchema, append_only: bool) -> Self {
        Self {
            id: schema.id,
            name: schema.name,
            attributes: schema.attributes,
            primary: schema.primary,
            columns: schema.columns,
            append_only,
        }
    }
}

impl DeleteRecords {
    pub fn new(table: Felt, rows: Vec<PrimaryValue>) -> Self {
        Self { table, rows }
    }
}

impl Record {
    #[allow(private_bounds)]
    pub fn new<T: ToKeyBytes>(id: T, values: Vec<u8>) -> Self {
        Self {
            id: id.to_key_bytes(),
            values,
        }
    }
}

impl FeltId for Record {
    fn id(&self) -> Felt {
        self.id.into()
    }
}

trait ToKeyBytes {
    fn to_key_bytes(&self) -> [u8; 32];
}

impl ToKeyBytes for Felt {
    fn to_key_bytes(&self) -> [u8; 32] {
        self.to_be_bytes()
    }
}

impl ToKeyBytes for [u8; 32] {
    fn to_key_bytes(&self) -> [u8; 32] {
        *self
    }
}
