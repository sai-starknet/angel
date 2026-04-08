use crate::error::DojoToriiError;
use crate::DojoToriiResult;
use dojo_introspect::selector::compute_selector_from_namespace_and_name;
use dojo_introspect::{DojoSchema, DojoSerde};
use introspect_types::transcode::Transcode;
use introspect_types::{Attribute, Attributes, CairoSerde, ColumnDef, ColumnInfo, PrimaryDef};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use torii_introspect::schema::TableSchema;

const LEGACY_ATTRIBUTE: &str = "legacy";

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct DojoTable {
    pub id: Felt,
    pub name: String,
    pub attributes: Vec<String>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub key_fields: Vec<Felt>,
    pub value_fields: Vec<Felt>,
    pub legacy: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct DojoTableInfo {
    pub name: String,
    pub attributes: Vec<String>,
    pub primary: PrimaryDef,
    pub columns: HashMap<Felt, ColumnInfo>,
    pub key_fields: Vec<Felt>,
    pub value_fields: Vec<Felt>,
    pub legacy: bool,
}

pub fn sort_columns(columns: Vec<ColumnDef>) -> (HashMap<Felt, ColumnInfo>, Vec<Felt>, Vec<Felt>) {
    let mut field_map = HashMap::new();
    let mut value_fields = Vec::new();
    let mut key_fields = Vec::new();
    for column in columns {
        let (id, info) = column.into();
        if info.has_attribute("key") {
            key_fields.push(id);
        } else {
            value_fields.push(id);
        }
        field_map.insert(id, info);
    }
    (field_map, key_fields, value_fields)
}

impl From<TableSchema> for DojoTable {
    fn from(value: TableSchema) -> Self {
        let (columns, key_fields, value_fields) = sort_columns(value.columns);
        let legacy = value.attributes.has_attribute(LEGACY_ATTRIBUTE);
        DojoTable {
            id: value.id,
            name: value.name,
            attributes: value.attributes.into_iter().map(|a| a.name).collect(),
            primary: value.primary,
            columns,
            key_fields,
            value_fields,
            legacy,
        }
    }
}

impl From<DojoTable> for TableSchema {
    fn from(value: DojoTable) -> Self {
        let DojoTable {
            id,
            name,
            attributes,
            primary,
            mut columns,
            key_fields,
            value_fields,
            legacy,
        } = value;
        let mut attributes = attributes
            .into_iter()
            .map(Attribute::new_empty)
            .collect::<Vec<_>>();
        if legacy && !attributes.has_attribute(LEGACY_ATTRIBUTE) {
            attributes.push(Attribute::new_empty(LEGACY_ATTRIBUTE.to_string()));
        }
        TableSchema {
            id,
            name,
            primary,
            columns: key_fields
                .into_iter()
                .chain(value_fields)
                .map(|selector| (selector, columns.remove(&selector).unwrap()).into())
                .collect(),
            attributes,
        }
    }
}

impl From<DojoTable> for (Felt, DojoTableInfo) {
    fn from(value: DojoTable) -> Self {
        (
            value.id,
            DojoTableInfo {
                name: value.name,
                attributes: value.attributes,
                primary: value.primary,
                columns: value.columns,
                key_fields: value.key_fields,
                value_fields: value.value_fields,
                legacy: value.legacy,
            },
        )
    }
}

impl From<(Felt, DojoTableInfo)> for DojoTable {
    fn from(value: (Felt, DojoTableInfo)) -> Self {
        let (id, info) = value;
        DojoTable {
            id,
            name: info.name,
            attributes: info.attributes,
            primary: info.primary,
            columns: info.columns,
            key_fields: info.key_fields,
            value_fields: info.value_fields,
            legacy: info.legacy,
        }
    }
}

impl From<(&Felt, &DojoTableInfo)> for DojoTable {
    fn from(value: (&Felt, &DojoTableInfo)) -> Self {
        let (id, info) = value;
        DojoTable {
            id: *id,
            name: info.name.clone(),
            attributes: info.attributes.clone(),
            primary: info.primary.clone(),
            columns: info.columns.clone(),
            key_fields: info.key_fields.clone(),
            value_fields: info.value_fields.clone(),
            legacy: info.legacy,
        }
    }
}

impl DojoTable {
    pub fn from_schema(
        schema: DojoSchema,
        namespace: &str,
        name: &str,
        primary: PrimaryDef,
    ) -> Self {
        let (columns, key_fields, value_fields) = sort_columns(schema.columns);
        Self {
            id: compute_selector_from_namespace_and_name(namespace, name),
            name: format!("{namespace}-{name}"),
            attributes: schema.attributes.iter().map(|a| a.name.clone()).collect(),
            primary,
            columns,
            key_fields,
            value_fields,
            legacy: schema.legacy,
        }
    }

    pub fn get_columns(&self, selectors: &[Felt]) -> DojoToriiResult<Vec<&ColumnInfo>> {
        selectors
            .iter()
            .map(|selector| self.get_column(selector))
            .collect()
    }

    pub fn get_column(&self, selector: &Felt) -> DojoToriiResult<&ColumnInfo> {
        self.columns
            .get(selector)
            .ok_or_else(|| DojoToriiError::ColumnNotFound(self.name.clone(), *selector))
    }

    pub fn selectors(&self) -> impl Iterator<Item = &Felt> + '_ {
        self.key_fields.iter().chain(self.value_fields.iter())
    }

    pub fn to_schema(&self) -> TableSchema {
        TableSchema {
            id: self.id,
            name: self.name.clone(),
            attributes: self
                .attributes
                .iter()
                .cloned()
                .map(Attribute::new_empty)
                .collect::<Vec<_>>(),
            primary: self.primary.clone(),
            columns: self
                .selectors()
                .map(|selector| (*selector, self.get_column(selector).cloned().unwrap()).into())
                .collect(),
        }
    }

    pub fn parse_keys(&self, keys: Vec<Felt>) -> DojoToriiResult<Vec<u8>> {
        let mut keys: CairoSerde<_> = keys.into();
        let columns = self.get_columns(&self.key_fields)?;
        columns
            .transcode_complete(&mut keys)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_values(&self, values: Vec<Felt>) -> DojoToriiResult<(Vec<Felt>, Vec<u8>)> {
        let mut output = Vec::new();
        self.add_parsed_values(values, &mut output)?;
        Ok((self.value_fields.clone(), output))
    }

    pub fn add_parsed_values(
        &self,
        values: Vec<Felt>,
        output: &mut Vec<u8>,
    ) -> DojoToriiResult<()> {
        let mut values: DojoSerde<_> = DojoSerde::new_from_source(values, self.legacy);
        let columns = self.get_columns(&self.value_fields)?;
        columns
            .transcode(&mut values, output)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_record(
        &self,
        keys: Vec<Felt>,
        values: Vec<Felt>,
    ) -> DojoToriiResult<(Vec<Felt>, Vec<u8>)> {
        let mut data = self.parse_keys(keys)?;
        self.add_parsed_values(values, &mut data)?;
        Ok((
            [self.key_fields.clone(), self.value_fields.clone()].concat(),
            data,
        ))
    }

    pub fn parse_field(&self, selector: Felt, data: Vec<Felt>) -> DojoToriiResult<Vec<u8>> {
        let mut data: DojoSerde<_> = DojoSerde::new_from_source(data, self.legacy);
        let column = self.get_column(&selector)?;
        column
            .transcode_complete(&mut data)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_fields(&self, selectors: &[Felt], data: &[Felt]) -> DojoToriiResult<Vec<u8>> {
        let mut data: DojoSerde<_> = DojoSerde::new_from_source(data, self.legacy);
        let columns = self.get_columns(selectors)?;
        columns
            .transcode_complete(&mut data)
            .map_err(DojoToriiError::TranscodeError)
    }
}

impl DojoTableInfo {
    pub fn get_columns(&self, selectors: &[Felt]) -> DojoToriiResult<Vec<&ColumnInfo>> {
        selectors
            .iter()
            .map(|selector| self.get_column(selector))
            .collect()
    }

    pub fn get_column(&self, selector: &Felt) -> DojoToriiResult<&ColumnInfo> {
        self.columns
            .get(selector)
            .ok_or_else(|| DojoToriiError::ColumnNotFound(self.name.clone(), *selector))
    }

    pub fn selectors(&self) -> impl Iterator<Item = &Felt> + '_ {
        self.key_fields.iter().chain(self.value_fields.iter())
    }

    pub fn to_schema(&self, id: Felt) -> TableSchema {
        TableSchema {
            id,
            name: self.name.clone(),
            attributes: self
                .attributes
                .iter()
                .cloned()
                .map(Attribute::new_empty)
                .collect::<Vec<_>>(),
            primary: self.primary.clone(),
            columns: self
                .selectors()
                .map(|selector| (*selector, self.get_column(selector).cloned().unwrap()).into())
                .collect(),
        }
    }

    pub fn parse_keys(&self, keys: Vec<Felt>) -> DojoToriiResult<Vec<u8>> {
        let mut keys: CairoSerde<_> = keys.into();
        let columns = self.get_columns(&self.key_fields)?;
        columns
            .transcode_complete(&mut keys)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_values(&self, values: Vec<Felt>) -> DojoToriiResult<(Vec<Felt>, Vec<u8>)> {
        let mut output = Vec::new();
        self.add_parsed_values(values, &mut output)?;
        Ok((self.value_fields.clone(), output))
    }

    pub fn add_parsed_values(
        &self,
        values: Vec<Felt>,
        output: &mut Vec<u8>,
    ) -> DojoToriiResult<()> {
        let mut values: DojoSerde<_> = DojoSerde::new_from_source(values, self.legacy);
        let columns = self.get_columns(&self.value_fields)?;
        columns
            .transcode(&mut values, output)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_record(
        &self,
        keys: Vec<Felt>,
        values: Vec<Felt>,
    ) -> DojoToriiResult<(Vec<Felt>, Vec<u8>)> {
        let mut data = self.parse_keys(keys)?;
        self.add_parsed_values(values, &mut data)?;
        Ok((
            [self.key_fields.clone(), self.value_fields.clone()].concat(),
            data,
        ))
    }

    pub fn parse_field(&self, selector: Felt, data: Vec<Felt>) -> DojoToriiResult<Vec<u8>> {
        let mut data: DojoSerde<_> = DojoSerde::new_from_source(data, self.legacy);
        let column = self.get_column(&selector)?;
        column
            .transcode_complete(&mut data)
            .map_err(DojoToriiError::TranscodeError)
    }

    pub fn parse_fields(&self, selectors: &[Felt], data: &[Felt]) -> DojoToriiResult<Vec<u8>> {
        let mut data: DojoSerde<_> = DojoSerde::new_from_source(data, self.legacy);
        let columns = self.get_columns(selectors)?;
        columns
            .transcode_complete(&mut data)
            .map_err(DojoToriiError::TranscodeError)
    }
}
