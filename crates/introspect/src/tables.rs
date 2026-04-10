use crate::Record;
use introspect_types::bytes::IntoByteSource;
use introspect_types::schema::Names;
use introspect_types::serialize::CairoSeFrom;
use introspect_types::serialize_def::CairoTypeSerialization;
use introspect_types::{CairoDeserializer, ColumnInfo, PrimaryDef, PrimaryTypeDef, TypeDef};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use std::ops::Deref;

pub struct RecordSchema<'a> {
    primary: &'a ColumnInfo,
    columns: Vec<&'a ColumnInfo>,
}

impl<'a> RecordSchema<'a> {
    pub fn new(primary: &'a ColumnInfo, columns: Vec<&'a ColumnInfo>) -> Self {
        Self { primary, columns }
    }
    pub fn columns(&self) -> &[&'a ColumnInfo] {
        &self.columns
    }
    pub fn all_columns(&self) -> Vec<&'a ColumnInfo> {
        std::iter::once(self.primary)
            .chain(self.columns.iter().copied())
            .collect()
    }
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.names()
    }

    pub fn primary(&self) -> &ColumnInfo {
        self.primary
    }
    pub fn primary_type_def(&self) -> &TypeDef {
        &self.primary.type_def
    }

    pub fn primary_name(&self) -> &str {
        &self.primary.name
    }
    pub fn to_frame<C: CairoTypeSerialization, M: SerializeEntries>(
        &'a self,
        record: &'a Record,
        metadata: &'a M,
        cairo_se: &'a C,
    ) -> RecordFrame<'a, C, M> {
        RecordFrame::new(record, self, metadata, cairo_se)
    }

    pub fn parse_records_with_metadata<
        S: Serializer,
        C: CairoTypeSerialization,
        M: SerializeEntries,
    >(
        &'a self,
        records: &'a [Record],
        metadata: &'a M,
        serializer: S,
        cairo_se: &'a C,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(records.len()))?;
        for record in records {
            seq.serialize_element(&self.to_frame(record, metadata, cairo_se))?;
        }
        seq.end()
    }
}

pub trait SerializeEntries {
    fn entry_count(&self) -> usize;
    fn serialize_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error>;
}

pub trait AsEntryPair {
    type Key;
    type Value;
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value);
}

impl<'a, T, D, C, E> SerializeEntries for CairoSeFrom<'a, T, D, C>
where
    E: AsEntryPair + 'a,
    C: CairoTypeSerialization,
    D: CairoDeserializer,
    T: Deref<Target = [&'a E]>,
    <E as AsEntryPair>::Key: Serialize,
    CairoSeFrom<'a, <E as AsEntryPair>::Value, D, C>: Serialize,
{
    fn entry_count(&self) -> usize {
        self.schema().deref().len()
    }

    fn serialize_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        for entry in self.schema().deref() {
            let (key, value) = entry.to_entry_pair();
            map.serialize_entry(key, &self.to_schema(value))?;
        }
        Ok(())
    }
}

impl AsEntryPair for ColumnInfo {
    type Key = String;
    type Value = TypeDef;
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value) {
        (&self.name, &self.type_def)
    }
}

impl AsEntryPair for PrimaryDef {
    type Key = String;
    type Value = PrimaryTypeDef;
    fn to_entry_pair(&self) -> (&Self::Key, &Self::Value) {
        (&self.name, &self.type_def)
    }
}

pub struct RecordWithMetadata<'a, M: SerializeEntries> {
    record: &'a Record,
    metadata: &'a M,
}

impl<'a, M: SerializeEntries> RecordWithMetadata<'a, M> {
    pub fn to_frame<C: CairoTypeSerialization>(
        &self,
        schema: &'a RecordSchema<'a>,
        cairo_se: &'a C,
    ) -> RecordFrame<'a, C, M> {
        RecordFrame::new(self.record, schema, self.metadata, cairo_se)
    }
}

pub struct RecordFrame<'a, C: CairoTypeSerialization, M: SerializeEntries> {
    primary: &'a ColumnInfo,
    columns: &'a [&'a ColumnInfo],
    id: &'a [u8; 32],
    values: &'a [u8],
    cairo_se: &'a C,
    metadata: &'a M,
}

impl<'a, C: CairoTypeSerialization, M: SerializeEntries> RecordFrame<'a, C, M> {
    pub fn new(
        record: &'a Record,
        schema: &'a RecordSchema<'a>,
        metadata: &'a M,
        cairo_se: &'a C,
    ) -> Self {
        Self {
            primary: schema.primary,
            columns: &schema.columns,
            id: &record.id,
            values: &record.values,
            metadata,
            cairo_se,
        }
    }

    pub fn parse_primary_as_entry<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        let mut id = self.id.into_source();
        map.serialize_entry(
            &self.primary.name,
            &CairoSeFrom::new(&self.primary.type_def, &mut id, self.cairo_se),
        )?;
        Ok(())
    }

    pub fn parse_record_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        let mut source = self.values.into_source();
        let sede = CairoSeFrom::new(&self.columns, &mut source, self.cairo_se);
        sede.serialize_entries::<S>(map)?;
        Ok(())
    }
}

impl<C: CairoTypeSerialization, M: SerializeEntries> SerializeEntries for RecordFrame<'_, C, M> {
    fn entry_count(&self) -> usize {
        1 + self.columns.len() + self.metadata.entry_count()
    }

    fn serialize_entries<S: Serializer>(
        &self,
        map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        self.parse_primary_as_entry::<S>(map)?;
        self.parse_record_entries::<S>(map)?;
        self.metadata.serialize_entries::<S>(map)
    }
}

impl<C: CairoTypeSerialization, M: SerializeEntries> Serialize for RecordFrame<'_, C, M> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.entry_count()))?;
        self.serialize_entries::<S>(&mut map)?;
        map.end()
    }
}

impl SerializeEntries for () {
    fn entry_count(&self) -> usize {
        0
    }

    fn serialize_entries<S: Serializer>(
        &self,
        _map: &mut <S as Serializer>::SerializeMap,
    ) -> Result<(), S::Error> {
        Ok(())
    }
}
