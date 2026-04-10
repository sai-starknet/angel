use std::fmt::Display;

use introspect_types::{Attribute, PrimaryDef, PrimaryTypeDef};
use itertools::Itertools;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
use sqlx::types::{BigDecimal, Json};
use sqlx::{Decode, Encode, Postgres, Type};
use starknet_types_raw::Felt;

#[derive(sqlx::Type, Debug)]
#[sqlx(type_name = "introspect.attribute", no_pg_array)]
pub struct PgAttribute {
    pub name: String,
    pub data: Option<Vec<u8>>,
}

impl PgHasArrayType for PgAttribute {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("introspect.attribute[]")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgFelt(pub [u8; 32]);

impl Type<Postgres> for PgFelt {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("felt252")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        *ty == PgTypeInfo::with_name("felt252") || <[u8] as Type<Postgres>>::compatible(ty)
    }
}

impl PgHasArrayType for PgFelt {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_felt252")
    }
}

impl Encode<'_, Postgres> for PgFelt {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&[u8] as Encode<Postgres>>::encode(self.0.as_slice(), buf)
    }
}

impl Decode<'_, Postgres> for PgFelt {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        let bytes = <&[u8] as Decode<Postgres>>::decode(value)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| format!("expected 32 bytes for felt252, got {}", bytes.len()))?;
        Ok(PgFelt(arr))
    }
}

impl Display for PgFelt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'\\x{}'::felt252", hex::encode(self.0))
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Uint128(pub BigDecimal);

impl Type<Postgres> for Uint128 {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("uint128")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        *ty == PgTypeInfo::with_name("uint128") || <BigDecimal as Type<Postgres>>::compatible(ty)
    }
}

impl PgHasArrayType for Uint128 {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_uint128")
    }
}

impl Encode<'_, Postgres> for Uint128 {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <BigDecimal as Encode<Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl Decode<'_, Postgres> for Uint128 {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        let bd = <BigDecimal as Decode<Postgres>>::decode(value)?;
        Ok(Uint128(bd))
    }
}

impl From<Uint128> for u128 {
    fn from(value: Uint128) -> Self {
        use bigdecimal::ToPrimitive;
        value.0.to_u128().expect("value out of range for u128")
    }
}

impl From<u128> for Uint128 {
    fn from(value: u128) -> Self {
        Uint128(BigDecimal::from(value))
    }
}

impl From<PgAttribute> for Attribute {
    fn from(value: PgAttribute) -> Self {
        Attribute {
            name: value.name,
            data: value.data,
        }
    }
}

impl From<Attribute> for PgAttribute {
    fn from(value: Attribute) -> Self {
        PgAttribute {
            name: value.name,
            data: value.data,
        }
    }
}

impl From<&Attribute> for PgAttribute {
    fn from(value: &Attribute) -> Self {
        PgAttribute {
            name: value.name.clone(),
            data: value.data.clone(),
        }
    }
}

impl From<PgFelt> for Felt {
    fn from(value: PgFelt) -> Self {
        value.0.into()
    }
}

impl From<Felt> for PgFelt {
    fn from(value: Felt) -> Self {
        PgFelt(value.to_be_bytes())
    }
}

impl From<&Felt> for PgFelt {
    fn from(value: &Felt) -> Self {
        PgFelt(value.to_be_bytes())
    }
}

#[derive(sqlx::Type, Debug)]
#[sqlx(type_name = "introspect.primary_def")]
pub struct PgPrimary {
    name: String,
    attributes: Vec<PgAttribute>,
    type_def: Json<PrimaryTypeDef>,
}

impl From<PgPrimary> for PrimaryDef {
    fn from(value: PgPrimary) -> Self {
        PrimaryDef {
            name: value.name,
            attributes: value.attributes.into_iter().map_into().collect(),
            type_def: value.type_def.0,
        }
    }
}

impl From<PrimaryDef> for PgPrimary {
    fn from(value: PrimaryDef) -> Self {
        PgPrimary {
            name: value.name,
            attributes: value.attributes.into_iter().map_into().collect(),
            type_def: Json(value.type_def),
        }
    }
}

impl From<&PrimaryDef> for PgPrimary {
    fn from(value: &PrimaryDef) -> Self {
        PgPrimary {
            name: value.name.clone(),
            attributes: value.attributes.iter().cloned().map_into().collect(),
            type_def: Json(value.type_def.clone()),
        }
    }
}

pub fn felt252_type(value: &Felt) -> String {
    format!("'\\x{}'::felt252", hex::encode(value.to_be_bytes()))
}

pub fn felt252_array_type(values: &[Felt]) -> String {
    let value_strs = values.iter().map(felt252_type).join(",");
    format!("ARRAY[{value_strs}]::felt252[]")
}

pub fn string_type(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn attribute_type(attr: &Attribute) -> String {
    let data = match &attr.data {
        Some(bytes) => format!("'\\x{}'::bytea", hex::encode(bytes)),
        None => "NULL".to_string(),
    };
    format!(
        "ROW({}, {})::introspect.attribute",
        string_type(&attr.name),
        data
    )
}

pub fn attributes_array_type(attrs: &[Attribute]) -> String {
    let attr_strs = attrs.iter().map(attribute_type).join(",");
    format!("ARRAY[{attr_strs}]::introspect.attribute[]")
}

pub fn primary_def_type(primary: &PrimaryDef) -> String {
    format!(
        "ROW({name}, {attributes}, {type_def})::introspect.primary_def",
        name = string_type(&primary.name),
        attributes = attributes_array_type(&primary.attributes),
        type_def = string_type(&serde_json::to_string(&primary.type_def).unwrap())
    )
}
