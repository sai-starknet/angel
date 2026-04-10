use crate::{TypeError, TypeResult};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::rc::Rc;

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
pub struct SchemaName(pub Rc<str>, pub String);

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
pub enum PostgresScalar {
    None,
    Boolean,
    SmallInt, // i16
    Int,      // i32
    BigInt,   // i64
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Uint256,
    Uint512,
    Felt252,
    StarknetHash,
    EthAddress,
    Text,
    Char31,
    Bytes31,
    Bytea,
    Composite(SchemaName),
}

#[derive(Debug)]
pub struct PrimaryKey {
    pub name: String,
    pub pg_type: PostgresScalar,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
pub struct PostgresType {
    pub scalar: PostgresScalar,
    pub array: PostgresArray,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PostgresField {
    pub name: String,
    pub pg_type: PostgresType,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
pub enum PostgresArray {
    None,
    Dynamic,
    Fixed(VecDeque<u32>),
}

impl From<PostgresScalar> for PostgresType {
    fn from(value: PostgresScalar) -> Self {
        PostgresType {
            scalar: value,
            array: PostgresArray::None,
        }
    }
}

impl Display for SchemaName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self.0.is_empty() {
            true => write!(f, r#""{}""#, self.1),
            false => write!(f, r#""{}"."{}""#, self.0, self.1),
        }
    }
}

impl Display for PostgresScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            PostgresScalar::None => f.write_str("VOID"),
            PostgresScalar::Boolean => f.write_str("BOOLEAN"),
            PostgresScalar::SmallInt => f.write_str("SMALLINT"),
            PostgresScalar::Int => f.write_str("INTEGER"),
            PostgresScalar::BigInt => f.write_str("BIGINT"),
            PostgresScalar::Int128 => f.write_str("public.int128"),
            PostgresScalar::Uint8 => f.write_str("public.uint8"),
            PostgresScalar::Uint16 => f.write_str("public.uint16"),
            PostgresScalar::Uint32 => f.write_str("public.uint32"),
            PostgresScalar::Uint64 => f.write_str("public.uint64"),
            PostgresScalar::Uint128 => f.write_str("public.uint128"),
            PostgresScalar::Uint256 => f.write_str("public.uint256"),
            PostgresScalar::Uint512 => f.write_str("public.uint512"),
            PostgresScalar::Felt252 => f.write_str("public.felt252"),
            PostgresScalar::Char31 => f.write_str("public.char31"),
            PostgresScalar::Bytes31 => f.write_str("public.byte31"),
            PostgresScalar::StarknetHash => f.write_str("public.starknet_hash"),
            PostgresScalar::EthAddress => f.write_str("public.eth_address"),
            PostgresScalar::Text => f.write_str("TEXT"),
            PostgresScalar::Bytea => f.write_str("BYTEA"),
            PostgresScalar::Composite(name) => name.fmt(f),
        }
    }
}

impl Display for PostgresArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            PostgresArray::None => Ok(()),
            PostgresArray::Dynamic => write!(f, "[]"),
            PostgresArray::Fixed(sizes) => sizes.iter().try_for_each(|size| write!(f, "[{size}]")),
        }
    }
}

impl Display for PostgresType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}{}", self.scalar, self.array)
    }
}

impl Display for PrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, r#""{}" {}"#, self.name, self.pg_type)
    }
}

impl Display for PostgresField {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, r#""{}" {}"#, self.name, self.pg_type)
    }
}

impl From<SchemaName> for PostgresType {
    fn from(value: SchemaName) -> Self {
        PostgresType {
            scalar: PostgresScalar::Composite(value),
            array: PostgresArray::None,
        }
    }
}

impl SchemaName {
    pub fn new<T: Into<String>>(schema: &Rc<str>, name: T) -> Self {
        Self(schema.clone(), name.into())
    }
    pub fn replace<S: Into<String>>(&mut self, name: S) -> String {
        std::mem::replace(&mut self.1, name.into())
    }
}

impl PostgresType {
    pub fn is_composite(&self) -> bool {
        matches!(self.scalar, PostgresScalar::Composite(_))
    }
    pub fn to_array(self, size: Option<u32>) -> TypeResult<Self> {
        let arr = match (self.array, size) {
            (PostgresArray::None, None) => PostgresArray::Dynamic,
            (PostgresArray::None, Some(size)) => PostgresArray::Fixed(VecDeque::from([size])),
            (PostgresArray::Fixed(mut sizes), Some(size)) => {
                sizes.push_back(size);
                PostgresArray::Fixed(sizes)
            }
            _ => return Err(TypeError::NestedArrays),
        };
        Ok(Self {
            scalar: self.scalar,
            array: arr,
        })
    }

    pub fn composite<S: Into<String>>(schema: &Rc<str>, name: S) -> Self {
        Self {
            scalar: PostgresScalar::Composite(SchemaName::new(schema, name)),
            array: PostgresArray::None,
        }
    }

    pub fn to_field(self, name: impl Into<String>) -> PostgresField {
        PostgresField::new(name, self)
    }
}

impl From<PostgresField> for (String, PostgresType) {
    fn from(val: PostgresField) -> Self {
        (val.name, val.pg_type)
    }
}
