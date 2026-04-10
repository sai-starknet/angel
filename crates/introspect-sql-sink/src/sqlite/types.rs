use std::fmt::Display;

use introspect_types::{ColumnDef, ColumnInfo, PrimaryDef, PrimaryTypeDef, TypeDef};

use crate::{TypeError, TypeResult};

pub enum SqliteType {
    Null,
    Text,
    Integer,
    Real,
    Blob,
    Json,
}

impl Display for SqliteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqliteType::Null => write!(f, "NULL"),
            SqliteType::Text => write!(f, "TEXT"),
            SqliteType::Integer => write!(f, "INTEGER"),
            SqliteType::Real => write!(f, "REAL"),
            SqliteType::Blob => write!(f, "BLOB"),
            SqliteType::Json => write!(f, "TEXT"),
        }
    }
}

pub struct SqliteColumn<'a> {
    pub name: &'a str,
    pub sql_type: SqliteType,
}

impl TryFrom<&TypeDef> for SqliteType {
    type Error = TypeError;

    fn try_from(value: &TypeDef) -> Result<Self, Self::Error> {
        match value {
            TypeDef::None => Ok(SqliteType::Null),
            TypeDef::Bool
            | TypeDef::U8
            | TypeDef::U16
            | TypeDef::U32
            | TypeDef::I8
            | TypeDef::I16
            | TypeDef::I32
            | TypeDef::I64 => Ok(SqliteType::Integer),
            TypeDef::U64 | TypeDef::U128 | TypeDef::U256 | TypeDef::U512 | TypeDef::I128 => {
                Ok(SqliteType::Text)
            }
            TypeDef::Felt252
            | TypeDef::ClassHash
            | TypeDef::ContractAddress
            | TypeDef::EthAddress
            | TypeDef::StorageAddress
            | TypeDef::StorageBaseAddress => Ok(SqliteType::Text),
            TypeDef::ShortUtf8 | TypeDef::Utf8String => Ok(SqliteType::Text),
            TypeDef::Bytes31
            | TypeDef::Bytes31Encoded(_)
            | TypeDef::ByteArray
            | TypeDef::ByteArrayEncoded(_) => Ok(SqliteType::Blob),
            TypeDef::Struct(_)
            | TypeDef::Enum(_)
            | TypeDef::Tuple(_)
            | TypeDef::Array(_)
            | TypeDef::FixedArray(_)
            | TypeDef::Option(_)
            | TypeDef::Nullable(_)
            | TypeDef::Result(_) => Ok(SqliteType::Json),
            TypeDef::Custom(_) => Ok(SqliteType::Blob),
            _ => Err(TypeError::UnsupportedType(value.item_name().to_string())),
        }
    }
}

impl TryFrom<&ColumnDef> for SqliteType {
    type Error = TypeError;

    fn try_from(value: &ColumnDef) -> Result<Self, Self::Error> {
        (&value.type_def).try_into()
    }
}

impl TryFrom<&ColumnInfo> for SqliteType {
    type Error = TypeError;

    fn try_from(value: &ColumnInfo) -> Result<Self, Self::Error> {
        (&value.type_def).try_into()
    }
}

impl TryFrom<&PrimaryTypeDef> for SqliteType {
    type Error = TypeError;

    fn try_from(value: &PrimaryTypeDef) -> Result<Self, Self::Error> {
        (&Into::<TypeDef>::into(value)).try_into()
    }
}

impl TryFrom<&PrimaryDef> for SqliteType {
    type Error = TypeError;

    fn try_from(value: &PrimaryDef) -> Result<Self, Self::Error> {
        (&value.type_def).try_into()
    }
}

impl<'a> TryFrom<&'a ColumnInfo> for SqliteColumn<'a> {
    type Error = TypeError;

    fn try_from(value: &'a ColumnInfo) -> Result<Self, Self::Error> {
        let sql_type = (&value.type_def).try_into()?;
        Ok(SqliteColumn {
            name: &value.name,
            sql_type,
        })
    }
}

impl SqliteType {
    pub fn placeholder(&self) -> &'static str {
        match self {
            SqliteType::Null => "NULL",
            SqliteType::Text | SqliteType::Integer | SqliteType::Real | SqliteType::Blob => "?",
            SqliteType::Json => "jsonb(?)",
        }
    }
    pub fn index_placeholder(&self, index: usize) -> String {
        match self {
            SqliteType::Null => "NULL".to_string(),
            SqliteType::Text | SqliteType::Integer | SqliteType::Real | SqliteType::Blob => {
                format!("?{index}")
            }
            SqliteType::Json => format!("jsonb(?{index})"),
        }
    }
}

impl<'a> SqliteColumn<'a> {
    pub fn placeholder(&self) -> &'static str {
        self.sql_type.placeholder()
    }
    pub fn index_placeholder(&self, index: usize) -> String {
        self.sql_type.index_placeholder(index)
    }
}

pub trait TypeDefSqliteExt {
    fn placeholder(&self) -> TypeResult<&'static str>;
    fn index_placeholder(&self, index: usize) -> TypeResult<String>;
}

impl TypeDefSqliteExt for TypeDef {
    fn placeholder(&self) -> TypeResult<&'static str> {
        self.try_into().map(|t: SqliteType| t.placeholder())
    }
    fn index_placeholder(&self, index: usize) -> TypeResult<String> {
        self.try_into()
            .map(|t: SqliteType| t.index_placeholder(index))
    }
}
