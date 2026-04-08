use super::query::{make_schema_query, CreatePgTable, CreatesType};
use super::utils::{AsBytes, HasherExt};
use super::{PostgresField, PostgresScalar, PostgresType, PrimaryKey, SchemaName};
use crate::{TypeError, TypeResult};
use introspect_types::{
    ArrayDef, ColumnDef, EnumDef, FixedArrayDef, MemberDef, OptionDef, PrimaryDef, PrimaryTypeDef,
    StructDef, TupleDef, TypeDef, VariantDef,
};
use itertools::Itertools;
use starknet_types_core::felt::Felt;
use std::rc::Rc;
use torii_sql::postgres::PgQuery;
use torii_sql::Queries;
use xxhash_rust::xxh3::Xxh3;

pub trait PostgresTypeExtractor {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType>;
}

pub trait PostgresFieldExtractor {
    type Id: AsBytes;
    fn name(&self) -> &str;
    fn type_def(&self) -> &TypeDef;
    fn id(&self) -> &Self::Id;
    fn branch(&self, branch: &Xxh3) -> Xxh3 {
        branch.branch(self.id())
    }
    fn extract_field(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresField> {
        Ok(PostgresField::new(
            self.name().to_string(),
            self.type_def()
                .extract_type(schema, &self.branch(branch), creates)?,
        ))
    }
}

impl PostgresField {
    pub fn new(name: impl Into<String>, pg_type: PostgresType) -> Self {
        Self {
            name: name.into(),
            pg_type,
        }
    }

    pub fn new_composite<S, T>(name: S, schema: &Rc<str>, type_name: T) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        Self {
            name: name.into(),
            pg_type: PostgresType::composite(schema, type_name),
        }
    }
}

impl PostgresFieldExtractor for ColumnDef {
    type Id = Felt;
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

impl PostgresFieldExtractor for MemberDef {
    type Id = String;
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn id(&self) -> &Self::Id {
        &self.name
    }
}

impl PostgresFieldExtractor for (&Felt, &VariantDef) {
    type Id = Felt;
    fn name(&self) -> &str {
        &self.1.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.1.type_def
    }
    fn id(&self) -> &Self::Id {
        self.0
    }
}

impl PostgresTypeExtractor for TypeDef {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType> {
        match self {
            TypeDef::None => Ok(PostgresScalar::None.into()),
            TypeDef::Bool => Ok(PostgresScalar::Boolean.into()),
            TypeDef::I8 | TypeDef::I16 => Ok(PostgresScalar::SmallInt.into()),
            TypeDef::I32 => Ok(PostgresScalar::Int.into()),
            TypeDef::I64 => Ok(PostgresScalar::BigInt.into()),
            TypeDef::U8 => Ok(PostgresScalar::Uint8.into()),
            TypeDef::U16 => Ok(PostgresScalar::Uint16.into()),
            TypeDef::U32 => Ok(PostgresScalar::Uint32.into()),
            TypeDef::U64 => Ok(PostgresScalar::Uint64.into()),
            TypeDef::U128 => Ok(PostgresScalar::Uint128.into()),
            TypeDef::I128 => Ok(PostgresScalar::Int128.into()),
            TypeDef::U256 => Ok(PostgresScalar::Uint256.into()),
            TypeDef::U512 => Ok(PostgresScalar::Uint512.into()),
            TypeDef::Felt252 => Ok(PostgresScalar::Felt252.into()),
            TypeDef::ContractAddress
            | TypeDef::ClassHash
            | TypeDef::StorageAddress
            | TypeDef::StorageBaseAddress => Ok(PostgresScalar::StarknetHash.into()),
            TypeDef::EthAddress => Ok(PostgresScalar::EthAddress.into()),
            TypeDef::Utf8String => Ok(PostgresScalar::Text.into()),
            TypeDef::ShortUtf8 => Ok(PostgresScalar::Char31.into()),
            TypeDef::ByteArray | TypeDef::ByteArrayEncoded(_) => Ok(PostgresScalar::Bytea.into()),
            TypeDef::Bytes31 | TypeDef::Bytes31Encoded(_) => Ok(PostgresScalar::Bytes31.into()),
            TypeDef::Tuple(type_defs) => type_defs.extract_type(schema, branch, creates),
            TypeDef::Enum(enum_def) => enum_def.extract_type(schema, branch, creates),
            TypeDef::Array(array_def) => array_def.extract_type(schema, branch, creates),
            TypeDef::FixedArray(fixed_array_def) => {
                fixed_array_def.extract_type(schema, branch, creates)
            }
            TypeDef::Struct(struct_def) => struct_def.extract_type(schema, branch, creates),
            TypeDef::Option(def) => def.type_def.extract_type(schema, branch, creates),
            TypeDef::Nullable(def) => def.type_def.extract_type(schema, branch, creates),
            TypeDef::Felt252Dict(_) | TypeDef::Result(_) | TypeDef::Ref(_) | TypeDef::Custom(_) => {
                Err(TypeError::UnsupportedType(format!("{self:?}")))
            }
        }
    }
}

impl From<&PrimaryDef> for PrimaryKey {
    fn from(primary: &PrimaryDef) -> Self {
        let pg_type = match primary.type_def {
            PrimaryTypeDef::Bool => PostgresScalar::Boolean,
            PrimaryTypeDef::I8 | PrimaryTypeDef::I16 => PostgresScalar::SmallInt,
            PrimaryTypeDef::I32 => PostgresScalar::Int,
            PrimaryTypeDef::I64 => PostgresScalar::BigInt,
            PrimaryTypeDef::U8 => PostgresScalar::Uint8,
            PrimaryTypeDef::U16 => PostgresScalar::Uint16,
            PrimaryTypeDef::U32 => PostgresScalar::Uint32,
            PrimaryTypeDef::U64 => PostgresScalar::Uint64,
            PrimaryTypeDef::U128 => PostgresScalar::Uint128,
            PrimaryTypeDef::I128 => PostgresScalar::Int128,
            PrimaryTypeDef::Felt252 => PostgresScalar::Felt252,
            PrimaryTypeDef::ContractAddress
            | PrimaryTypeDef::ClassHash
            | PrimaryTypeDef::StorageAddress
            | PrimaryTypeDef::StorageBaseAddress => PostgresScalar::StarknetHash,
            PrimaryTypeDef::EthAddress => PostgresScalar::EthAddress,
            PrimaryTypeDef::ShortUtf8 => PostgresScalar::Char31,
            PrimaryTypeDef::Bytes31 | PrimaryTypeDef::Bytes31Encoded(_) => PostgresScalar::Bytes31,
        };
        PrimaryKey {
            name: primary.name.clone(),
            pg_type,
        }
    }
}

impl PostgresTypeExtractor for ArrayDef {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType> {
        self.type_def
            .extract_type(schema, branch, creates)?
            .to_array(None)
    }
}

impl PostgresTypeExtractor for FixedArrayDef {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType> {
        self.type_def
            .extract_type(schema, branch, creates)?
            .to_array(Some(self.size))
    }
}

impl PostgresTypeExtractor for StructDef {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType> {
        let members = self
            .members
            .iter()
            .map(|f| f.extract_field(schema, branch, creates))
            .collect::<TypeResult<Vec<_>>>()?;
        let name = branch.type_name(&self.name);
        creates.push(CreatesType::new_struct(schema, &name, members).into());
        Ok(PostgresType::composite(schema, name))
    }
}

impl PostgresTypeExtractor for EnumDef {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType> {
        let name = branch.type_name(&self.name);
        let variants_type = branch.type_name(&format!("v_{}", self.name));
        let variant_names = self.variants.values().map(|v| v.name.clone()).collect_vec();
        creates.push(CreatesType::new_enum(schema, &variants_type, variant_names).into());
        let mut fields = Vec::with_capacity(self.variants.len() + 1);
        fields.push(PostgresField::new_composite(
            "_variant",
            schema,
            variants_type,
        ));
        for variant in &self.variants {
            match variant.1.type_def {
                TypeDef::None => {}
                _ => fields.push(variant.extract_field(schema, branch, creates)?),
            }
        }
        creates.push(CreatesType::new_struct(schema, &name, fields).into());
        Ok(PostgresType::composite(schema, name))
    }
}

impl PostgresTypeExtractor for TupleDef {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType> {
        let mut variants = Vec::with_capacity(self.elements.len());
        for (i, element) in self.elements.iter().enumerate() {
            variants.push(
                element
                    .extract_type(schema, &branch.branch(&(i as u32)), creates)?
                    .to_field(format!("_{}", i)),
            );
        }
        let name = branch.type_name("tuple");
        creates.push(CreatesType::new_struct(schema, &name, variants).into());
        Ok(PostgresType::composite(schema, name))
    }
}

impl PostgresTypeExtractor for OptionDef {
    fn extract_type(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        creates: &mut Vec<impl From<CreatesType>>,
    ) -> TypeResult<PostgresType> {
        self.type_def.extract_type(schema, branch, creates)
    }
}

impl CreatePgTable {
    pub fn new(
        schema: &Rc<str>,
        id: &Felt,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        append_only: bool,
    ) -> TypeResult<Self> {
        let mut creates: Vec<CreatesType> = Vec::new();
        let branch = Xxh3::new_based(id);
        let primary = primary.into();
        let columns = columns
            .iter()
            .map(|col| col.extract_field(schema, &branch, &mut creates))
            .collect::<TypeResult<Vec<_>>>()?;
        Ok(Self {
            name: SchemaName::new(schema, name),
            primary,
            columns,
            pg_types: creates,
            append_only,
        })
    }
    pub fn make_queries(&self, queries: &mut Vec<PgQuery>) {
        if !self.name.1.is_empty() {
            queries.add(make_schema_query(&self.name.0));
        }
        for pg_type in &self.pg_types {
            queries.add(pg_type.to_string());
        }
        queries.add(self.to_string());
        if !self.append_only {
            queries.add(format!(
                r#"CREATE TRIGGER set_timestamps BEFORE INSERT ON {} FOR EACH ROW EXECUTE FUNCTION introspect.set_default_timestamps();"#,
                self.name
            ));
        }
    }
}
