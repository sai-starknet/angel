use super::create::PostgresTypeExtractor;
use super::query::{ColumnUpgrade, StructMod, StructMods, TableUpgrade};
use super::{HasherExt, PostgresScalar, PostgresType};
use crate::{
    DeadField, Table, TableResult, TypeError, TypeResult, UpgradeError, UpgradeResult,
    UpgradeResultExt,
};
use introspect_types::{
    ArrayDef, ColumnDef, EnumDef, FixedArrayDef, MemberDef, OptionDef, PrimaryDef, PrimaryTypeDef,
    ResultInto, StructDef, TupleDef, TypeDef, VariantDef,
};
use std::collections::HashMap;
use std::rc::Rc;
use xxhash_rust::xxh3::Xxh3;

pub trait PgTableUpgrade {
    fn upgrade_table(
        &mut self,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
    ) -> TableResult<TableUpgrade>;
    fn retype_primary(&mut self, new: &PrimaryTypeDef) -> UpgradeResult<Option<PostgresType>>;
}

impl PgTableUpgrade for Table {
    fn upgrade_table(
        &mut self,
        name: &str,
        primary: &PrimaryDef,
        columns: &[ColumnDef],
    ) -> TableResult<TableUpgrade> {
        let branch = Xxh3::new_based(&self.id);
        let schema: Rc<str> = self.namespace();
        let mut table_mod = TableUpgrade::new(&schema, self.id, self.name.clone());
        table_mod.rename_table(name);
        table_mod.rename_column(&mut self.primary.name, &primary.name);
        let pg_type = self
            .retype_primary(&primary.type_def)
            .to_table_result(&self.name, &self.primary.name)?;
        table_mod.retype_primary(&self.primary.name, pg_type);
        for column in columns {
            let branch = branch.branch(&column.id);
            if let Some(current) = self.columns.get_mut(&column.id) {
                let upgraded = table_mod.rename_column(&mut current.name, &column.name);
                let mut column_upgrade = table_mod.column_upgrade(upgraded);
                let pg_type = current
                    .type_def
                    .compare_type(
                        &schema,
                        &branch,
                        &column.type_def,
                        &mut self.dead,
                        &mut column_upgrade,
                    )
                    .to_table_result(&self.name, &current.name)?;
                let type_name = current.type_def.get_pg_type(&schema, &branch)?;
                table_mod.retype_column(column, pg_type, column_upgrade, type_name);
            } else {
                let (column_id, info) = column.clone().into();
                self.columns.insert(column_id, info);
                let pg_type =
                    column
                        .type_def
                        .extract_type(&schema, &branch, &mut table_mod.atomic)?;
                table_mod.add_column(column_id, &column.name, pg_type);
            }
        }
        Ok(table_mod)
    }
    fn retype_primary(&mut self, new: &PrimaryTypeDef) -> UpgradeResult<Option<PostgresType>> {
        use super::PostgresScalar::{
            BigInt, Felt252 as PgFelt252, Int, Int128, SmallInt, Uint128, Uint16, Uint32, Uint64,
            Uint8,
        };
        use introspect_types::PrimaryTypeDef::{
            Bool as PBool, Bytes31 as PBytes31, Bytes31Encoded as PBytes31Encoded,
            ClassHash as PClassHash, ContractAddress as PContractAddress,
            EthAddress as PEthAddress, Felt252 as PFelt252, ShortUtf8 as PShortUtf8,
            StorageAddress as PStorageAddress, StorageBaseAddress as PStorageBaseAddress,
            I128 as PI128, I16 as PI16, I32 as PI32, I64 as PI64, I8 as PI8, U128 as PU128,
            U16 as PU16, U32 as PU32, U64 as PU64, U8 as PU8,
        };
        use introspect_types::TypeDef::{
            Bool, Bytes31, Bytes31Encoded, ClassHash, ContractAddress, EthAddress, Felt252,
            ShortUtf8, StorageAddress, StorageBaseAddress, I128, I16, I32, I64, I8, U128, U16, U32,
            U64, U8,
        };
        match (&self.primary.type_def, new) {
            (Bool, PBool)
            | (U8, PU8)
            | (U16, PU16)
            | (U32, PU32)
            | (U64, PU64)
            | (U128, PU128)
            | (I8, PI8)
            | (I16, PI16)
            | (I32, PI32)
            | (I64, PI64)
            | (I128, PI128)
            | (ShortUtf8, PShortUtf8)
            | (EthAddress, PEthAddress)
            | (ClassHash, PClassHash)
            | (ContractAddress, PContractAddress)
            | (StorageAddress, PStorageAddress)
            | (StorageBaseAddress, PStorageBaseAddress)
            | (Bytes31, PBytes31)
            | (Bytes31Encoded(_), PBytes31Encoded(_))
            | (Felt252, PFelt252) => Ok(None),
            (Bool, PU8) => self.primary.type_def.update_as(U8, Uint8),
            (Bool | U8, PU16) => self.primary.type_def.update_as(U16, Uint16),
            (Bool | U8 | U16, PU32) => self.primary.type_def.update_as(U32, Uint32),
            (Bool | U8 | U16 | U32, PU64) => self.primary.type_def.update_as(U64, Uint64),
            (Bool | U8 | U16 | U32 | U64, PU128) => self.primary.type_def.update_as(U128, Uint128),
            (Bool | U8, PI8) => self.primary.type_def.update_as(I8, SmallInt),
            (Bool | U8 | I8, PI16) => self.primary.type_def.update_as(I16, SmallInt),
            (Bool | U8 | U16 | I8 | I16, PI32) => self.primary.type_def.update_as(I32, Int),
            (Bool | U8 | U16 | U32 | I8 | I16 | I32, PI64) => {
                self.primary.type_def.update_as(I64, BigInt)
            }
            (Bool | U8 | U16 | U32 | U64 | I8 | I16 | I32 | I64, PI128) => {
                self.primary.type_def.update_as(I128, Int128)
            }
            (
                EthAddress,
                PClassHash | PContractAddress | PStorageAddress | PStorageBaseAddress | PFelt252,
            ) => self.primary.type_def.update_to(&new.into(), PgFelt252),
            (
                ClassHash | ContractAddress | StorageAddress | StorageBaseAddress | Felt252,
                PClassHash | PContractAddress | PStorageAddress | PStorageBaseAddress | PFelt252,
            ) => self.primary.type_def.update_no_change(&new.into()),
            _ => UpgradeError::primary_upgrade_err(&self.primary.type_def, new),
        }
    }
}

pub trait ExtractItem {
    fn as_struct(&mut self) -> UpgradeResult<&mut StructDef>;
    fn as_enum(&mut self) -> UpgradeResult<&mut EnumDef>;
    fn as_fixed_array(&mut self) -> UpgradeResult<&mut FixedArrayDef>;
    fn as_array(&mut self) -> UpgradeResult<&mut TypeDef>;
    fn as_tuple(&mut self) -> UpgradeResult<&mut TupleDef>;
    fn update_as_array(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &ArrayDef,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>>;
    fn update_as_option(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &TypeDef,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>>;
    fn get_pg_type(&self, schema: &Rc<str>, branch: &Xxh3) -> TypeResult<PostgresType>;
}

impl ExtractItem for TypeDef {
    fn as_struct(&mut self) -> UpgradeResult<&mut StructDef> {
        match self {
            TypeDef::Struct(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "Struct"),
        }
    }
    fn as_enum(&mut self) -> UpgradeResult<&mut EnumDef> {
        match self {
            TypeDef::Enum(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "Enum"),
        }
    }
    fn as_fixed_array(&mut self) -> UpgradeResult<&mut FixedArrayDef> {
        match self {
            TypeDef::FixedArray(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "FixedArray"),
        }
    }
    fn as_array(&mut self) -> UpgradeResult<&mut TypeDef> {
        match self {
            TypeDef::Array(def) => Ok(&mut def.type_def),
            TypeDef::FixedArray(def) => Ok(&mut def.type_def),
            item => UpgradeError::type_upgrade_to_err(item, "Array"),
        }
    }
    fn as_tuple(&mut self) -> UpgradeResult<&mut TupleDef> {
        match self {
            TypeDef::Tuple(def) => Ok(def),
            item => UpgradeError::type_upgrade_to_err(item, "Tuple"),
        }
    }

    fn update_as_array(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &ArrayDef,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>> {
        self.as_array()?
            .compare_type(schema, branch, new, dead, queries)?
            .map(|a| a.to_array(None))
            .transpose()
            .err_into()
    }
    fn update_as_option(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &TypeDef,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>> {
        match self {
            TypeDef::Option(def) => def.compare_type(schema, branch, new, dead, queries),
            _ => {
                let pg_type = self.compare_type(schema, branch, new, dead, queries);
                *self = OptionDef::new_type_def(std::mem::take(self));
                pg_type
            }
        }
    }
    fn get_pg_type(&self, schema: &Rc<str>, branch: &Xxh3) -> TypeResult<PostgresType> {
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
            TypeDef::Tuple(_) => Ok(PostgresType::composite(schema, branch.tuple_name())),
            TypeDef::Enum(def) => Ok(PostgresType::composite(schema, branch.type_name(&def.name))),
            TypeDef::Array(def) => def.get_pg_type(schema, branch)?.to_array(None),
            TypeDef::FixedArray(def) => def
                .type_def
                .get_pg_type(schema, branch)?
                .to_array(Some(def.size)),
            TypeDef::Struct(def) => {
                Ok(PostgresType::composite(schema, branch.type_name(&def.name)))
            }
            TypeDef::Option(def) => def.get_pg_type(schema, branch),
            TypeDef::Nullable(def) => def.get_pg_type(schema, branch),
            TypeDef::Felt252Dict(_) | TypeDef::Result(_) | TypeDef::Ref(_) | TypeDef::Custom(_) => {
                Err(TypeError::UnsupportedType(format!("{self:?}")))
            }
        }
    }
}

pub trait CompareType {
    fn compare_type(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>>;
}

pub trait UpdateType: Clone {
    fn update_no_change(&mut self, new: &Self) -> UpgradeResult<Option<PostgresType>> {
        *self = new.clone();
        Ok(None)
    }
    fn update_to(
        &mut self,
        def: &Self,
        pg: impl Into<PostgresType>,
    ) -> UpgradeResult<Option<PostgresType>> {
        *self = def.clone();
        Ok(Some(pg.into()))
    }
    fn update_as(
        &mut self,
        def: Self,
        pg: impl Into<PostgresType>,
    ) -> UpgradeResult<Option<PostgresType>> {
        *self = def;
        Ok(Some(pg.into()))
    }
}

impl UpdateType for TypeDef {}
impl UpdateType for PrimaryTypeDef {}

pub trait UpgradeField {
    fn type_def(&self) -> &TypeDef;
    fn type_def_mut(&mut self) -> &mut TypeDef;
    fn name(&self) -> &str;
    fn upgrade_field(
        &mut self,
        schema: &Rc<str>,
        name: &str,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<()> {
        let pg_type =
            self.type_def_mut()
                .compare_type(schema, branch, new.type_def(), dead, queries)?;
        queries.maybe_alter(schema, name, self.name(), pg_type);
        Ok(())
    }
    fn add_field(
        &self,
        schema: &Rc<str>,
        branch: &Xxh3,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<StructMod> {
        let pg_type = self
            .type_def()
            .extract_type(schema, branch, &mut queries.atomic)?;
        Ok(StructMod::add(self.name(), pg_type))
    }
}

impl UpgradeField for MemberDef {
    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn type_def_mut(&mut self) -> &mut TypeDef {
        &mut self.type_def
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl UpgradeField for VariantDef {
    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
    fn type_def_mut(&mut self) -> &mut TypeDef {
        &mut self.type_def
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl CompareType for TypeDef {
    fn compare_type(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &TypeDef,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>> {
        use introspect_types::TypeDef::{
            Array, Bool, ByteArray, ByteArrayEncoded, Bytes31, Bytes31Encoded, ClassHash,
            ContractAddress, Enum, EthAddress, Felt252, FixedArray, None as TDNone, ShortUtf8,
            StorageAddress, StorageBaseAddress, Struct, Tuple, Utf8String, I128, I16, I32, I64, I8,
            U128, U16, U256, U32, U512, U64, U8,
        };
        use PostgresScalar::{
            BigInt, Felt252 as PgFelt252, Int, Int128, SmallInt, Uint128, Uint16, Uint256, Uint32,
            Uint512, Uint64, Uint8,
        };
        match (&*self, new) {
            (TDNone, TDNone)
            | (Utf8String, Utf8String)
            | (ShortUtf8, ShortUtf8)
            | (Bool, Bool)
            | (U8, U8)
            | (U16, U16)
            | (U32, U32)
            | (U64, U64)
            | (U128, U128)
            | (U256, U256)
            | (U512, U512)
            | (I8, I8)
            | (I16, I16)
            | (I32, I32)
            | (I64, I64)
            | (I128, I128)
            | (Felt252, Felt252)
            | (EthAddress, EthAddress)
            | (ClassHash, ClassHash)
            | (ContractAddress, ContractAddress)
            | (StorageAddress, StorageAddress)
            | (StorageBaseAddress, StorageBaseAddress)
            | (Bytes31, Bytes31)
            | (ByteArray, ByteArray) => Ok(None),
            (I8, I16)
            | (Bytes31, Bytes31Encoded(_))
            | (ByteArray, ByteArrayEncoded(_))
            | (
                ClassHash | ContractAddress | StorageAddress | StorageBaseAddress,
                ClassHash | ContractAddress | StorageAddress | StorageBaseAddress,
            ) => self.update_no_change(new),
            (Bool, U8) => self.update_as(U8, Uint8),
            (Bool | U8, U16) => self.update_as(U16, Uint16),
            (Bool | U8 | U16, U32) => self.update_as(U32, Uint32),
            (Bool | U8 | U16 | U32, U64) => self.update_as(U64, Uint64),
            (Bool | U8 | U16 | U32 | U64, U128) => self.update_as(U128, Uint128),
            (Bool | U8 | U16 | U32 | U64 | U128, U256) => self.update_as(U256, Uint256),
            (Bool | U8 | U16 | U32 | U64 | U128 | U256, U512) => self.update_as(U512, Uint512),
            (Bool | U8, I8) => self.update_as(I8, SmallInt),
            (Bool | U8, I16) => self.update_as(I16, SmallInt),
            (Bool | U8 | U16 | I8 | I16, I32) => self.update_as(I32, Int),
            (Bool | U8 | U16 | U32 | I8 | I16 | I32, I64) => self.update_as(I64, BigInt),
            (Bool | U8 | U16 | U32 | U64 | I8 | I16 | I32 | I64, I128) => {
                self.update_as(I128, Int128)
            }
            (ClassHash | ContractAddress | StorageAddress | StorageBaseAddress, Felt252) => {
                self.update_as(Felt252, PgFelt252)
            }
            (_, Struct(new_def)) => self
                .as_struct()?
                .compare_type(schema, branch, new_def, dead, queries),
            (_, Enum(new_def)) => self
                .as_enum()?
                .compare_type(schema, branch, new_def, dead, queries),
            (_, FixedArray(new_def)) => self
                .as_fixed_array()?
                .compare_type(schema, branch, new_def, dead, queries),
            (_, Array(new_def)) => self.update_as_array(schema, branch, new_def, dead, queries),
            (_, Tuple(new_def)) => self
                .as_tuple()?
                .compare_type(schema, branch, new_def, dead, queries),
            (_, TypeDef::Option(new_def)) => {
                self.update_as_option(schema, branch, new_def, dead, queries)
            }
            _ => UpgradeError::type_upgrade_err(self, new),
        }
    }
}

impl CompareType for StructDef {
    fn compare_type(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &StructDef,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>> {
        let mut mods: Vec<StructMod> = Vec::new();
        let mut current_map: HashMap<String, MemberDef> = std::mem::take(&mut self.members)
            .into_iter()
            .map(|m| (m.name.clone(), m))
            .collect();
        let struct_name = branch.type_name(&self.name);
        for member in &new.members {
            let branch = branch.branch(&member.name);
            if let Some(mut current) = current_map
                .remove(&member.name)
                .or_else(|| dead.remove(&branch.digest128()).map(Into::into))
            {
                current.upgrade_field(schema, &struct_name, &branch, member, dead, queries)?;
                self.members.push(current);
            } else {
                mods.push(member.add_field(schema, &branch, queries)?);
                self.members.push(member.clone());
            }
        }
        for (_, dead_member) in current_map.drain() {
            let id = branch.branch(&dead_member.name).digest128();
            queries.add_dead_member(id, &dead_member);
            dead.insert(id, dead_member.into());
        }
        queries.add_struct_mod(schema, struct_name, mods);
        Ok(None)
    }
}

impl CompareType for EnumDef {
    fn compare_type(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &EnumDef,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>> {
        let mut rename = Vec::new();
        let mut add = Vec::new();
        let mut mods = Vec::new();
        let enum_name = branch.type_name(&self.name);
        for (id, variant) in &new.variants {
            let branch = branch.branch(id);
            if let Some(current) = self.variants.get_mut(id) {
                if current.name != variant.name {
                    rename.push((current.name.clone(), variant.name.clone()));
                    mods.rename(&current.name, &variant.name);
                    current.name = variant.name.clone();
                }
                current.upgrade_field(schema, &enum_name, &branch, variant, dead, queries)?;
            } else {
                add.push(variant.name.clone());
                variant.add_field(schema, &branch, queries)?;
                self.variants.insert(*id, variant.clone());
                self.order.push(*id);
            }
        }
        queries.add_enum_mod(
            schema,
            branch.type_name(&format!("v_{}", self.name)),
            rename,
            add,
        );
        queries.add_struct_mod(schema, enum_name, mods);
        Ok(None)
    }
}

impl CompareType for FixedArrayDef {
    fn compare_type(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>> {
        let pg_type = self
            .type_def
            .compare_type(schema, branch, &new.type_def, dead, queries)?;
        if self.size > new.size {
            return UpgradeError::array_shorten_err(self.size, new.size);
        }
        self.size = new.size;
        pg_type
            .map(|pg_type| pg_type.to_array(Some(self.size)))
            .transpose()
            .err_into()
    }
}

impl CompareType for TupleDef {
    fn compare_type(
        &mut self,
        schema: &Rc<str>,
        branch: &Xxh3,
        new: &Self,
        dead: &mut HashMap<u128, DeadField>,
        queries: &mut ColumnUpgrade,
    ) -> UpgradeResult<Option<PostgresType>> {
        let mut mods = Vec::new();
        let name = branch.tuple_name();
        if self.elements.len() > new.elements.len() {
            return Err(UpgradeError::TupleReductionError);
        }
        for (n, ty) in new.elements.iter().enumerate() {
            let branch = branch.branch(&(n as u32));
            if let Some(current) = self.elements.get_mut(n) {
                let pg_type = current.compare_type(schema, &branch, ty, dead, queries)?;
                queries.maybe_alter(schema, &name, &format!("_{}", n), pg_type);
            } else {
                self.elements.push(ty.clone());
                mods.add(
                    format!("_{}", n),
                    ty.extract_type(schema, &branch, &mut queries.atomic)?,
                );
            }
        }
        queries.add_struct_mod(schema, name, mods);
        Ok(None)
    }
}
