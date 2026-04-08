use introspect_types::{
    EnumDef, FixedArrayDef, MemberDef, RefDef, ResultDef, StructDef, TypeDef, VariantDef,
};
use starknet_types_core::felt::Felt;

pub trait TypeLibrary {
    type Error;
    fn get_ref_type(&self, id: Felt) -> Result<TypeDef, Self::Error>;
    fn set_ref_type(&mut self, id: Felt, type_def: TypeDef) -> Result<(), Self::Error>;
    fn get_ref_expanded(&self, id: Felt) -> Result<TypeDef, Self::Error> {
        let mut type_def = self.get_ref_type(id)?;
        self.expand_type_in_place(&mut type_def)?;
        Ok(type_def)
    }
    fn expand_type(&self, type_def: &TypeDef) -> Result<TypeDef, Self::Error> {
        let mut type_def = type_def.clone();
        self.expand_type_in_place(&mut type_def)?;
        Ok(type_def)
    }

    fn expand_type_in_place(&self, type_def: &mut TypeDef) -> Result<(), Self::Error> {
        match type_def {
            TypeDef::Tuple(inner) => inner
                .elements
                .iter_mut()
                .try_for_each(|e| self.expand_type_in_place(e)),
            TypeDef::Array(inner) => self.expand_type_in_place(&mut inner.type_def),
            TypeDef::FixedArray(inner) => self.expand_type_in_place(&mut inner.type_def),
            TypeDef::Felt252Dict(inner) => self.expand_type_in_place(&mut inner.type_def),
            TypeDef::Struct(inner) => self.expand_struct_in_place(inner),
            TypeDef::Enum(inner) => self.expand_enum_in_place(inner),
            TypeDef::Ref(RefDef { id }) => self.get_ref_expanded(*id).map(|t| {
                *type_def = t;
            }),
            TypeDef::Option(inner) => self.expand_type_in_place(&mut inner.type_def),
            TypeDef::Result(inner) => self.expand_result_in_place(inner),
            TypeDef::Nullable(inner) => self.expand_type_in_place(&mut inner.type_def),
            TypeDef::None
            | TypeDef::Felt252
            | TypeDef::ShortUtf8
            | TypeDef::Bytes31
            | TypeDef::Bytes31Encoded(_)
            | TypeDef::Bool
            | TypeDef::U8
            | TypeDef::U16
            | TypeDef::U32
            | TypeDef::U64
            | TypeDef::U128
            | TypeDef::U256
            | TypeDef::U512
            | TypeDef::I8
            | TypeDef::I16
            | TypeDef::I32
            | TypeDef::I64
            | TypeDef::I128
            | TypeDef::ClassHash
            | TypeDef::ContractAddress
            | TypeDef::EthAddress
            | TypeDef::StorageAddress
            | TypeDef::StorageBaseAddress
            | TypeDef::ByteArray
            | TypeDef::Utf8String
            | TypeDef::ByteArrayEncoded(_)
            | TypeDef::Custom(_) => Ok(()),
        }
    }

    fn expand_boxed_type_in_place(&self, type_def: &mut Box<TypeDef>) -> Result<(), Self::Error> {
        self.expand_type_in_place(&mut *type_def)
    }
    fn expand_fixed_array_in_place(&self, fa: &mut FixedArrayDef) -> Result<(), Self::Error> {
        self.expand_type_in_place(&mut fa.type_def)
    }
    fn expand_struct_in_place(&self, s: &mut StructDef) -> Result<(), Self::Error> {
        s.members
            .iter_mut()
            .try_for_each(|member| self.expand_member_in_place(member))
    }
    fn expand_enum_in_place(&self, e: &mut EnumDef) -> Result<(), Self::Error> {
        e.variants
            .iter_mut()
            .try_for_each(|(_, field)| self.expand_variant_in_place(field))
    }

    fn expand_variant_in_place(&self, variant: &mut VariantDef) -> Result<(), Self::Error> {
        self.expand_type_in_place(&mut variant.type_def)
    }
    fn expand_member_in_place(&self, member: &mut MemberDef) -> Result<(), Self::Error> {
        self.expand_type_in_place(&mut member.type_def)
    }
    fn expand_result_in_place(&self, result: &mut ResultDef) -> Result<(), Self::Error> {
        self.expand_type_in_place(&mut result.ok)?;
        self.expand_type_in_place(&mut result.err)
    }
}
