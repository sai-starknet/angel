use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
use sqlx::{Decode, Encode, Postgres, Type};

use crate::types::SqlFelt;

impl Type<Postgres> for SqlFelt {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("felt252")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        *ty == PgTypeInfo::with_name("felt252") || <[u8] as Type<Postgres>>::compatible(ty)
    }
}

impl PgHasArrayType for SqlFelt {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_felt252")
    }
}

impl Encode<'_, Postgres> for SqlFelt {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&[u8] as Encode<Postgres>>::encode(self.0.as_slice(), buf)
    }
}

impl Decode<'_, Postgres> for SqlFelt {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        let bytes = <&[u8] as Decode<Postgres>>::decode(value)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| format!("expected 32 bytes for felt252, got {}", bytes.len()))?;
        Ok(SqlFelt(arr))
    }
}
