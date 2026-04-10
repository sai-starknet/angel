use crate::types::SqlFelt;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::{Decode, Encode, Sqlite, Type};

impl Type<Sqlite> for SqlFelt {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <String as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for SqlFelt {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        let mut hex = String::with_capacity(66);
        hex.push_str("0x");
        for byte in &self.0 {
            use std::fmt::Write;
            write!(hex, "{byte:02x}").unwrap();
        }
        Encode::<Sqlite>::encode(hex, buf)
    }
}

impl Decode<'_, Sqlite> for SqlFelt {
    fn decode(value: <Sqlite as sqlx::Database>::ValueRef<'_>) -> Result<Self, BoxDynError> {
        let s = <String as Decode<Sqlite>>::decode(value)?;
        let s = s.strip_prefix("0x").unwrap_or(&s);
        if s.len() != 64 {
            return Err(format!("expected 64 hex chars for felt252, got {}", s.len()).into());
        }
        let mut arr = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = hex_nibble(chunk[0])?;
            let lo = hex_nibble(chunk[1])?;
            arr[i] = (hi << 4) | lo;
        }
        Ok(SqlFelt(arr))
    }
}

fn hex_nibble(c: u8) -> Result<u8, BoxDynError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("invalid hex char: {}", c as char).into()),
    }
}
