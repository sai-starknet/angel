use starknet_types_raw::Felt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SqlFelt(pub [u8; 32]);

impl From<SqlFelt> for Felt {
    fn from(value: SqlFelt) -> Self {
        value.0.into()
    }
}

impl From<Felt> for SqlFelt {
    fn from(value: Felt) -> Self {
        SqlFelt(value.to_be_bytes())
    }
}

impl From<&Felt> for SqlFelt {
    fn from(value: &Felt) -> Self {
        SqlFelt(value.to_be_bytes())
    }
}
