use primitive_types::U256 as PrimitiveU256;
use starknet::core::types::{Felt, U256};
use starknet_types_raw::Felt as RawFelt;

pub(crate) fn felt_to_blob(value: Felt) -> Vec<u8> {
    value.to_bytes_be().to_vec()
}

pub(crate) fn blob_to_felt(bytes: &[u8]) -> Felt {
    assert!(bytes.len() <= 32, "database stores canonical felt blobs");
    let mut padded = [0u8; 32];
    padded[32 - bytes.len()..].copy_from_slice(bytes);
    Felt::from_bytes_be(&padded)
}

pub(crate) fn u256_to_blob(value: U256) -> Vec<u8> {
    if value == U256::from(0u8) {
        return vec![0];
    }

    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(&value.high().to_be_bytes());
    bytes[16..].copy_from_slice(&value.low().to_be_bytes());
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(31);
    bytes[start..].to_vec()
}

pub(crate) fn blob_to_u256(bytes: &[u8]) -> U256 {
    let mut padded = [0u8; 32];
    let offset = 32usize.saturating_sub(bytes.len());
    padded[offset..].copy_from_slice(&bytes[bytes.len().saturating_sub(32)..]);
    let high = u128::from_be_bytes(padded[..16].try_into().unwrap());
    let low = u128::from_be_bytes(padded[16..].try_into().unwrap());
    U256::from_words(low, high)
}

pub(crate) fn u256_to_bytes(value: U256) -> Vec<u8> {
    u256_to_blob(value)
}

pub(crate) fn bytes_to_u256(bytes: &[u8]) -> U256 {
    blob_to_u256(bytes)
}

pub(crate) fn core_to_raw_felt(value: Felt) -> RawFelt {
    RawFelt::from_be_bytes(value.to_bytes_be()).unwrap()
}

pub(crate) fn core_to_primitive_u256(value: U256) -> PrimitiveU256 {
    PrimitiveU256::from_big_endian(&u256_to_bytes(value))
}

pub(crate) fn primitive_to_core_u256(value: PrimitiveU256) -> U256 {
    blob_to_u256(&value.to_big_endian())
}
