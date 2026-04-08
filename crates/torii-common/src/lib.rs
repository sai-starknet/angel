//! Common utilities for Torii token indexers
//!
//! Provides efficient conversions between Starknet types and storage/wire formats,
//! and shared helpers like token metadata fetching.

pub mod json;
pub mod metadata;
pub mod token_uri;
pub mod utils;

pub use metadata::{MetadataFetcher, TokenMetadata};
use primitive_types::U256 as PrimitiveU256;
use starknet::core::types::{Felt as CoreFelt, U256 as CoreU256};
use starknet_types_raw::Felt as RawFelt;
pub use token_uri::{
    process_token_uri_request, TokenStandard, TokenUriRequest, TokenUriResult, TokenUriSender,
    TokenUriService, TokenUriStore,
};

// ===== U256 conversions =====

pub trait U256Blob: Sized {
    fn to_blob_bytes(self) -> Vec<u8>;
    fn from_blob_bytes(bytes: &[u8]) -> Self;
}

impl U256Blob for PrimitiveU256 {
    fn to_blob_bytes(self) -> Vec<u8> {
        if self.is_zero() {
            return vec![0];
        }

        let bytes = self.to_big_endian();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(31);
        bytes[start..].to_vec()
    }

    fn from_blob_bytes(bytes: &[u8]) -> Self {
        Self::from_big_endian(bytes)
    }
}

impl U256Blob for CoreU256 {
    fn to_blob_bytes(self) -> Vec<u8> {
        if self == Self::from(0u8) {
            return vec![0];
        }

        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(&self.high().to_be_bytes());
        bytes[16..].copy_from_slice(&self.low().to_be_bytes());
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(31);
        bytes[start..].to_vec()
    }

    fn from_blob_bytes(bytes: &[u8]) -> Self {
        let mut padded = [0u8; 32];
        let source = if bytes.len() > 32 {
            &bytes[bytes.len() - 32..]
        } else {
            bytes
        };
        let offset = 32 - source.len();
        padded[offset..].copy_from_slice(source);
        let high = u128::from_be_bytes(padded[..16].try_into().unwrap());
        let low = u128::from_be_bytes(padded[16..].try_into().unwrap());
        Self::from_words(low, high)
    }
}

/// Convert U256 to variable-length BLOB for storage (big-endian, compact)
pub fn u256_to_blob<U>(value: U) -> Vec<u8>
where
    U: U256Blob,
{
    value.to_blob_bytes()
}

/// Convert BLOB back to U256 (big-endian)
pub fn blob_to_u256<U>(bytes: &[u8]) -> U
where
    U: U256Blob,
{
    U::from_blob_bytes(bytes)
}

/// Alias for u256_to_blob (same format, different context name)
pub fn u256_to_bytes<U>(value: U) -> Vec<u8>
where
    U: U256Blob,
{
    u256_to_blob(value)
}

/// Alias for blob_to_u256 (same format, different context name)
pub fn bytes_to_u256<U>(bytes: &[u8]) -> U
where
    U: U256Blob,
{
    blob_to_u256(bytes)
}

pub trait FeltBlob: Sized {
    fn to_blob_bytes(self) -> Vec<u8>;
    fn from_blob_bytes(bytes: &[u8]) -> Option<Self>;
}

impl FeltBlob for RawFelt {
    fn to_blob_bytes(self) -> Vec<u8> {
        self.to_be_bytes_vec()
    }

    fn from_blob_bytes(bytes: &[u8]) -> Option<Self> {
        Self::from_be_bytes_slice(bytes).ok()
    }
}

impl FeltBlob for CoreFelt {
    fn to_blob_bytes(self) -> Vec<u8> {
        self.to_bytes_be().to_vec()
    }

    fn from_blob_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > 32 {
            return None;
        }

        let mut padded = [0u8; 32];
        padded[32 - bytes.len()..].copy_from_slice(bytes);
        Some(Self::from_bytes_be(&padded))
    }
}

pub fn felt_to_blob<F>(value: F) -> Vec<u8>
where
    F: FeltBlob,
{
    value.to_blob_bytes()
}

pub fn bytes_to_felt<F>(bytes: &[u8]) -> Option<F>
where
    F: FeltBlob,
{
    F::from_blob_bytes(bytes)
}

pub fn blob_to_felt<F>(bytes: &[u8]) -> F
where
    F: FeltBlob,
{
    bytes_to_felt(bytes).expect("database stores canonical felt blobs")
}
