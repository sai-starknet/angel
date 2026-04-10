//! Common utilities for Torii token indexers
//!
//! Provides efficient conversions between Starknet types and storage/wire formats,
//! and shared helpers like token metadata fetching.

pub mod json;
pub mod metadata;
pub mod token_uri;
pub mod utils;

pub use metadata::{MetadataFetcher, TokenMetadata};
use primitive_types::U256;
pub use token_uri::{
    process_token_uri_request, TokenStandard, TokenUriRequest, TokenUriResult, TokenUriSender,
    TokenUriService, TokenUriStore,
};

// ===== U256 conversions =====

/// Convert U256 to variable-length BLOB for storage (big-endian, compact)
///
/// Compression strategy:
/// - Zero value: 1 byte (0x00)
/// - Non-zero values: Trim leading zero bytes, store remaining bytes
pub fn u256_to_blob(value: U256) -> Vec<u8> {
    if value.is_zero() {
        return vec![0];
    }
    let bytes = value.to_big_endian();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(31);
    bytes[start..].to_vec()
}

/// Convert BLOB back to U256 (big-endian)
pub fn blob_to_u256(bytes: &[u8]) -> U256 {
    U256::from_big_endian(&bytes)
}

/// Alias for u256_to_blob (same format, different context name)
pub fn u256_to_bytes(value: U256) -> Vec<u8> {
    u256_to_blob(value)
}

/// Alias for blob_to_u256 (same format, different context name)
pub fn bytes_to_u256(bytes: &[u8]) -> U256 {
    blob_to_u256(bytes)
}
