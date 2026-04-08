//! Sample events for testing SqlSink
//!
//! This module contains predefined blockchain events that demonstrate
//! the types of data SqlSink processes. These events are used by the
//! SampleExtractor to generate test data.
//! The decoder is where the events are decoded into envelopes based on the event content.

use starknet_types_raw::Felt;
use torii::etl::StarknetEvent;

const DUMMY_CONTRACT_ADDRESS: Felt =
    Felt::from_hex_unchecked("0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7");

/// Generate sample events for testing the SQL sink
///
/// These events demonstrate SQL operations (insert, update) using readable selector names.
/// Each event will be:
/// - Decoded by SqlDecoder based on the selector to create typed envelopes (SqlInsert, SqlUpdate)
/// - Processed by SqlSink and inserted into the events table
/// - Published to gRPC subscribers
///
/// Returns a vector of sample Starknet events.
pub fn generate_sample_events() -> Vec<StarknetEvent> {
    vec![
        StarknetEvent {
            from_address: DUMMY_CONTRACT_ADDRESS,
            keys: vec![
                Felt::selector("insert"),
                Felt::from_short_ascii_str_unchecked("user"),
            ],
            data: vec![Felt::from(100u64)],
            block_number: 0,
            transaction_hash: Felt::ZERO,
        },
        StarknetEvent {
            from_address: DUMMY_CONTRACT_ADDRESS,
            keys: vec![
                Felt::selector("update"),
                Felt::from_short_ascii_str_unchecked("user"),
            ],
            data: vec![Felt::from(200u64)],
            block_number: 0,
            transaction_hash: Felt::ZERO,
        },
        StarknetEvent {
            from_address: DUMMY_CONTRACT_ADDRESS,
            keys: vec![
                Felt::selector("insert"),
                Felt::from_short_ascii_str_unchecked("order"),
            ],
            data: vec![Felt::from(150u64)],
            block_number: 0,
            transaction_hash: Felt::ZERO,
        },
        StarknetEvent {
            from_address: DUMMY_CONTRACT_ADDRESS,
            keys: vec![
                Felt::selector("update"),
                Felt::from_short_ascii_str_unchecked("order"),
            ],
            data: vec![Felt::from(300u64)],
            block_number: 0,
            transaction_hash: Felt::ZERO,
        },
    ]
}
