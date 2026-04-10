pub mod context;

use super::envelope::Envelope;
use async_trait::async_trait;
use starknet_types_raw::Felt;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use torii_types::event::{EventContext, StarknetEvent};
use xxhash_rust::const_xxh3::xxh3_64;

pub use context::DecoderContext;

/// Decoder transforms blockchain events into typed envelopes
///
/// # Design
/// Decoders are responsible for:
/// - Examining raw blockchain events.
/// - Filtering events they're interested in (by contract address, event keys, etc.).
/// - Creating typed `Envelope` wrappers with specific `TypeId`s.
/// - **Populating envelope metadata** with event-specific data for sink access.
/// - Skipping events they don't recognize.
///
/// # Multi-Decoder Pattern
/// Multiple decoders can process the same events:
/// - One event may produce multiple envelopes (different sinks).
/// - One event may be skipped by all decoders (no envelopes).
/// - Each decoder is typically associated with a specific sink.
///
/// # Metadata Best Practice
///
/// **Important**: If sinks need access to original event data (like block number, transaction hash,
/// contract address, etc.), the decoder should add this information to the envelope's **metadata** (or body if it's relevant).
///
/// Why? Sinks should avoid iterating through `batch.events` (O(n) operation). Instead:
/// - Decoder extracts relevant event fields → envelope metadata
/// - Sink reads metadata → O(1) access
///
/// For block/transaction context, sinks can use the enriched batch HashMaps:
/// - `batch.blocks[&block_number]` - O(1) lookup for block timestamp, hash, etc.
/// - `batch.transactions[&tx_hash]` - O(1) lookup for sender, calldata, etc.
///
/// # Example
///
/// ```rust,ignore
/// use crate::etl::decoder::Decoder;
/// use crate::etl::envelope::{Envelope, TypeId, TypedBody};
/// use crate::etl::decoder::StarknetEvent;
/// use async_trait::async_trait;
/// use std::collections::HashMap;
/// use starknet_types_raw::Felt;
///
/// pub struct MyDecoder {
///     contract_filters: Vec<Felt>,
/// }
///
/// impl MyDecoder {
///     fn is_interested(&self, event: &StarknetEvent) -> bool {
///         // Filter logic here
///         true
///     }
/// }
///
/// #[async_trait]
/// impl Decoder for MyDecoder {
///     fn decoder_name(&self) -> &str {
///         "my_decoder"
///     }
///
///     async fn decode_event(&self, event: &StarknetEvent) -> anyhow::Result<Vec<Envelope>> {
///         if !self.is_interested(event) {
///             return Ok(Vec::new());
///         }
///
///         // Extract only the data you need from the event
///         let body = MyEventType { /* decoded fields */ };
///
///         // Add event-specific data to metadata for sink access
///         let mut metadata = HashMap::new();
///         metadata.insert("block_number".to_string(),
///             event.block_number.unwrap_or(0).to_string());
///         metadata.insert("from_address".to_string(),
///             format!("{:#x}", event.from_address));
///
///         Ok(vec![Envelope::new("my_key", Box::new(body), metadata)])
///     }
/// }
/// ```
///
/// # Performance
///
/// **Zero-copy filtering**: Decoders receive `&StarknetEvent` (reference), allowing:
/// - Process events without cloning.
/// - Only extract data for events the decoder is interested in.
/// - Multiple decoders process the same events without memory duplication.
#[async_trait]
pub trait Decoder: Send + Sync {
    /// Returns the unique name of this decoder
    ///
    /// This name is used to generate a deterministic `DecoderId` that identifies
    /// this decoder in the contract registry. The name should be:
    /// - Unique across all decoders in the system
    /// - Stable (never change it, or contract mappings will break)
    /// - Lowercase and descriptive (e.g., "erc20", "erc721", "custom_game_events")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// fn decoder_name(&self) -> &str {
    ///     "erc20" // DecoderId will be hash("erc20")
    /// }
    /// ```
    fn decoder_name(&self) -> &str;

    /// Decode event data into typed envelopes
    ///
    /// This is the primary method that decoders should implement.
    /// Returns an empty Vec if the decoder is not interested in this event.
    ///
    /// # Arguments
    /// * `keys` - Event keys (selectors and other identifiers).
    /// * `data` - Event data fields.
    /// * `context` -  from_address, block_number, transaction_hash
    ///
    /// # Returns
    /// Vector of envelopes produced from this event (empty if not interested).
    async fn decode(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> anyhow::Result<Vec<Envelope>>;

    /// Decode a single event into envelopes (convenience method)
    ///
    /// Default implementation calls `decode()` with event fields.
    /// Override only if you want to change the input type or add pre-processing.
    ///
    /// # Arguments
    /// * `event` - Reference to the event to decode.
    ///
    /// # Returns
    /// Vector of envelopes produced from this event (empty if not interested).

    async fn decode_event(&self, event: &StarknetEvent) -> anyhow::Result<Vec<Envelope>> {
        self.decode(&event.keys, &event.data, event.context()).await
    }

    /// Decode multiple events into typed envelopes (convenience method)
    ///
    /// Default implementation calls `decode_event` for each event.
    /// Override only if batch processing provides meaningful optimization.
    ///
    /// # Arguments
    /// * `events` - Reference to event slice.
    ///
    /// # Returns
    /// Vector of all envelopes produced from all events.
    async fn decode_events(&self, events: &[StarknetEvent]) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();
        for event in events {
            all_envelopes.extend(self.decode_event(event).await?);
        }
        Ok(all_envelopes)
    }
}

/// Decoder identifier based on decoder name hash
///
/// Similar to `EnvelopeTypeId`, this uses a hash of the decoder's name
/// to create a unique, deterministic identifier. This ensures that decoder
/// IDs remain consistent across restarts and don't depend on registration order.
///
/// # Example
///
/// ```rust,ignore
/// const ERC20_DECODER_ID = DecoderId::new("erc20");
/// const ERC721_DECODER_ID = DecoderId::new("erc721");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DecoderId(u64);

impl DecoderId {
    /// Creates a DecoderId from a decoder name (deterministic)
    pub const fn new(type_name: &str) -> Self {
        Self(xxh3_64(type_name.as_bytes()))
    }

    /// Creates a DecoderId from a u64 value (for deserialization)
    pub fn from_u64(value: u64) -> Self {
        DecoderId(value)
    }

    /// Returns the DecoderId as a u64
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Contract filtering strategy (explicit mappings + blacklist)
///
/// Provides two mechanisms for controlling decoder execution:
///
/// 1. **Explicit mappings** (contract → Vec<DecoderId>):
///    - Most efficient: O(k) where k = number of mapped decoders
///    - Use for known contracts where you know exactly which decoders apply
///    - Example: USDC contract → [ERC20 decoder]
///
/// 2. **Blacklist** (HashSet<Felt>):
///    - Fast discard: O(1) lookup before any decoder logic
///    - Use for noisy contracts that emit many irrelevant events
///    - Can coexist with mappings (but contract can't be in both)
///
/// For unmapped contracts (not in mappings or blacklist), the behavior depends on
/// whether a `ContractRegistry` is configured:
/// - With registry: Auto-identification via ABI inspection
/// - Without registry: Events tried against all decoders
///
/// # Validation
///
/// The `validate()` method ensures no contract appears in both mappings and blacklist.
///
/// # Example
///
/// ```rust,ignore
/// use crate::etl::decoder::{ContractFilter, DecoderId};
/// use starknet::core::types::Felt;
///
/// let usdc = Felt::from_hex("0x123...").unwrap();
/// let noisy = Felt::from_hex("0xabc...").unwrap();
/// let erc20_id = DecoderId::new("erc20");
///
/// let filter = ContractFilter::new()
///     .map_contract(usdc, vec![erc20_id])  // Explicit mapping
///     .blacklist_contract(noisy);          // Blacklist noisy contract
/// ```
#[derive(Debug, Clone, Default)]
pub struct ContractFilter {
    /// Explicit mappings: contract → list of decoder IDs
    pub mappings: HashMap<Felt, Vec<DecoderId>>,

    /// Blacklist: contracts to ignore entirely
    pub blacklist: HashSet<Felt>,
}

impl ContractFilter {
    /// Create empty filter (process all contracts with all decoders)
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a contract should be processed
    ///
    /// # Returns
    /// - `true` if the contract is not blacklisted
    /// - `false` if the contract is explicitly blacklisted
    pub fn allows(&self, contract: Felt) -> bool {
        !self.blacklist.contains(&contract)
    }

    /// Get decoders for a contract
    ///
    /// # Returns
    /// - `Some(&Vec<DecoderId>)` if explicit mapping exists (use ONLY these decoders)
    /// - `None` if no mapping exists (try ALL decoders - auto-discovery)
    pub fn get_decoders(&self, contract: Felt) -> Option<&Vec<DecoderId>> {
        self.mappings.get(&contract)
    }

    /// Validate configuration (no contract in both mapping and blacklist)
    pub fn validate(&self) -> anyhow::Result<()> {
        for addr in self.mappings.keys() {
            if self.blacklist.contains(addr) {
                anyhow::bail!("Contract {addr:#x} appears in both mapping and blacklist");
            }
        }
        Ok(())
    }

    /// Add explicit mapping: contract → decoders
    pub fn map_contract(mut self, contract: Felt, decoder_ids: Vec<DecoderId>) -> Self {
        self.mappings.insert(contract, decoder_ids);
        self
    }

    /// Add contract to blacklist
    pub fn blacklist_contract(mut self, contract: Felt) -> Self {
        self.blacklist.insert(contract);
        self
    }

    /// Add multiple contracts to blacklist
    pub fn blacklist_contracts(mut self, contracts: Vec<Felt>) -> Self {
        self.blacklist.extend(contracts);
        self
    }
}
