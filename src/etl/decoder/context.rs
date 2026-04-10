//! DecoderContext manages multiple decoders with contract filtering.
//!
//! The DecoderContext routes events to all registered decoders, optionally
//! filtering by contract address (whitelist/blacklist).
//!
//! # Design
//!
//! - Decoders are identified by their `decoder_name()` (hashed to DecoderId)
//! - Explicit contract mappings take highest priority
//! - Registry mappings (from auto-identification) take second priority
//! - Unmapped contracts with no registry fall back to all decoders
//! - Deterministic ordering: decoders are always called in sorted DecoderId order

use super::{ContractFilter, Decoder, DecoderId};
use crate::etl::decoder::EventContext;
use crate::etl::engine_db::EngineDb;
use crate::etl::envelope::Envelope;
use async_trait::async_trait;
use starknet_types_raw::Felt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// DecoderContext manages multiple decoders with contract filtering.
///
/// Routes events to decoders based on:
/// 1. Explicit mappings (highest priority, from ContractFilter)
/// 2. Registry mappings (from ContractRegistry auto-identification)
/// 3. All decoders (fallback when no registry is configured)
///
#[allow(dead_code)]
pub struct DecoderContext {
    /// Decoders indexed by their ID (hash of name)
    decoders: HashMap<DecoderId, Arc<dyn Decoder>>,

    /// EngineDb for ETL state persistence (cursor, not contract mappings)
    engine_db: Arc<EngineDb>,

    /// Contract filter (explicit mappings + blacklist)
    contract_filter: ContractFilter,

    /// Cached mappings from ContractRegistry (populated externally via batch identification)
    /// Key: contract address, Value: list of decoder IDs
    /// Empty Vec means "identified but no decoders match"
    /// None value means "not yet identified" (will try all decoders)
    registry_cache: Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,

    /// Whether a registry is configured (affects fallback behavior)
    has_registry: bool,
}

impl DecoderContext {
    /// Create a new DecoderContext
    ///
    /// # Arguments
    ///
    /// * `decoders` - List of decoders to manage
    /// * `engine_db` - Database for ETL state persistence
    /// * `contract_filter` - Explicit mappings + blacklist
    pub fn new(
        decoders: Vec<Arc<dyn Decoder>>,
        engine_db: Arc<EngineDb>,
        contract_filter: ContractFilter,
    ) -> Self {
        let decoder_map = Self::build_decoder_map(&decoders);

        let filter_desc =
            if contract_filter.mappings.is_empty() && contract_filter.blacklist.is_empty() {
                "none (will try all decoders for all contracts)".to_string()
            } else {
                let mut parts = Vec::new();
                if !contract_filter.mappings.is_empty() {
                    parts.push(format!(
                        "{} explicit mappings",
                        contract_filter.mappings.len()
                    ));
                }
                if !contract_filter.blacklist.is_empty() {
                    parts.push(format!("{} blacklisted", contract_filter.blacklist.len()));
                }
                parts.join(", ")
            };

        tracing::info!(
            target: "torii::etl::decoder_context",
            "Initialized DecoderContext with {} decoders and filter: {}",
            decoder_map.len(),
            filter_desc
        );

        Self {
            decoders: decoder_map,
            engine_db,
            contract_filter,
            registry_cache: Arc::new(RwLock::new(HashMap::new())),
            has_registry: false,
        }
    }

    /// Create a new DecoderContext with registry support
    ///
    /// When a registry is configured:
    /// - Contracts in explicit mappings use those specific decoders
    /// - Contracts in registry_cache use their cached decoders
    /// - Contracts not in either fall back to trying all decoders (auto-discovery)
    ///
    /// # Arguments
    ///
    /// * `decoders` - List of decoders to manage
    /// * `engine_db` - Database for ETL state persistence
    /// * `contract_filter` - Explicit mappings + blacklist
    /// * `registry_cache` - Shared cache from ContractRegistry
    pub fn with_registry(
        decoders: Vec<Arc<dyn Decoder>>,
        engine_db: Arc<EngineDb>,
        contract_filter: ContractFilter,
        registry_cache: Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,
    ) -> Self {
        let decoder_map = Self::build_decoder_map(&decoders);

        let filter_desc =
            if contract_filter.mappings.is_empty() && contract_filter.blacklist.is_empty() {
                "none (using registry for contract identification)".to_string()
            } else {
                let mut parts = Vec::new();
                if !contract_filter.mappings.is_empty() {
                    parts.push(format!(
                        "{} explicit mappings",
                        contract_filter.mappings.len()
                    ));
                }
                if !contract_filter.blacklist.is_empty() {
                    parts.push(format!("{} blacklisted", contract_filter.blacklist.len()));
                }
                parts.push("+ registry".to_string());
                parts.join(", ")
            };

        tracing::info!(
            target: "torii::etl::decoder_context",
            "Initialized DecoderContext with {} decoders, registry enabled, and filter: {}",
            decoder_map.len(),
            filter_desc
        );

        Self {
            decoders: decoder_map,
            engine_db,
            contract_filter,
            registry_cache,
            has_registry: true,
        }
    }

    /// Get the shared registry cache (for external updates)
    pub fn registry_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>> {
        self.registry_cache.clone()
    }

    /// Check if registry is configured
    pub fn has_registry(&self) -> bool {
        self.has_registry
    }

    /// Build decoder map from list (helper for constructor)
    fn build_decoder_map(decoders: &[Arc<dyn Decoder>]) -> HashMap<DecoderId, Arc<dyn Decoder>> {
        let mut decoder_map = HashMap::new();

        for decoder in decoders {
            let name = decoder.decoder_name();
            let id = DecoderId::new(name);

            assert!(
                !decoder_map.contains_key(&id),
                "Duplicate decoder name '{name}' (id: {id:?}). Decoder names must be unique!"
            );

            tracing::debug!(
                target: "torii::etl::decoder_context",
                "Registered decoder '{}' with ID {:?}",
                name,
                id
            );

            decoder_map.insert(id, decoder.clone());
        }

        decoder_map
    }

    /// Get a decoder by its ID
    pub fn get_decoder(&self, id: &DecoderId) -> Option<&Arc<dyn Decoder>> {
        self.decoders.get(id)
    }

    /// Get all registered decoder IDs (sorted for determinism)
    pub fn decoder_ids(&self) -> Vec<DecoderId> {
        let mut ids: Vec<_> = self.decoders.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Get a reference to the engine database
    pub fn engine_db(&self) -> &Arc<EngineDb> {
        &self.engine_db
    }

    /// Decode an event using specific decoders
    async fn decode_with_decoders(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
        decoder_ids: &[DecoderId],
    ) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        for decoder_id in decoder_ids {
            if let Some(decoder) = self.decoders.get(decoder_id) {
                match decoder.decode(keys, data, context).await {
                    Ok(envelopes) => {
                        if !envelopes.is_empty() {
                            tracing::trace!(
                                target: "torii::etl::decoder_context",
                                "Decoder '{}' decoded event from {:#x} into {} envelope(s)",
                                decoder.decoder_name(),
                                context.from_address,
                                envelopes.len()
                            );
                        }
                        all_envelopes.extend(envelopes);
                    }
                    Err(e) => {
                        let selector = keys
                            .first()
                            .map_or_else(|| "<missing>".to_string(), |felt| format!("{felt:#x}"));
                        tracing::warn!(
                            target: "torii::etl::decoder_context",
                            contract = %format!("{:#x}", context.from_address),
                            selector = %selector,
                            tx_hash = %format!("{:#x}", context.transaction_hash),
                            block_number = context.block_number,
                            "Decoder '{}' failed: {}",
                            decoder.decoder_name(),
                            e
                        );
                    }
                }
            } else {
                tracing::trace!(
                    target: "torii::etl::decoder_context",
                    "Decoder ID {:?} not found for contract {:#x}",
                    decoder_id,
                    context.from_address
                );
            }
        }

        Ok(all_envelopes)
    }

    /// Decode an event using all registered decoders (fallback)
    async fn decode_with_all_decoders(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> anyhow::Result<Vec<Envelope>> {
        let mut all_envelopes = Vec::new();

        for decoder in self.decoders.values() {
            match decoder.decode(keys, data, context).await {
                Ok(envelopes) => {
                    if !envelopes.is_empty() {
                        tracing::trace!(
                            target: "torii::etl::decoder_context",
                            "Decoder '{}' decoded event from {:#x} into {} envelope(s)",
                            decoder.decoder_name(),
                            context.from_address,
                            envelopes.len()
                        );
                    }
                    all_envelopes.extend(envelopes);
                }
                Err(e) => {
                    let selector = keys
                        .first()
                        .map_or_else(|| "<missing>".to_string(), |felt| format!("{felt:#x}"));
                    tracing::warn!(
                        target: "torii::etl::decoder_context",
                        contract = %format!("{:#x}", context.from_address),
                        selector = %selector,
                        tx_hash = %format!("{:#x}", context.transaction_hash),
                        block_number = context.block_number,
                        "Decoder '{}' failed: {}",
                        decoder.decoder_name(),
                        e
                    );
                }
            }
        }

        Ok(all_envelopes)
    }
}

#[async_trait]
impl Decoder for DecoderContext {
    fn decoder_name(&self) -> &'static str {
        "context"
    }

    async fn decode(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> anyhow::Result<Vec<Envelope>> {
        // 1. Check blacklist first
        if !self.contract_filter.allows(context.from_address) {
            return Ok(Vec::new());
        }

        // 2. Check explicit mappings (highest priority)
        if let Some(decoder_ids) = self.contract_filter.get_decoders(context.from_address) {
            return self
                .decode_with_decoders(keys, data, context, decoder_ids)
                .await;
        }

        // 3. Check registry cache (if registry is configured)
        if self.has_registry {
            let cache = self.registry_cache.read().await;
            if let Some(decoder_ids) = cache.get(&context.from_address) {
                if decoder_ids.is_empty() {
                    // Contract was identified but no decoders match - skip silently
                    return Ok(Vec::new());
                }

                // Registry cache can contain stale decoder IDs after decoder-ID scheme changes
                // or upgrades. If so, evict and fall back to all decoders for this event.
                let invalid_ids: Vec<DecoderId> = decoder_ids
                    .iter()
                    .copied()
                    .filter(|id| !self.decoders.contains_key(id))
                    .collect();
                if !invalid_ids.is_empty() {
                    drop(cache);
                    {
                        let mut cache = self.registry_cache.write().await;
                        cache.remove(&context.from_address);
                    }
                    tracing::debug!(
                        target: "torii::etl::decoder_context",
                        contract = %format!("{:#x}", context.from_address),
                        invalid_decoder_ids = ?invalid_ids,
                        "Evicted stale decoder mapping from registry cache; falling back to all decoders"
                    );
                    return self.decode_with_all_decoders(keys, data, context).await;
                }

                // Clone to release lock before async decode
                let decoder_ids = decoder_ids.clone();
                drop(cache);
                return self
                    .decode_with_decoders(keys, data, context, &decoder_ids)
                    .await;
            }
            // Not in registry cache = not yet identified, try all decoders
            // This enables auto-discovery: decoders can identify events they understand
            tracing::trace!(
                target: "torii::etl::decoder_context",
                contract = %format!("{:#x}", context.from_address),
                "Contract not in registry cache, trying all decoders"
            );
            drop(cache);
            return self.decode_with_all_decoders(keys, data, context).await;
        }

        // 4. No registry: try all decoders (fallback for non-block-range extractors)
        self.decode_with_all_decoders(keys, data, context).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etl::engine_db::{EngineDb, EngineDbConfig};
    use crate::etl::envelope::{Envelope, TypeId, TypedBody};
    use async_trait::async_trait;
    use std::any::Any;

    #[derive(Debug)]
    struct TestBody {
        seq: u64,
    }

    impl TypedBody for TestBody {
        fn envelope_type_id(&self) -> TypeId {
            TypeId::new("test.body")
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    struct OrderedDecoder {
        contract: Felt,
    }

    #[async_trait]
    impl Decoder for OrderedDecoder {
        fn decoder_name(&self) -> &'static str {
            "ordered_decoder"
        }

        async fn decode(&self, event: &EmittedEvent) -> anyhow::Result<Vec<Envelope>> {
            if event.from_address != self.contract {
                return Ok(Vec::new());
            }

            Ok(vec![Envelope::new(
                format!("evt-{}", event.block_number.unwrap_or_default()),
                Box::new(TestBody {
                    seq: event.block_number.unwrap_or_default(),
                }),
                HashMap::new(),
            )])
        }
    }

    async fn make_engine_db() -> Arc<EngineDb> {
        Arc::new(
            EngineDb::new(EngineDbConfig {
                path: "sqlite::memory:".to_string(),
            })
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn decode_preserves_order_with_parallel_chunks() {
        let contract = Felt::from(0x1234_u64);
        let decoder: Arc<dyn Decoder> = Arc::new(OrderedDecoder { contract });
        let engine_db = make_engine_db().await;
        let context = DecoderContext::new(vec![decoder], engine_db, ContractFilter::new());

        let events = (0..600_u64)
            .map(|seq| EmittedEvent {
                from_address: contract,
                keys: Vec::new(),
                data: Vec::new(),
                block_hash: None,
                block_number: Some(seq),
                transaction_hash: Felt::from(seq + 1),
            })
            .collect::<Vec<_>>();

        let envelopes = Decoder::decode_events(&context, &events).await.unwrap();
        let actual = envelopes
            .iter()
            .map(|envelope| envelope.downcast_ref::<TestBody>().unwrap().seq)
            .collect::<Vec<_>>();
        let expected = (0..600_u64).collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
