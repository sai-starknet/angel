//! Contract registry for caching contract→decoder mappings.
//!
//! The registry loads cached mappings from database and provides shared access.
//! It also supports runtime identification of unknown contracts by fetching their ABIs
//! and running identification rules.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// Maximum number of requests per batch to avoid RPC limits
const MAX_BATCH_SIZE: usize = 500;
use std::sync::Arc;

use super::IdentificationRule;
use crate::etl::decoder::DecoderId;
use crate::etl::engine_db::EngineDb;
use crate::etl::extractor::ContractAbi;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use starknet::core::types::requests::{GetClassHashAtRequest, GetClassRequest};
use starknet::core::types::{BlockId, BlockTag};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use tokio::sync::RwLock;

/// Trait for contract identification (object-safe).
///
/// This trait allows type-erased contract identification, so that `ToriiConfig`
/// can store a registry without knowing the concrete provider type.
#[async_trait]
pub trait ContractIdentifier: Send + Sync {
    /// Identify contracts by fetching their ABIs and running rules.
    ///
    /// Returns HashMap of contract → decoder IDs for newly identified contracts.
    async fn identify_contracts(
        &self,
        contract_addresses: &[Felt],
    ) -> Result<HashMap<Felt, Vec<DecoderId>>>;

    /// Get a shared reference to the cache.
    fn shared_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>;
}

/// Contract registry for caching contract→decoder mappings.
///
/// The registry manages:
/// - In-memory cache of contract→decoder mappings
/// - Persistence to EngineDb for restart recovery
///
/// # Thread Safety
///
/// The registry uses interior mutability (RwLock) for the cache,
/// allowing shared access while supporting concurrent reads and
/// exclusive writes during identification.
///
/// # Performance
///
/// Uses batch JSON-RPC requests to identify multiple contracts efficiently:
/// - Batch 1: Fetch all class hashes at once
/// - Batch 2: Fetch all unique contract classes (deduplicated by class hash)
///
/// This reduces N×2 sequential API calls to just 2 batch calls.
pub struct ContractRegistry {
    /// Starknet provider for fetching contract ABIs (JsonRpcClient for batch support)
    provider: Arc<JsonRpcClient<HttpTransport>>,

    /// Engine database for persistence
    engine_db: Arc<EngineDb>,

    /// Identification rules for matching contract ABIs to decoders
    rules: Vec<Box<dyn IdentificationRule>>,

    /// In-memory positive cache: contract → decoders
    /// Wrapped in Arc for sharing with DecoderContext
    cache: Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>,

    /// Bounded in-memory negative cache for contracts with no decoder match.
    ///
    /// This avoids repeated identification work for unknown contracts while
    /// preventing unbounded memory growth.
    negative_cache: Arc<RwLock<NegativeCache>>,

    /// Maximum number of chunked RPC requests to execute concurrently.
    rpc_parallelism: usize,
}

/// Bounded in-memory negative cache (FIFO/LRU-like).
struct NegativeCache {
    set: HashSet<Felt>,
    order: VecDeque<Felt>,
    capacity: usize,
}

impl NegativeCache {
    fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn contains(&self, contract: &Felt) -> bool {
        self.set.contains(contract)
    }

    fn insert(&mut self, contract: Felt) -> Vec<Felt> {
        if self.capacity == 0 {
            return Vec::new();
        }

        if self.set.contains(&contract) {
            self.order.retain(|c| c != &contract);
            self.order.push_back(contract);
            return Vec::new();
        }

        self.set.insert(contract);
        self.order.push_back(contract);

        let mut evicted = Vec::new();
        while self.set.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
                evicted.push(oldest);
            } else {
                break;
            }
        }
        evicted
    }

    fn remove(&mut self, contract: &Felt) {
        if self.set.remove(contract) {
            self.order.retain(|c| c != contract);
        }
    }
}

impl ContractRegistry {
    /// Maximum number of contracts to keep in negative cache.
    const NEGATIVE_CACHE_CAPACITY: usize = 100_000;

    /// Create a new contract registry.
    ///
    /// # Arguments
    ///
    /// * `provider` - JsonRpcClient provider for fetching ABIs (supports batch requests)
    /// * `engine_db` - Database for persistence
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>, engine_db: Arc<EngineDb>) -> Self {
        Self {
            provider,
            engine_db,
            rules: Vec::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            negative_cache: Arc::new(RwLock::new(NegativeCache::new(
                Self::NEGATIVE_CACHE_CAPACITY,
            ))),
            rpc_parallelism: 0,
        }
    }

    /// Add an identification rule.
    ///
    /// Rules are run in order during identification. All matching decoders
    /// from all rules are collected.
    pub fn with_rule(mut self, rule: Box<dyn IdentificationRule>) -> Self {
        tracing::debug!(
            target: "torii::etl::identification",
            "Registered identification rule: {}",
            rule.name()
        );
        self.rules.push(rule);
        self
    }

    pub fn with_rpc_parallelism(mut self, rpc_parallelism: usize) -> Self {
        self.rpc_parallelism = rpc_parallelism;
        self
    }

    fn resolved_rpc_parallelism(&self) -> usize {
        if self.rpc_parallelism == 0 {
            std::thread::available_parallelism()
                .map(|parallelism| parallelism.get().clamp(2, 8))
                .unwrap_or(4)
        } else {
            self.rpc_parallelism.max(1)
        }
    }

    /// Load cached mappings from database on startup.
    ///
    /// This should be called during initialization to restore
    /// previously identified contracts.
    pub async fn load_from_db(&self) -> Result<usize> {
        let mappings = self.engine_db.get_all_contract_decoders().await?;
        let mut loaded_positive = 0usize;
        let mut loaded_empty = 0usize;

        for (contract, decoder_ids, _timestamp) in mappings {
            if decoder_ids.is_empty() {
                self.cache_empty(contract).await;
                loaded_empty += 1;
                continue;
            }
            {
                let mut negative_cache = self.negative_cache.write().await;
                negative_cache.remove(&contract);
            }
            {
                let mut cache = self.cache.write().await;
                cache.insert(contract, decoder_ids);
            }
            loaded_positive += 1;
        }

        tracing::info!(
            target: "torii::etl::identification",
            loaded_positive,
            loaded_empty,
            "Loaded contract mappings from database"
        );

        Ok(loaded_positive)
    }

    /// Get a shared reference to the cache for use with DecoderContext.
    ///
    /// This allows the DecoderContext to read from the registry's cache
    /// without needing a direct reference to the registry (which has a generic Provider type).
    pub fn shared_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>> {
        self.cache.clone()
    }

    /// Identify contracts by fetching their ABIs and running rules.
    ///
    /// This method identifies unknown contracts from a batch of events by:
    /// 1. Filtering out contracts already in the cache
    /// 2. Batch fetching all class hashes at once
    /// 3. Batch fetching all unique contract classes (deduplicated by class hash)
    /// 4. Running all identification rules
    /// 5. Caching positives in memory and database, negatives in bounded memory
    ///
    /// # Performance
    ///
    /// Uses batch JSON-RPC requests to minimize API calls:
    /// - Instead of N×2 sequential calls, makes just 2 batch calls
    /// - Class fetching is deduplicated (multiple contracts may share the same class)
    ///
    /// # Arguments
    ///
    /// * `contract_addresses` - Contract addresses from the current batch
    ///
    /// # Returns
    ///
    /// HashMap of contract → decoder IDs for contracts that were successfully identified.
    /// Contracts that fail identification are cached in a bounded in-memory negative cache.
    pub async fn identify_contracts(
        &self,
        contract_addresses: &[Felt],
    ) -> Result<HashMap<Felt, Vec<DecoderId>>> {
        // Deduplicate and filter out contracts already in cache
        let unique_addresses: HashSet<Felt> = contract_addresses.iter().copied().collect();
        let cache = self.cache.read().await;
        let negative_cache = self.negative_cache.read().await;
        let unknown: Vec<Felt> = unique_addresses
            .into_iter()
            .filter(|addr| !cache.contains_key(addr) && !negative_cache.contains(addr))
            .collect();
        drop(cache);
        drop(negative_cache);

        if unknown.is_empty() {
            return Ok(HashMap::new());
        }

        tracing::debug!(
            target: "torii::etl::identification",
            "Identifying {} unknown contracts using batch requests",
            unknown.len()
        );

        // BATCH 1: Fetch all class hashes (chunked to respect RPC limits)
        let mut contract_to_class: HashMap<Felt, Felt> = HashMap::new();

        let rpc_parallelism = self.resolved_rpc_parallelism();
        ::metrics::gauge!("torii_rpc_parallelism").set(rpc_parallelism as f64);

        let class_hash_tasks = unknown
            .chunks(MAX_BATCH_SIZE)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let class_hash_requests: Vec<ProviderRequestData> = chunk
                    .iter()
                    .map(|&addr| {
                        ProviderRequestData::GetClassHashAt(GetClassHashAtRequest {
                            block_id: BlockId::Tag(BlockTag::Latest),
                            contract_address: addr.into(),
                        })
                    })
                    .collect();
                let chunk_addresses = chunk.to_vec();
                async move {
                    let chunk_start = std::time::Instant::now();
                    let class_hash_responses = self
                        .provider
                        .batch_requests(&class_hash_requests)
                        .await
                        .context("Failed to batch fetch class hashes")?;
                    ::metrics::histogram!(
                        "torii_rpc_chunk_duration_seconds",
                        "extractor" => "registry",
                        "method" => "get_class_hash_at_batch"
                    )
                    .record(chunk_start.elapsed().as_secs_f64());
                    Ok::<_, anyhow::Error>((chunk_index, chunk_addresses, class_hash_responses))
                }
            })
            .collect::<Vec<_>>();

        let mut class_hash_chunks = stream::iter(class_hash_tasks)
            .buffer_unordered(rpc_parallelism)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        class_hash_chunks.sort_by_key(|(chunk_index, _, _)| *chunk_index);

        for (_, chunk_addresses, class_hash_responses) in class_hash_chunks {
            // Map contract → class_hash, track failures
            for (addr, response) in chunk_addresses.iter().zip(class_hash_responses) {
                if let ProviderResponseData::GetClassHashAt(class_hash) = response {
                    contract_to_class.insert(*addr, class_hash.into());
                } else {
                    tracing::debug!(
                        target: "torii::etl::identification",
                        contract = %format!("{:#x}", addr),
                        "Failed to get class hash, caching as empty"
                    );
                    self.cache_empty(*addr).await;
                }
            }
        }

        if contract_to_class.is_empty() {
            return Ok(HashMap::new());
        }

        // BATCH 2: Fetch unique classes (deduplicated by class hash, chunked to respect RPC limits)
        let unique_class_hashes: Vec<Felt> = contract_to_class
            .values()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        tracing::debug!(
            target: "torii::etl::identification",
            contracts = contract_to_class.len(),
            unique_classes = unique_class_hashes.len(),
            "Fetching unique contract classes"
        );

        // Map class_hash → ContractAbi
        let mut class_to_abi: HashMap<Felt, ContractAbi> = HashMap::new();

        let class_tasks = unique_class_hashes
            .chunks(MAX_BATCH_SIZE)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let class_requests: Vec<ProviderRequestData> = chunk
                    .iter()
                    .map(|&class_hash| {
                        ProviderRequestData::GetClass(GetClassRequest {
                            block_id: BlockId::Tag(BlockTag::Latest),
                            class_hash: class_hash.into(),
                        })
                    })
                    .collect();
                let chunk_hashes = chunk.to_vec();
                async move {
                    let chunk_start = std::time::Instant::now();
                    let class_responses = self
                        .provider
                        .batch_requests(&class_requests)
                        .await
                        .context("Failed to batch fetch classes")?;
                    ::metrics::histogram!(
                        "torii_rpc_chunk_duration_seconds",
                        "extractor" => "registry",
                        "method" => "get_class_batch"
                    )
                    .record(chunk_start.elapsed().as_secs_f64());
                    Ok::<_, anyhow::Error>((chunk_index, chunk_hashes, class_responses))
                }
            })
            .collect::<Vec<_>>();

        let mut class_chunks = stream::iter(class_tasks)
            .buffer_unordered(rpc_parallelism)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        class_chunks.sort_by_key(|(chunk_index, _, _)| *chunk_index);

        for (_, chunk_hashes, class_responses) in class_chunks {
            for (class_hash, response) in chunk_hashes.iter().zip(class_responses) {
                match response {
                    ProviderResponseData::GetClass(contract_class) => {
                        match ContractAbi::from_contract_class(contract_class) {
                            Ok(abi) => {
                                class_to_abi.insert(*class_hash, abi);
                            }
                            Err(e) => {
                                tracing::debug!(
                                    target: "torii::etl::identification",
                                    class_hash = %format!("{:#x}", class_hash),
                                    error = %e,
                                    "Failed to parse ABI"
                                );
                            }
                        }
                    }
                    _ => {
                        tracing::debug!(
                            target: "torii::etl::identification",
                            class_hash = %format!("{:#x}", class_hash),
                            "Failed to get class"
                        );
                    }
                }
            }
        }

        // Run identification rules for each contract
        let mut results = HashMap::new();
        let mut positives: HashMap<Felt, Vec<DecoderId>> = HashMap::new();
        let mut negatives: Vec<Felt> = Vec::new();

        for (contract_address, class_hash) in &contract_to_class {
            let decoder_ids = if let Some(abi) = class_to_abi.get(class_hash) {
                self.run_rules(*contract_address, *class_hash, abi)
            } else {
                Vec::new()
            };

            if decoder_ids.is_empty() {
                negatives.push(*contract_address);
            } else {
                tracing::info!(
                    target: "torii::etl::identification",
                    contract = %format!("{:#x}", contract_address),
                    decoders = ?decoder_ids,
                    "Contract identified"
                );
                positives.insert(*contract_address, decoder_ids.clone());
            }

            results.insert(*contract_address, decoder_ids);
        }

        // Batch update caches
        if !negatives.is_empty() {
            let mut negative_cache = self.negative_cache.write().await;
            let mut cache = self.cache.write().await;

            for contract_address in &negatives {
                let evicted = negative_cache.insert(*contract_address);
                cache.insert(*contract_address, Vec::new());

                for contract in evicted {
                    cache.remove(&contract);
                }
            }
        }

        if !positives.is_empty() {
            {
                let mut negative_cache = self.negative_cache.write().await;
                for contract_address in positives.keys() {
                    negative_cache.remove(contract_address);
                }
            }
            {
                let mut cache = self.cache.write().await;
                for (contract_address, decoder_ids) in &positives {
                    cache.insert(*contract_address, decoder_ids.clone());
                }
            }

            // Batch persist to database
            if let Err(e) = self.engine_db.set_contract_decoders_batch(&positives).await {
                tracing::warn!(
                    target: "torii::etl::identification",
                    count = positives.len(),
                    error = %e,
                    "Failed to batch persist contract identifications"
                );
            }
        }

        Ok(results)
    }

    /// Run all identification rules on a contract's ABI.
    fn run_rules(
        &self,
        contract_address: Felt,
        class_hash: Felt,
        abi: &ContractAbi,
    ) -> Vec<DecoderId> {
        let mut matched_decoders = BTreeSet::new();
        for rule in &self.rules {
            match rule.identify_by_abi(contract_address, class_hash, abi) {
                Ok(decoder_ids) => {
                    if !decoder_ids.is_empty() {
                        tracing::debug!(
                            target: "torii::etl::identification",
                            contract = %format!("{:#x}", contract_address),
                            rule = rule.name(),
                            decoders = ?decoder_ids,
                            "Rule matched"
                        );
                        matched_decoders.extend(decoder_ids);
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        target: "torii::etl::identification",
                        contract = %format!("{:#x}", contract_address),
                        rule = rule.name(),
                        error = %e,
                        "Rule failed"
                    );
                }
            }
        }
        matched_decoders.into_iter().collect()
    }

    /// Cache empty result for a contract that failed identification.
    async fn cache_empty(&self, contract_address: Felt) {
        let evicted = {
            let mut negative_cache = self.negative_cache.write().await;
            negative_cache.insert(contract_address)
        };

        let mut cache = self.cache.write().await;
        cache.insert(contract_address, Vec::new());
        for contract in evicted {
            cache.remove(&contract);
        }
    }
}

#[async_trait]
impl ContractIdentifier for ContractRegistry {
    async fn identify_contracts(
        &self,
        contract_addresses: &[Felt],
    ) -> Result<HashMap<Felt, Vec<DecoderId>>> {
        // Delegate to the inherent method
        ContractRegistry::identify_contracts(self, contract_addresses).await
    }

    fn shared_cache(&self) -> Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>> {
        ContractRegistry::shared_cache(self)
    }
}
