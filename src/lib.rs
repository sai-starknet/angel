//! Torii - Modular blockchain indexer.
//!
//! This library aims at providing a modular and high-performance blockchain indexer.
//! The current implementation is still WIP, but gives a good idea of the architecture and the capabilities.

pub mod command;
pub mod etl;
pub mod grpc;
pub mod http;
pub mod metrics;

// Include generated protobuf code
pub mod proto {
    pub mod torii {
        tonic::include_proto!("torii");
    }
}

// Re-export commonly used types for external sink authors
use crate::etl::StarknetEvent;
pub use async_trait::async_trait;

pub use {axum, tokio, tonic};

// Re-export UpdateType for sink implementations
pub use grpc::UpdateType;

use axum::Router as AxumRouter;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tower::Service;
use tower_http::cors::{Any as CorsAny, CorsLayer};

use command::{CommandBus, CommandHandler};
use etl::decoder::{ContractFilter, DecoderId};
use etl::extractor::{Extractor, SyntheticExtractor, SyntheticExtractorAdapter};
use etl::identification::{ContractIdentifier, IdentificationRule};
use etl::sink::{EventBus, Sink};
use etl::{Decoder, DecoderContext, MultiSink, SampleExtractor};
use grpc::{create_grpc_service, GrpcState, SubscriptionManager};
use http::create_http_router;
use starknet_types_raw::Felt;

// Include the file descriptor set generated at build time.
// This is also exported publicly so external sink authors can use it for reflection.
const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../target/descriptor.bin");

/// Torii core gRPC service descriptor set (for reflection).
///
/// External sink authors should use this when building custom reflection services
/// that include both core Torii and their sink services.
///
/// # Example
///
/// ```rust,ignore
/// use torii::TORII_DESCRIPTOR_SET;
///
/// let reflection = tonic_reflection::server::Builder::configure()
///     .register_encoded_file_descriptor_set(TORII_DESCRIPTOR_SET)
///     .register_encoded_file_descriptor_set(my_sink::FILE_DESCRIPTOR_SET)
///     .build_v1()?;
/// ```
///
/// This descriptor set is generated at build time from `proto/torii.proto`
/// by the `build.rs` script. See `build.rs` for details.
pub const TORII_DESCRIPTOR_SET: &[u8] = FILE_DESCRIPTOR_SET;

/// Configuration for Torii server with pluggable sinks and decoders.
#[derive(Debug, Clone)]
pub struct EtlConcurrencyConfig {
    pub max_prefetch_batches: usize,
}

impl Default for EtlConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_prefetch_batches: 2,
        }
    }
}

impl EtlConcurrencyConfig {
    pub fn resolved_prefetch_batches(&self) -> usize {
        self.max_prefetch_batches.max(1)
    }
}

pub struct ToriiConfig {
    /// Port to listen on.
    pub port: u16,

    /// Host to bind to.
    pub host: String,

    /// Custom sinks to register (not yet initialized).
    pub sinks: Vec<Box<dyn Sink>>,

    /// Custom decoders to register.
    pub decoders: Vec<Arc<dyn Decoder>>,

    /// Optional pre-built gRPC router with sink services.
    ///
    /// Users can add their sink gRPC services to a router and pass it here.
    /// Torii will add the core service to this router. This works around Rust's
    /// type system limitations while keeping all services on the same port.
    ///
    /// If None, Torii creates a fresh router with only core services.
    pub partial_grpc_router: Option<tonic::transport::server::Router>,

    /// Whether the user has already added reflection to the gRPC router
    ///
    /// If true, Torii will skip adding reflection services (to avoid conflicts).
    pub custom_reflection: bool,

    /// ETL cycle interval in seconds.
    pub cycle_interval: u64,

    /// Events per cycle.
    pub events_per_cycle: usize,

    /// Sample events for testing (provided by sinks).
    pub sample_events: Vec<StarknetEvent>,

    /// Extractor for fetching blockchain events.
    ///
    /// If None, a SampleExtractor will be used for testing.
    pub extractor: Option<Box<dyn Extractor>>,

    /// Root directory for all databases (engine, sinks, registry).
    ///
    /// Defaults to current directory if not specified.
    pub database_root: PathBuf,

    /// Optional explicit engine database URL/path.
    ///
    /// If set, Torii uses this value directly when creating `EngineDb`.
    /// This can be a PostgreSQL URL (`postgres://...`) or SQLite path/URL.
    pub engine_database_url: Option<String>,

    /// Contract filter (explicit mappings + blacklist).
    pub contract_filter: ContractFilter,

    /// Identification rules for auto-discovery of contract types.
    ///
    /// When a contract is encountered that's not in the explicit mappings,
    /// these rules are used to identify the contract type by inspecting its ABI.
    /// The identified mapping is then cached for future events.
    ///
    /// NOTE: Rules alone don't enable auto-identification. You must also provide
    /// a registry cache via `with_registry_cache()` for identification to work.
    pub identification_rules: Vec<Box<dyn IdentificationRule>>,

    /// Optional shared registry cache from ContractRegistry.
    ///
    /// If provided, the DecoderContext will use this cache to look up
    /// contract→decoder mappings for contracts not in explicit mappings.
    /// The cache is typically populated by a ContractRegistry running batch
    /// identification before decoding.
    pub registry_cache:
        Option<Arc<tokio::sync::RwLock<std::collections::HashMap<Felt, Vec<DecoderId>>>>>,

    /// Optional contract identifier for runtime identification.
    ///
    /// If provided (via `with_contract_identifier()`), unknown contracts
    /// will be identified by fetching their ABIs during the ETL loop.
    /// This enables auto-discovery of token contracts without explicit mapping.
    pub contract_identifier: Option<Arc<dyn ContractIdentifier>>,

    /// Graceful shutdown timeout in seconds (default: 30).
    ///
    /// When a shutdown signal is received, the system will wait up to this
    /// duration for the current ETL batch to complete before forcing shutdown.
    pub shutdown_timeout: u64,

    /// ETL concurrency controls.
    pub etl_concurrency: EtlConcurrencyConfig,

    /// Background command handlers registered for the command bus.
    pub command_handlers: Vec<Box<dyn CommandHandler>>,

    /// Command bus queue size.
    pub command_bus_queue_size: usize,

    /// Optional TLS listener configuration.
    pub tls: Option<ToriiTlsConfig>,
}

impl ToriiConfig {
    pub fn builder() -> ToriiConfigBuilder {
        ToriiConfigBuilder::default()
    }
}

#[derive(Debug, Clone)]
pub struct ToriiTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub alpn_protocols: Vec<Vec<u8>>,
}

impl ToriiTlsConfig {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            alpn_protocols: vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        }
    }

    pub fn with_alpn_protocols(mut self, alpn_protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = alpn_protocols;
        self
    }

    fn alpn_names(&self) -> Vec<String> {
        self.alpn_protocols
            .iter()
            .map(|protocol| String::from_utf8_lossy(protocol).into_owned())
            .collect()
    }
}

/// Builder for ToriiConfig.
#[derive(Default)]
pub struct ToriiConfigBuilder {
    port: Option<u16>,
    host: Option<String>,
    sinks: Vec<Box<dyn Sink>>,
    decoders: Vec<Arc<dyn Decoder>>,
    partial_grpc_router: Option<tonic::transport::server::Router>,
    custom_reflection: bool,
    cycle_interval: Option<u64>,
    events_per_cycle: Option<usize>,
    sample_events: Vec<StarknetEvent>,
    extractor: Option<Box<dyn Extractor>>,
    database_root: Option<PathBuf>,
    engine_database_url: Option<String>,
    contract_filter: Option<ContractFilter>,
    identification_rules: Vec<Box<dyn IdentificationRule>>,
    registry_cache:
        Option<Arc<tokio::sync::RwLock<std::collections::HashMap<Felt, Vec<DecoderId>>>>>,
    contract_identifier: Option<Arc<dyn ContractIdentifier>>,
    shutdown_timeout: Option<u64>,
    etl_concurrency: Option<EtlConcurrencyConfig>,
    command_handlers: Vec<Box<dyn CommandHandler>>,
    command_bus_queue_size: Option<usize>,
    tls: Option<ToriiTlsConfig>,
}

impl ToriiConfigBuilder {
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }

    /// Adds a sink (which may also provide a gRPC service).
    ///
    /// Sinks should NOT be wrapped in Arc yet - they will be initialized and wrapped by Torii.
    pub fn add_sink_boxed(mut self, sink: Box<dyn Sink>) -> Self {
        self.sinks.push(sink);
        self
    }

    /// Adds multiple sinks at once.
    pub fn with_sinks_boxed(mut self, sinks: Vec<Box<dyn Sink>>) -> Self {
        self.sinks.extend(sinks);
        self
    }

    /// Adds a decoder.
    pub fn add_decoder(mut self, decoder: Arc<dyn Decoder>) -> Self {
        self.decoders.push(decoder);
        self
    }

    /// Adds multiple decoders at once.
    pub fn with_decoders(mut self, decoders: Vec<Arc<dyn Decoder>>) -> Self {
        self.decoders.extend(decoders);
        self
    }

    /// Sets a pre-built gRPC router with sink services.
    ///
    /// Users should add their sink gRPC services to a router and pass it here.
    /// Torii will add the core Torii service to this router before starting.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tonic::transport::Server;
    ///
    /// let router = Server::builder()
    ///     .accept_http1(true)
    ///     .add_service(tonic_web::enable(SqlSinkServer::new(service)));
    ///
    /// let config = ToriiConfig::builder()
    ///     .with_grpc_router(router)
    ///     .build();
    /// ```
    pub fn with_grpc_router(mut self, router: tonic::transport::server::Router) -> Self {
        self.partial_grpc_router = Some(router);
        self
    }

    /// Indicate that the gRPC router already includes reflection services
    ///
    /// Set this to true if you've already added reflection services to your
    /// gRPC router (with all sink descriptor sets). Torii will skip adding
    /// reflection to avoid route conflicts.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let reflection = tonic_reflection::server::Builder::configure()
    ///     .register_encoded_file_descriptor_set(TORII_DESCRIPTOR_SET)
    ///     .register_encoded_file_descriptor_set(SQL_SINK_DESCRIPTOR_SET)
    ///     .build_v1()?;
    ///
    /// let router = Server::builder()
    ///     .add_service(reflection);
    ///
    /// let config = ToriiConfig::builder()
    ///     .with_grpc_router(router)
    ///     .with_custom_reflection(true)  // Skip Torii's reflection
    ///     .build();
    /// ```
    pub fn with_custom_reflection(mut self, custom: bool) -> Self {
        self.custom_reflection = custom;
        self
    }

    /// Sets the ETL cycle interval in seconds.
    pub fn cycle_interval(mut self, seconds: u64) -> Self {
        self.cycle_interval = Some(seconds);
        self
    }

    /// Sets the number of events per cycle.
    pub fn events_per_cycle(mut self, count: usize) -> Self {
        self.events_per_cycle = Some(count);
        self
    }

    /// Adds sample events for testing.
    pub fn with_sample_events(mut self, events: Vec<StarknetEvent>) -> Self {
        self.sample_events.extend(events);
        self
    }

    /// Sets the extractor for fetching blockchain events.
    ///
    /// If not set, a SampleExtractor will be used for testing.
    pub fn with_extractor(mut self, extractor: Box<dyn Extractor>) -> Self {
        self.extractor = Some(extractor);
        self
    }

    /// Sets a synthetic extractor wrapped as a regular ETL extractor.
    ///
    /// This is intended for tests and local deterministic runs where the full
    /// ingestion pipeline should execute without a live provider.
    pub fn with_synthetic_extractor<T>(mut self, extractor: T) -> Self
    where
        T: SyntheticExtractor + 'static,
    {
        self.extractor = Some(Box::new(SyntheticExtractorAdapter::new(extractor)));
        self
    }

    /// Sets the root directory for all databases.
    ///
    /// All Torii databases (engine, sinks, registry) will be placed in this directory.
    /// Defaults to current directory if not specified.
    pub fn database_root(mut self, path: impl Into<PathBuf>) -> Self {
        self.database_root = Some(path.into());
        self
    }

    /// Sets an explicit engine database URL/path.
    ///
    /// Examples:
    /// - `postgres://user:pass@localhost:5432/torii`
    /// - `sqlite::memory:`
    /// - `./torii-data/engine.db`
    pub fn engine_database_url(mut self, url: impl Into<String>) -> Self {
        self.engine_database_url = Some(url.into());
        self
    }

    /// Sets the contract filter (mappings + blacklist).
    pub fn with_contract_filter(mut self, filter: ContractFilter) -> Self {
        self.contract_filter = Some(filter);
        self
    }

    /// Add explicit contract→decoder mapping (most efficient).
    ///
    /// Events from this contract will ONLY be tried with the specified decoders.
    /// This provides O(k) performance where k is the number of mapped decoders.
    pub fn map_contract(mut self, contract: Felt, decoder_ids: Vec<DecoderId>) -> Self {
        self.contract_filter
            .get_or_insert_with(ContractFilter::new)
            .mappings
            .insert(contract, decoder_ids);
        self
    }

    /// Add contract to blacklist (fast discard).
    ///
    /// Events from this contract will be discarded immediately (O(1) check).
    pub fn blacklist_contract(mut self, contract: Felt) -> Self {
        self.contract_filter
            .get_or_insert_with(ContractFilter::new)
            .blacklist
            .insert(contract);
        self
    }

    /// Add multiple contracts to blacklist.
    ///
    /// Events from these contracts will be discarded immediately (O(1) check).
    pub fn blacklist_contracts(mut self, contracts: Vec<Felt>) -> Self {
        self.contract_filter
            .get_or_insert_with(ContractFilter::new)
            .blacklist
            .extend(contracts);
        self
    }

    /// Add identification rule for auto-discovery.
    ///
    /// Identification rules are used to automatically identify contract types
    /// by inspecting their ABI. When a contract is encountered that's not in
    /// the explicit mappings, the registry will:
    /// 1. Fetch the contract's ABI from the chain
    /// 2. Run all identification rules
    /// 3. Cache the contract→decoder mapping for future events
    ///
    /// NOTE: Rules alone don't enable auto-identification. You must also provide
    /// a registry cache via `with_registry_cache()` for identification to work.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = ToriiConfig::builder()
    ///     .with_identification_rule(Box::new(Erc20Rule::new()))
    ///     .with_identification_rule(Box::new(Erc721Rule::new()))
    ///     .build();
    /// ```
    pub fn with_identification_rule(mut self, rule: Box<dyn IdentificationRule>) -> Self {
        self.identification_rules.push(rule);
        self
    }

    /// Set the shared registry cache from a ContractRegistry.
    ///
    /// When a registry cache is provided, the DecoderContext will:
    /// - Look up contract→decoder mappings from the cache
    /// - Skip events from contracts not in the cache (they should have been identified before decode)
    ///
    /// Without a registry cache, events from unmapped contracts are tried against all decoders.
    ///
    /// # Usage Pattern
    ///
    /// ```rust,ignore
    /// // Create registry with your provider and rules
    /// let registry = ContractRegistry::new(provider, engine_db)
    ///     .with_rule(Box::new(Erc20Rule::new()))
    ///     .with_rule(Box::new(Erc721Rule::new()));
    ///
    /// // Load cached mappings from database
    /// registry.load_from_db().await?;
    ///
    /// // Pass the cache to config
    /// let config = ToriiConfig::builder()
    ///     .with_registry_cache(registry.shared_cache())
    ///     .build();
    /// ```
    pub fn with_registry_cache(
        mut self,
        cache: Arc<tokio::sync::RwLock<std::collections::HashMap<Felt, Vec<DecoderId>>>>,
    ) -> Self {
        self.registry_cache = Some(cache);
        self
    }

    /// Set a contract identifier for runtime identification.
    ///
    /// When a contract identifier is provided, unknown contracts in each batch
    /// will be automatically identified by fetching their ABIs and running
    /// identification rules. This enables auto-discovery without explicit mapping.
    ///
    /// This method automatically extracts the registry cache for the DecoderContext.
    ///
    /// # Usage Pattern
    ///
    /// ```rust,ignore
    /// // Create registry with your provider and rules
    /// let registry = Arc::new(ContractRegistry::new(provider, engine_db)
    ///     .with_rule(Box::new(Erc20Rule::new()))
    ///     .with_rule(Box::new(Erc721Rule::new())));
    ///
    /// // Load cached mappings from database
    /// registry.load_from_db().await?;
    ///
    /// // Pass the full registry (enables runtime identification)
    /// let config = ToriiConfig::builder()
    ///     .with_contract_identifier(registry)
    ///     .build();
    /// ```
    pub fn with_contract_identifier(mut self, identifier: Arc<dyn ContractIdentifier>) -> Self {
        // Extract cache for DecoderContext
        self.registry_cache = Some(identifier.shared_cache());
        self.contract_identifier = Some(identifier);
        self
    }

    /// Sets the graceful shutdown timeout in seconds.
    ///
    /// When a shutdown signal (SIGINT/SIGTERM) is received, the system will wait
    /// up to this duration for the current ETL batch to complete before forcing
    /// shutdown. Default is 30 seconds.
    pub fn shutdown_timeout(mut self, seconds: u64) -> Self {
        self.shutdown_timeout = Some(seconds);
        self
    }

    /// Sets ETL prefetch and decode concurrency.
    pub fn etl_concurrency(mut self, config: EtlConcurrencyConfig) -> Self {
        self.etl_concurrency = Some(config);
        self
    }

    pub fn with_command_handler(mut self, handler: Box<dyn CommandHandler>) -> Self {
        self.command_handlers.push(handler);
        self
    }

    pub fn with_command_handlers(mut self, handlers: Vec<Box<dyn CommandHandler>>) -> Self {
        self.command_handlers.extend(handlers);
        self
    }

    pub fn command_bus_queue_size(mut self, size: usize) -> Self {
        self.command_bus_queue_size = Some(size.max(1));
        self
    }

    pub fn with_tls(mut self, tls: ToriiTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Builds the Torii configuration.
    ///
    /// # Panics
    ///
    /// Panics if the contract filter configuration is invalid (e.g., a contract appears
    /// in both the mapping and blacklist).
    pub fn build(self) -> ToriiConfig {
        let contract_filter = self.contract_filter.unwrap_or_default();

        // Validate contract filter
        if let Err(e) = contract_filter.validate() {
            panic!("Invalid contract filter configuration: {e}");
        }

        ToriiConfig {
            port: self.port.unwrap_or(8080),
            host: self.host.unwrap_or_else(|| "0.0.0.0".to_string()),
            sinks: self.sinks,
            decoders: self.decoders,
            partial_grpc_router: self.partial_grpc_router,
            custom_reflection: self.custom_reflection,
            cycle_interval: self.cycle_interval.unwrap_or(3),
            events_per_cycle: self.events_per_cycle.unwrap_or(5),
            sample_events: self.sample_events,
            extractor: self.extractor,
            database_root: self.database_root.unwrap_or_else(|| PathBuf::from(".")),
            engine_database_url: self.engine_database_url,
            contract_filter,
            identification_rules: self.identification_rules,
            registry_cache: self.registry_cache,
            contract_identifier: self.contract_identifier,
            shutdown_timeout: self.shutdown_timeout.unwrap_or(30),
            etl_concurrency: self.etl_concurrency.unwrap_or_default(),
            command_handlers: self.command_handlers,
            command_bus_queue_size: self.command_bus_queue_size.unwrap_or(4096),
            tls: self.tls,
        }
    }
}

/// Starts the Torii server with custom configuration.
///
/// NOTE: The caller is responsible for initializing the tracing subscriber before calling this function.
///
/// TODO: this function is just too big. But it has the whole workflow.
/// This will be split into smaller functions in the future with associated configuration for each step.
pub async fn run(config: ToriiConfig) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(target: "torii::main", "Starting Torii with {} sink(s) and {} decoder(s)",
        config.sinks.len(), config.decoders.len());

    match metrics::init_from_env() {
        Ok(true) => {
            tracing::info!(target: "torii::main", "Prometheus metrics enabled at /metrics");
            metrics::set_build_info(env!("CARGO_PKG_VERSION"));
        }
        Ok(false) => {
            tracing::info!(target: "torii::main", "Prometheus metrics disabled by TORII_METRICS_ENABLED");
        }
        Err(e) => {
            tracing::warn!(target: "torii::main", error = %e, "Failed to initialize metrics recorder");
        }
    }

    let subscription_manager = Arc::new(SubscriptionManager::new());
    let event_bus = Arc::new(EventBus::new(subscription_manager.clone()));
    for handler in &config.command_handlers {
        handler.attach_event_bus(event_bus.clone());
    }
    let command_bus = CommandBus::new(config.command_handlers, config.command_bus_queue_size)?;

    // Create SinkContext for initialization
    let sink_context = etl::sink::SinkContext {
        database_root: config.database_root.clone(),
        command_bus: command_bus.sender(),
    };

    let mut initialized_sinks: Vec<Arc<dyn Sink>> = Vec::new();

    for mut sink in config.sinks {
        // Box is used for sinks since we need to call initialize (mutable reference).
        sink.initialize(event_bus.clone(), &sink_context).await?;
        // Convert Box<dyn Sink> to Arc<dyn Sink> since now we can use it immutably.
        initialized_sinks.push(Arc::from(sink));
    }

    let multi_sink = Arc::new(MultiSink::new(initialized_sinks));

    // Create EngineDb (needed by DecoderContext)
    let engine_db_path = config.engine_database_url.clone().unwrap_or_else(|| {
        config
            .database_root
            .join("engine.db")
            .to_string_lossy()
            .to_string()
    });
    let engine_db_config = etl::engine_db::EngineDbConfig {
        path: engine_db_path,
    };
    let engine_db = etl::EngineDb::new(engine_db_config).await?;
    let engine_db = Arc::new(engine_db);

    // Create extractor early so we can get the provider for contract identification
    let extractor: Box<dyn Extractor> = if let Some(extractor) = config.extractor {
        tracing::info!(target: "torii::etl", "Using configured extractor");
        extractor
    } else {
        tracing::info!(target: "torii::etl", "No extractor configured, using SampleExtractor for testing");
        if config.sample_events.is_empty() {
            tracing::warn!(target: "torii::etl", "No sample events provided, ETL loop will idle");
        } else {
            tracing::info!(
                target: "torii::etl",
                "Loaded {} sample event types (will cycle through them)",
                config.sample_events.len()
            );
        }
        Box::new(SampleExtractor::new(
            config.sample_events,
            config.events_per_cycle,
        ))
    };

    // Create DecoderContext with contract filtering and optional registry
    let decoder_context = if let Some(registry_cache) = config.registry_cache {
        tracing::info!(
            target: "torii::etl",
            "Creating DecoderContext with registry cache (auto-identification enabled)"
        );
        DecoderContext::with_registry(
            config.decoders,
            engine_db.clone(),
            config.contract_filter,
            registry_cache,
        )
    } else {
        tracing::info!(
            target: "torii::etl",
            "Creating DecoderContext without registry (all decoders for unmapped contracts)"
        );
        DecoderContext::new(config.decoders, engine_db.clone(), config.contract_filter)
    };

    let topics = multi_sink.topics();

    let grpc_state = GrpcState::new(subscription_manager.clone(), topics);
    let grpc_service = create_grpc_service(grpc_state);

    let has_user_grpc_services = config.partial_grpc_router.is_some();
    let mut grpc_router = if let Some(partial_router) = config.partial_grpc_router {
        tracing::info!(target: "torii::main", "Using user-provided gRPC router with sink services");
        partial_router.add_service(tonic_web::enable(grpc_service))
    } else {
        Server::builder()
            // Accept HTTP/1.1 requests required for gRPC-Web to work.
            .accept_http1(true)
            .add_service(tonic_web::enable(grpc_service))
    };

    if config.custom_reflection {
        tracing::info!(target: "torii::main", "Using custom reflection services (user-provided)");
    } else {
        let reflection_v1 = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?
            .accept_compressed(CompressionEncoding::Gzip);

        let reflection_v1alpha = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1alpha()?
            .accept_compressed(CompressionEncoding::Gzip);

        grpc_router = grpc_router
            .add_service(tonic_web::enable(reflection_v1))
            .add_service(tonic_web::enable(reflection_v1alpha));

        tracing::info!(target: "torii::main", "Added reflection services (core descriptors only)");
    }

    let sinks_routes = multi_sink.build_routes();
    let http_router = create_http_router().merge(sinks_routes);

    let cors = CorsLayer::new()
        .allow_origin(CorsAny)
        .allow_methods(CorsAny)
        .allow_headers(CorsAny)
        .expose_headers(vec![
            axum::http::HeaderName::from_static("grpc-status"),
            axum::http::HeaderName::from_static("grpc-message"),
            axum::http::HeaderName::from_static("grpc-status-details-bin"),
            axum::http::HeaderName::from_static("x-grpc-web"),
            axum::http::HeaderName::from_static("content-type"),
        ]);

    // Until some compatibility issues are resolved with axum, we need to allow this deprecated code.
    // See: https://github.com/hyperium/tonic/issues/1964.
    #[allow(warnings, deprecated)]
    let app = AxumRouter::new()
        .merge(grpc_router.into_router())
        .merge(http_router)
        .layer(cors);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    let tls_acceptor = config.tls.as_ref().map(build_tls_acceptor).transpose()?;
    tracing::info!(target: "torii::main", "Server listening on {}", addr);
    if let Some(tls) = &config.tls {
        tracing::info!(
            target: "torii::main",
            cert = %tls.cert_path.display(),
            key = %tls.key_path.display(),
            alpn = ?tls.alpn_names(),
            "TLS enabled for listener"
        );
    }

    tracing::info!(target: "torii::main", "gRPC Services:");
    tracing::info!(target: "torii::main", "   torii.Torii - Core service");
    if has_user_grpc_services {
        tracing::info!(target: "torii::main", "   + User-provided sink gRPC services");
    }

    // Create cancellation token for graceful shutdown coordination.
    let shutdown_token = CancellationToken::new();

    // Setup and start the ETL pipeline.
    let etl_multi_sink = multi_sink.clone();
    let etl_engine_db = engine_db.clone();
    let cycle_interval = config.cycle_interval;
    let etl_shutdown_token = shutdown_token.clone();
    let etl_concurrency = config.etl_concurrency.clone();

    // Move multi_decoder into the task (can't clone since it owns the registry)
    let etl_decoder_context = decoder_context;

    // Optional contract identifier for runtime identification
    let contract_identifier = config.contract_identifier;

    // Extractor was already created earlier (to get provider), make it mutable for the ETL loop
    let extractor = Arc::new(tokio::sync::Mutex::new(extractor));

    let etl_handle = tokio::spawn(async move {
        tracing::info!(target: "torii::etl", "Starting ETL pipeline...");

        // Wait a bit for the server to be ready.
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        #[derive(Debug)]
        struct PrefetchedBatch {
            batch: etl::extractor::ExtractionBatch,
            cursor: Option<String>,
            extractor_finished: bool,
        }

        let prefetch_capacity = etl_concurrency.resolved_prefetch_batches();
        let (prefetch_tx, mut prefetch_rx) =
            tokio::sync::mpsc::channel::<PrefetchedBatch>(prefetch_capacity);
        let queue_depth = Arc::new(AtomicUsize::new(0));

        let (identify_tx, identify_handle) = if let Some(identifier) = contract_identifier.clone() {
            let (tx, mut rx) =
                tokio::sync::mpsc::channel::<Vec<Felt>>(prefetch_capacity.saturating_mul(2).max(8));
            let handle = tokio::spawn(async move {
                while let Some(contract_addresses) = rx.recv().await {
                    if contract_addresses.is_empty() {
                        continue;
                    }

                    let identify_start = std::time::Instant::now();
                    if let Err(e) = identifier.identify_contracts(&contract_addresses).await {
                        tracing::warn!(
                            target: "torii::etl",
                            error = %e,
                            "Contract identification failed"
                        );
                    }
                    ::metrics::histogram!("torii_registry_identify_duration_seconds")
                        .record(identify_start.elapsed().as_secs_f64());
                }
            });
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        let producer_extractor = extractor.clone();
        let producer_engine_db = etl_engine_db.clone();
        let producer_shutdown = etl_shutdown_token.clone();
        let producer_identify_tx = identify_tx.clone();
        let producer_queue_depth = queue_depth.clone();

        let producer_handle = tokio::spawn(async move {
            let mut cursor: Option<String> = None;

            loop {
                if producer_shutdown.is_cancelled() {
                    tracing::info!(target: "torii::etl", "Shutdown requested, stopping prefetch producer");
                    break;
                }

                let batch = {
                    let mut extractor = producer_extractor.lock().await;
                    extractor.extract(cursor.clone(), &producer_engine_db).await
                };

                let batch = match batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        tracing::error!(target: "torii::etl", "Extract failed: {}", e);
                        ::metrics::counter!("torii_etl_cycle_total", "status" => "extract_error")
                            .increment(1);
                        if producer_shutdown.is_cancelled() {
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(cycle_interval)).await;
                        continue;
                    }
                };

                let should_pause = batch.is_empty();
                let new_cursor = batch.cursor.clone();

                if let Some(ref identify_tx) = producer_identify_tx {
                    let contract_addresses: Vec<Felt> = batch
                        .events
                        .iter()
                        .map(|event| event.from_address)
                        .collect::<std::collections::HashSet<_>>()
                        .into_iter()
                        .collect();

                    if !contract_addresses.is_empty() {
                        match identify_tx.try_send(contract_addresses) {
                            Ok(()) => {
                                ::metrics::counter!(
                                    "torii_registry_identify_jobs_total",
                                    "status" => "enqueued"
                                )
                                .increment(1);
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                ::metrics::counter!(
                                    "torii_registry_identify_jobs_total",
                                    "status" => "dropped"
                                )
                                .increment(1);
                                tracing::debug!(
                                    target: "torii::etl",
                                    "Contract identify queue full, skipping identify for this batch"
                                );
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                ::metrics::counter!(
                                    "torii_registry_identify_jobs_total",
                                    "status" => "closed"
                                )
                                .increment(1);
                            }
                        }
                    }
                }

                let extractor_finished = {
                    let extractor = producer_extractor.lock().await;
                    extractor.is_finished()
                };

                let stall_start = std::time::Instant::now();
                if prefetch_tx
                    .send(PrefetchedBatch {
                        batch,
                        cursor: new_cursor.clone(),
                        extractor_finished,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
                ::metrics::histogram!("torii_etl_prefetch_stall_seconds")
                    .record(stall_start.elapsed().as_secs_f64());
                producer_queue_depth.fetch_add(1, Ordering::Relaxed);
                ::metrics::gauge!("torii_etl_prefetch_queue_depth")
                    .set(producer_queue_depth.load(Ordering::Relaxed) as f64);

                cursor = new_cursor;

                if extractor_finished {
                    break;
                }

                if let Some(prefetched_cursor) = &cursor {
                    tracing::trace!(
                        target: "torii::etl",
                        cursor = prefetched_cursor,
                        "Prefetched ETL batch"
                    );
                }

                if prefetch_tx.is_closed() || producer_shutdown.is_cancelled() {
                    break;
                }

                if let Some(ref prefetched_cursor) = cursor {
                    tracing::trace!(
                        target: "torii::etl",
                        cursor = prefetched_cursor,
                        "Advanced producer cursor"
                    );
                }

                if should_pause {
                    tokio::time::sleep(tokio::time::Duration::from_secs(cycle_interval)).await;
                }
            }
        });

        let mut committed_cursor: Option<String> = None;
        let mut shutdown_requested = false;

        loop {
            ::metrics::gauge!("torii_etl_inflight_cycles").set(1.0);

            let wait_start = std::time::Instant::now();
            let next_batch = if shutdown_requested {
                prefetch_rx.recv().await
            } else {
                tokio::select! {
                    maybe_batch = prefetch_rx.recv() => maybe_batch,
                    () = etl_shutdown_token.cancelled() => {
                        shutdown_requested = true;
                        continue;
                    }
                }
            };
            ::metrics::histogram!("torii_etl_prefetch_stall_seconds")
                .record(wait_start.elapsed().as_secs_f64());

            let Some(prefetched) = next_batch else {
                ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                break;
            };
            queue_depth.fetch_sub(1, Ordering::Relaxed);
            ::metrics::gauge!("torii_etl_prefetch_queue_depth")
                .set(queue_depth.load(Ordering::Relaxed) as f64);

            let cycle_start = std::time::Instant::now();
            let batch = prefetched.batch;
            let new_cursor = prefetched.cursor;

            if batch.is_empty() {
                if let Some(ref cursor_str) = new_cursor {
                    if committed_cursor.as_ref() != Some(cursor_str) {
                        let commit_result = {
                            let mut extractor = extractor.lock().await;
                            extractor.commit_cursor(cursor_str, &etl_engine_db).await
                        };
                        if let Err(e) = commit_result {
                            tracing::error!(
                                target: "torii::etl",
                                "Failed to commit cursor for empty batch: {}",
                                e
                            );
                            ::metrics::counter!("torii_cursor_commit_failures_total").increment(1);
                        } else {
                            committed_cursor.clone_from(&new_cursor);
                        }
                    }
                }

                if prefetched.extractor_finished {
                    tracing::info!(target: "torii::etl", "Extractor finished, stopping ETL loop");
                    ::metrics::counter!("torii_etl_cycle_total", "status" => "empty").increment(1);
                    ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                        .record(cycle_start.elapsed().as_secs_f64());
                    ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                    break;
                }

                ::metrics::counter!("torii_etl_cycle_total", "status" => "empty").increment(1);
                ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                    .record(cycle_start.elapsed().as_secs_f64());
                ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);

                if shutdown_requested {
                    continue;
                }
                continue;
            }

            tracing::info!(
                target: "torii::etl",
                "Extracted {} events",
                batch.len()
            );
            ::metrics::counter!("torii_events_extracted_total").increment(batch.len() as u64);
            ::metrics::counter!("torii_tx_processed_total")
                .increment(batch.transactions.len() as u64);
            ::metrics::counter!("torii_extract_batch_size_total", "unit" => "events")
                .increment(batch.events.len() as u64);
            ::metrics::counter!("torii_extract_batch_size_total", "unit" => "blocks")
                .increment(batch.blocks.len() as u64);
            ::metrics::counter!("torii_extract_batch_size_total", "unit" => "transactions")
                .increment(batch.transactions.len() as u64);

            // Update the engine DB stats for now here. Temporary.
            let latest_block = batch.blocks.keys().max().copied().unwrap_or(0);
            if let Err(e) = etl_engine_db
                .update_head(latest_block, batch.len() as u64)
                .await
            {
                tracing::warn!(target: "torii::etl", "Failed to update engine DB: {}", e);
            }

            // Transform the events into envelopes.
            let envelopes = match etl_decoder_context.decode_events(&batch.events).await {
                Ok(envelopes) => envelopes,
                Err(e) => {
                    tracing::error!(target: "torii::etl", "Decode failed: {}", e);
                    ::metrics::counter!("torii_decode_failures_total", "stage" => "decode")
                        .increment(1);
                    ::metrics::counter!("torii_etl_cycle_total", "status" => "decode_error")
                        .increment(1);
                    ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                        .record(cycle_start.elapsed().as_secs_f64());
                    ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                    continue;
                }
            };
            ::metrics::counter!("torii_events_decoded_total").increment(batch.events.len() as u64);
            ::metrics::counter!("torii_decode_envelopes_total").increment(envelopes.len() as u64);

            // Load the envelopes into the sinks.
            if let Err(e) = etl_multi_sink.process(&envelopes, &batch).await {
                tracing::error!(target: "torii::etl", "Sink processing failed: {}", e);
                ::metrics::counter!("torii_etl_cycle_total", "status" => "sink_error").increment(1);
                ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                    .record(cycle_start.elapsed().as_secs_f64());
                ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);
                continue;
            }

            // Count successfully processed payloads (post-sink processing).
            ::metrics::counter!("torii_events_processed_total")
                .increment(batch.events.len() as u64);
            ::metrics::counter!("torii_transactions_processed_total")
                .increment(batch.transactions.len() as u64);

            // CRITICAL: Commit cursor ONLY AFTER successful sink processing.
            // This ensures no data loss if the process is killed during extraction or sink processing.
            if let Some(ref cursor_str) = new_cursor {
                let commit_result = {
                    let mut extractor = extractor.lock().await;
                    extractor.commit_cursor(cursor_str, &etl_engine_db).await
                };
                if let Err(e) = commit_result {
                    tracing::error!(target: "torii::etl", "Failed to commit cursor: {}", e);
                    ::metrics::counter!("torii_cursor_commit_failures_total").increment(1);
                    // Continue anyway - cursor will be re-processed on restart (safe, just duplicate work)
                } else {
                    committed_cursor.clone_from(&new_cursor);
                }
            }

            if let Some(chain_head) = batch.chain_head {
                let gap = chain_head.saturating_sub(latest_block);
                ::metrics::gauge!("torii_etl_cycle_gap_blocks").set(gap as f64);
            }
            ::metrics::gauge!("torii_etl_last_success_timestamp_seconds")
                .set(chrono::Utc::now().timestamp() as f64);
            ::metrics::counter!("torii_etl_cycle_total", "status" => "ok").increment(1);
            ::metrics::histogram!("torii_etl_cycle_duration_seconds")
                .record(cycle_start.elapsed().as_secs_f64());
            ::metrics::gauge!("torii_etl_inflight_cycles").set(0.0);

            tracing::info!(target: "torii::etl", "ETL cycle complete");
        }

        if let Err(e) = producer_handle.await {
            tracing::warn!(target: "torii::etl", error = %e, "Prefetch producer join failed");
        }
        drop(identify_tx);
        if let Some(handle) = identify_handle {
            if let Err(e) = handle.await {
                tracing::warn!(target: "torii::etl", error = %e, "Identifier worker join failed");
            }
        }
        tracing::info!(target: "torii::etl", "ETL loop completed gracefully");
    });

    // Setup signal handlers for graceful shutdown
    let server_shutdown_token = shutdown_token.clone();
    let shutdown_timeout = config.shutdown_timeout;

    let shutdown_signal = async move {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {
                tracing::info!(target: "torii::main", "Received SIGINT (Ctrl+C), initiating graceful shutdown...");
            }
            () = terminate => {
                tracing::info!(target: "torii::main", "Received SIGTERM, initiating graceful shutdown...");
            }
        }

        // Signal shutdown to ETL loop
        server_shutdown_token.cancel();
    };

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let mut make_svc = app.into_make_service();
    let http = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());

    let server = async move {
        tokio::pin!(shutdown_signal);
        loop {
            let (tcp, _remote_addr) = tokio::select! {
                result = listener.accept() => match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!(target: "torii::main", "Accept error: {}", e);
                        continue;
                    }
                },
                () = &mut shutdown_signal => break,
            };
            let tower_service = make_svc.call(()).await.expect("infallible");
            let hyper_service = hyper_util::service::TowerToHyperService::new(tower_service);
            let builder = http.clone();
            let tls_acceptor = tls_acceptor.clone();
            tokio::spawn(async move {
                if let Some(tls_acceptor) = tls_acceptor {
                    let tls_stream = match tls_acceptor.accept(tcp).await {
                        Ok(stream) => stream,
                        Err(err) => {
                            tracing::debug!(
                                target: "torii::main",
                                error = %err,
                                "TLS handshake failed"
                            );
                            return;
                        }
                    };

                    if let Err(err) = builder
                        .serve_connection_with_upgrades(
                            hyper_util::rt::TokioIo::new(tls_stream),
                            hyper_service,
                        )
                        .await
                    {
                        tracing::debug!(target: "torii::main", error = %err, "Connection ended");
                    }
                } else if let Err(err) = builder
                    .serve_connection_with_upgrades(
                        hyper_util::rt::TokioIo::new(tcp),
                        hyper_service,
                    )
                    .await
                {
                    tracing::debug!(target: "torii::main", error = %err, "Connection ended");
                }
            });
        }
        Ok::<_, std::io::Error>(())
    };

    // Give active connections 15 seconds to close gracefully, then force shutdown.
    // This prevents hanging on long-lived gRPC streaming connections.
    const SERVER_SHUTDOWN_TIMEOUT_SECS: u64 = 15;
    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!(target: "torii::main", "Server error: {}", e);
            }
        }
        () = async {
            // Wait for shutdown signal + timeout
            shutdown_token.cancelled().await;
            tokio::time::sleep(Duration::from_secs(SERVER_SHUTDOWN_TIMEOUT_SECS)).await;
        } => {
            tracing::warn!(
                target: "torii::main",
                "Server connections did not close within {}s, forcing shutdown",
                SERVER_SHUTDOWN_TIMEOUT_SECS
            );
        }
    }

    tracing::info!(target: "torii::main", "HTTP/gRPC server stopped, waiting for ETL loop to complete...");

    // Wait for ETL loop to finish with timeout
    match tokio::time::timeout(Duration::from_secs(shutdown_timeout), etl_handle).await {
        Ok(Ok(())) => {
            tracing::info!(target: "torii::main", "ETL loop completed successfully");
        }
        Ok(Err(e)) => {
            tracing::error!(target: "torii::main", "ETL loop panicked: {}", e);
        }
        Err(_) => {
            tracing::warn!(
                target: "torii::main",
                "ETL loop did not complete within {}s timeout, forcing shutdown",
                shutdown_timeout
            );
        }
    }

    tracing::info!(target: "torii::main", "Torii shutdown complete");
    command_bus.shutdown().await;

    Ok(())
}

fn build_tls_acceptor(
    config: &ToriiTlsConfig,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error>> {
    ensure_rustls_crypto_provider();
    let cert_chain = load_cert_chain(&config.cert_path)?;
    let private_key = load_private_key(&config.key_path)?;

    let mut server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)?;
    server_config
        .alpn_protocols
        .clone_from(&config.alpn_protocols);

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
}

fn ensure_rustls_crypto_provider() {
    if tokio_rustls::rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    }
}

fn load_cert_chain(
    path: &Path,
) -> Result<Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>, Box<dyn std::error::Error>>
{
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, io::Error>>()?;

    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates found in {}", path.display()),
        )
        .into());
    }

    Ok(certs)
}

fn load_private_key(
    path: &Path,
) -> Result<tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let private_key = rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in {}", path.display()),
        )
    })?;

    Ok(private_key)
}
