//! Torii ERC20 - Starknet ERC20 Token Indexer
//!
//! A production-ready indexer for ERC20 tokens on Starknet.
//!
//! # Features
//!
//! - Explicit contract mapping (ETH, STRK, custom tokens)
//! - Auto-discovery of ERC20 contracts (can be disabled)
//! - Real-time transfer and approval tracking
//! - SQLite persistence
//! - gRPC subscriptions for real-time updates
//! - Historical queries with pagination
//!
//! # Usage
//!
//! ```bash
//! # Index from block 100,000 with auto-discovery
//! torii-erc20 --from-block 100000
//!
//! # Strict mode: only index ETH and STRK (no auto-discovery)
//! torii-erc20 --no-auto-discovery
//!
//! # Index specific contracts
//! torii-erc20 --contracts 0x123...,0x456... --no-auto-discovery
//! ```

mod config;

use anyhow::Result;
use clap::Parser;
use config::Config;
use starknet::core::types::Felt;
use std::sync::Arc;
#[cfg(feature = "profiling")]
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::codec::CompressionEncoding;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{BlockRangeConfig, BlockRangeExtractor};
use torii_runtime_common::database::resolve_single_db_setup;
#[cfg(feature = "profiling")]
use torii_sql::DbBackend;

// Import from the library crate
use torii_erc20::proto::erc20_server::Erc20Server;
use torii_erc20::{
    Erc20Decoder, Erc20MetadataCommandHandler, Erc20Service, Erc20Sink, Erc20Storage,
    FILE_DESCRIPTOR_SET,
};

const ERC20_METADATA_COMMAND_PARALLELISM: usize = 1;
const ERC20_METADATA_COMMAND_QUEUE_SIZE: usize = 4096;
const ERC20_METADATA_MAX_RETRIES: u8 = 3;

#[tokio::main]
async fn main() -> Result<()> {
    // Start profiler if enabled
    #[cfg(feature = "profiling")]
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    // Parse CLI arguments
    let config = Config::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    tracing::info!("Starting Torii ERC20 Indexer");
    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("From block: {}", config.from_block);
    tracing::info!("Database: {}", config.db_path);
    tracing::info!("ETL cycle interval: {}s", config.cycle_interval);
    if let Some(url) = &config.database_url {
        tracing::info!("Engine database URL: {}", url);
    }
    tracing::info!(
        "Auto-discovery: {}",
        if config.no_auto_discovery {
            "disabled"
        } else {
            "enabled"
        }
    );

    // Create storage
    let db_setup = resolve_single_db_setup(&config.db_path, config.database_url.as_deref());
    let storage = Arc::new(Erc20Storage::new(&db_setup.storage_url).await?);
    tracing::info!("Database initialized");

    // Create Starknet provider
    let provider = Arc::new(starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    ));

    // Create extractor
    let extractor_config = BlockRangeConfig {
        rpc_url: config.rpc_url.clone(),
        from_block: config.from_block,
        to_block: config.to_block,
        batch_size: 50,
        retry_policy: torii::etl::extractor::RetryPolicy::default(),
        rpc_parallelism: 0,
    };

    let extractor = Box::new(BlockRangeExtractor::new(provider.clone(), extractor_config));
    tracing::info!("Extractor configured");

    // Create decoder
    let decoder = Arc::new(Erc20Decoder::new());

    // Create gRPC service
    let grpc_service = Erc20Service::new(storage.clone());

    // Create sink with gRPC service for dual publishing
    let sink = Box::new(
        Erc20Sink::new(storage.clone())
            .with_grpc_service(grpc_service.clone())
            .with_metadata_pipeline(
                ERC20_METADATA_COMMAND_PARALLELISM,
                ERC20_METADATA_COMMAND_QUEUE_SIZE,
                ERC20_METADATA_MAX_RETRIES,
            ),
    );

    // Build gRPC router with Erc20 service and reflection
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("Failed to build gRPC reflection service")
        .accept_compressed(CompressionEncoding::Gzip);

    let grpc_service = Erc20Server::new(grpc_service).accept_compressed(CompressionEncoding::Gzip);

    let grpc_router = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .add_service(reflection);

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .database_root(&db_setup.database_root)
        .command_bus_queue_size(ERC20_METADATA_COMMAND_QUEUE_SIZE)
        .cycle_interval(config.cycle_interval)
        .engine_database_url(db_setup.engine_url.clone())
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(sink)
        .with_command_handler(Box::new(Erc20MetadataCommandHandler::new(
            provider.clone(),
            storage.clone(),
            ERC20_METADATA_MAX_RETRIES,
        )))
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true); // We've added our own reflection

    // Add well-known contracts (ETH, STRK) with explicit mappings
    let erc20_decoder_id = DecoderId::new("erc20");
    for (address, name) in config.well_known_contracts() {
        tracing::info!("Mapping {} at {:#x} to ERC20 decoder", name, address);
        torii_config = torii_config.map_contract(address, vec![erc20_decoder_id]);
    }

    // Add custom contracts with explicit mappings
    for contract_str in &config.contracts {
        let address = Felt::from_hex(contract_str)?;
        tracing::info!("Mapping custom contract {:#x} to ERC20 decoder", address);
        torii_config = torii_config.map_contract(address, vec![erc20_decoder_id]);
    }

    // Log mode: no_auto_discovery affects whether unmapped contracts are tried
    if config.no_auto_discovery {
        tracing::info!("Strict mode: ONLY explicitly mapped contracts will be indexed");
        tracing::info!("  (Unmapped contracts will NOT be tried with ERC20 decoder)");
    } else {
        tracing::info!(
            "Auto-discovery enabled: unmapped contracts will be tried with ERC20 decoder"
        );
    }

    let torii_config = torii_config.build();

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("gRPC service available at localhost:{}", config.port);
    tracing::info!("  - torii.Torii (EventBus subscriptions)");
    tracing::info!("  - torii.sinks.erc20.Erc20 (queries and rich subscriptions)");

    // Run Torii (blocks until shutdown)
    torii::run(torii_config)
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;

    // Print final statistics
    tracing::info!("Final Statistics:");
    if let Ok(transfer_count) = storage.get_transfer_count().await {
        tracing::info!("  Total transfers: {}", transfer_count);
    }
    if let Ok(approval_count) = storage.get_approval_count().await {
        tracing::info!("  Total approvals: {}", approval_count);
    }
    if let Ok(token_count) = storage.get_token_count().await {
        tracing::info!("  Unique tokens: {}", token_count);
    }
    if let Ok(Some(latest_block)) = storage.get_latest_block().await {
        tracing::info!("  Latest block: {}", latest_block);
    }

    // Generate flamegraph if profiling enabled
    #[cfg(feature = "profiling")]
    {
        if let Ok(report) = guard.report().build() {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let db_backend = db_setup
                .storage_url
                .parse::<DbBackend>()
                .map_err(anyhow::Error::new)?;
            let filename = format!("flamegraph-torii-erc20-block-range-{db_backend}-{ts}.svg");
            let file = std::fs::File::create(&filename).unwrap();
            report.flamegraph(file).unwrap();
            tracing::info!("Flamegraph generated: {}", filename);
        }
    }

    Ok(())
}
