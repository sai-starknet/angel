//! Torii Tokens - Unified Starknet Token Indexer
//!
//! A production-ready indexer for ERC20, ERC721, and ERC1155 tokens on Starknet.
//!
//! # Features
//!
//! - Explicit contract mapping with token type specification
//! - ERC20: Transfer and approval tracking
//! - ERC721: NFT ownership tracking with transfer history
//! - ERC1155: Semi-fungible token transfer tracking (single and batch)
//! - SQLite persistence with separate tables per token type
//! - gRPC subscriptions for real-time updates
//! - Historical queries with pagination
//!
//! # Extraction Modes
//!
//! - **block-range** (default): Fetches ALL events from each block.
//!   Single global cursor. Best for full chain indexing.
//!
//! - **event**: Uses `starknet_getEvents` with per-contract cursors.
//!   Easy to add new contracts without re-indexing existing ones.
//!
//! # Usage
//!
//! ```bash
//! # Block range mode (default) - full chain indexing
//! torii-tokens --include-well-known --from-block 0
//!
//! # Enable observability (metrics endpoint + collection)
//! torii-tokens --observability --include-well-known --from-block 0
//!
//! # Event mode - per-contract cursors
//! torii-tokens --mode event --erc20 0x...ETH,0x...STRK --from-block 0
//!
//! # Add a new contract in event mode (just restart with updated list)
//! torii-tokens --mode event --erc20 0x...ETH,0x...STRK,0x...USDC --from-block 0
//! # USDC starts from block 0, ETH and STRK resume from their cursors
//! ```

mod config;

use anyhow::Result;
use clap::Parser;
use config::{Config, ExtractionMode, MetadataMode};
use starknet::core::types::Felt;
use starknet::providers::Provider;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "profiling")]
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::codec::CompressionEncoding;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{
    BlockRangeConfig, BlockRangeExtractor, ContractEventConfig, EventExtractor,
    EventExtractorConfig, Extractor, GlobalEventExtractor, GlobalEventExtractorConfig, RetryPolicy,
};
use torii::etl::identification::ContractRegistry;
use torii::EtlConcurrencyConfig;
use torii_common::{MetadataFetcher, TokenUriService};
use torii_config_common::apply_observability_env;
use torii_runtime_common::database::resolve_token_db_setup;
#[cfg(feature = "profiling")]
use torii_sql::DbBackend;

// Import from ERC20 library crate
use torii_erc20::proto::erc20_server::Erc20Server;
use torii_erc20::{
    Erc20Decoder, Erc20MetadataCommandHandler, Erc20Rule, Erc20Service, Erc20Sink, Erc20Storage,
    FILE_DESCRIPTOR_SET as ERC20_DESCRIPTOR_SET,
};

use torii_erc721::proto::erc721_server::Erc721Server;
use torii_erc721::{
    Erc721Decoder, Erc721MetadataCommandHandler, Erc721Rule, Erc721Service, Erc721Sink,
    Erc721Storage, FILE_DESCRIPTOR_SET as ERC721_DESCRIPTOR_SET,
};

// Import from ERC1155 library crate
use torii_erc1155::proto::erc1155_server::Erc1155Server;
use torii_erc1155::{
    Erc1155Decoder, Erc1155MetadataCommandHandler, Erc1155Rule, Erc1155Service, Erc1155Sink,
    Erc1155Storage, FILE_DESCRIPTOR_SET as ERC1155_DESCRIPTOR_SET,
};

async fn contracts_from_registry(
    engine_db: &torii::etl::EngineDb,
) -> Result<(Vec<Felt>, Vec<Felt>, Vec<Felt>)> {
    let mappings = engine_db.get_all_contract_decoders().await?;
    let erc20_id = DecoderId::new("erc20");
    let erc721_id = DecoderId::new("erc721");
    let erc1155_id = DecoderId::new("erc1155");

    let mut erc20 = Vec::new();
    let mut erc721 = Vec::new();
    let mut erc1155 = Vec::new();

    for (contract, decoder_ids, _) in mappings {
        if decoder_ids.contains(&erc20_id) {
            erc20.push(contract);
        }
        if decoder_ids.contains(&erc721_id) {
            erc721.push(contract);
        }
        if decoder_ids.contains(&erc1155_id) {
            erc1155.push(contract);
        }
    }

    Ok((erc20, erc721, erc1155))
}

fn extend_unique(target: &mut Vec<Felt>, additions: Vec<Felt>) {
    let mut seen: HashSet<Felt> = target.iter().copied().collect();
    for addr in additions {
        if seen.insert(addr) {
            target.push(addr);
        }
    }
}

async fn bootstrap_registry_for_event_mode(
    provider: Arc<
        starknet::providers::jsonrpc::JsonRpcClient<starknet::providers::jsonrpc::HttpTransport>,
    >,
    engine_db: &torii::etl::EngineDb,
    registry: &ContractRegistry,
    config: &Config,
) -> Result<usize> {
    let chain_head = provider
        .block_number()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch chain head for bootstrap: {e}"))?;

    let max_to = config
        .from_block
        .saturating_add(config.event_bootstrap_blocks.saturating_sub(1));
    let configured_to = config.to_block.unwrap_or(u64::MAX);
    let bootstrap_to = chain_head.min(max_to).min(configured_to);

    if config.from_block > bootstrap_to {
        tracing::warn!(
            target: "torii_tokens",
            from_block = config.from_block,
            bootstrap_to,
            chain_head,
            "Skipping bootstrap discovery: from_block is above available bootstrap range"
        );
        return Ok(0);
    }

    tracing::info!(
        target: "torii_tokens",
        from_block = config.from_block,
        to_block = bootstrap_to,
        batch_size = config.batch_size,
        "Starting automatic event-mode bootstrap discovery"
    );

    let mut extractor = BlockRangeExtractor::new(
        provider,
        BlockRangeConfig {
            rpc_url: config.rpc_url.clone(),
            from_block: config.from_block,
            to_block: Some(bootstrap_to),
            batch_size: config.batch_size,
            retry_policy: RetryPolicy::default(),
            rpc_parallelism: config.rpc_parallelism,
        },
    );

    let mut first_extract = true;
    let mut identified_total = 0usize;

    loop {
        let cursor = if first_extract {
            first_extract = false;
            Some(format!("block:{}", config.from_block.saturating_sub(1)))
        } else {
            None
        };

        let batch = extractor.extract(cursor, engine_db).await?;
        if batch.is_empty() {
            if extractor.is_finished() {
                break;
            }
            continue;
        }

        let contract_addresses: Vec<Felt> = batch
            .events
            .iter()
            .map(|event| event.from_address)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if !contract_addresses.is_empty() {
            let identified = registry.identify_contracts(&contract_addresses).await?;
            identified_total += identified.len();
        }

        if extractor.is_finished() {
            break;
        }
    }

    tracing::info!(
        target: "torii_tokens",
        identified_total,
        "Completed automatic event-mode bootstrap discovery"
    );

    Ok(identified_total)
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    let config = Config::parse();
    run_indexer(config).await
}

/// Run the main indexer
async fn run_indexer(config: Config) -> Result<()> {
    #[cfg(feature = "profiling")]
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    tracing::info!("Starting Torii Unified Token Indexer");
    apply_observability_env(config.observability);
    if config.observability {
        tracing::info!("Observability: enabled via --observability");
    } else {
        tracing::info!("Observability: disabled (flag not set)");
    }
    tracing::info!("Mode: {:?}", config.mode);
    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("From block: {}", config.from_block);
    if let Some(to_block) = config.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Database directory: {}", config.db_dir);
    if let Some(url) = &config.database_url {
        tracing::info!("Engine database URL: {}", url);
    }
    if let Some(url) = &config.storage_database_url {
        tracing::info!("Storage database URL: {}", url);
    }

    if config.mode == ExtractionMode::BlockRange || config.mode == ExtractionMode::GlobalEvent {
        if config.has_tokens() {
            tracing::info!(
                "Global extraction mode with explicit contracts (will also auto-discover)"
            );
        } else {
            tracing::info!("Global extraction mode with full auto-discovery enabled");
        }
    }

    let provider = Arc::new(starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    ));

    let mut all_erc20_addresses: Vec<Felt> = Vec::new();
    let mut all_erc721_addresses: Vec<Felt> = Vec::new();
    let mut all_erc1155_addresses: Vec<Felt> = Vec::new();

    if config.include_well_known {
        for (address, name) in Config::well_known_erc20_contracts() {
            tracing::info!("Adding well-known ERC20: {} at {:#x}", name, address);
            all_erc20_addresses.push(address);
        }
    }

    for addr_str in &config.erc20 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC20 contract: {:#x}", address);
        all_erc20_addresses.push(address);
    }

    for addr_str in &config.erc721 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC721 contract: {:#x}", address);
        all_erc721_addresses.push(address);
    }

    for addr_str in &config.erc1155 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC1155 contract: {:#x}", address);
        all_erc1155_addresses.push(address);
    }

    let db_dir = Path::new(&config.db_dir);
    std::fs::create_dir_all(db_dir)?;

    let effective_metadata_mode = config.metadata_mode.clone().unwrap_or(MetadataMode::Inline);
    tracing::info!("Metadata mode: {:?}", effective_metadata_mode);

    if config.metadata_backfill_only {
        tracing::warn!(
            "--metadata-backfill-only is set. Dedicated metadata backfill flow is not implemented yet; exiting."
        );
        return Ok(());
    }

    let db_setup = resolve_token_db_setup(
        db_dir,
        config.database_url.as_deref(),
        config.storage_database_url.as_deref(),
    )?;

    tracing::info!(
        "Engine backend: {:?} ({})",
        db_setup.engine_backend,
        db_setup.engine_url
    );
    tracing::info!(
        "ERC20 storage backend: {:?} ({})",
        db_setup.erc20_backend,
        db_setup.erc20_url
    );
    tracing::info!(
        "ERC721 storage backend: {:?} ({})",
        db_setup.erc721_backend,
        db_setup.erc721_url
    );
    tracing::info!(
        "ERC1155 storage backend: {:?} ({})",
        db_setup.erc1155_backend,
        db_setup.erc1155_url
    );

    let engine_db_config = torii::etl::engine_db::EngineDbConfig {
        path: db_setup.engine_url.clone(),
    };
    let engine_db = Arc::new(torii::etl::EngineDb::new(engine_db_config).await?);

    let registry = Arc::new(
        ContractRegistry::new(provider.clone(), engine_db.clone())
            .with_rpc_parallelism(config.rpc_parallelism)
            .with_rule(Box::new(Erc20Rule::new()))
            .with_rule(Box::new(Erc721Rule::new()))
            .with_rule(Box::new(Erc1155Rule::new())),
    );

    // Load any previously identified contracts from database
    let loaded_count = registry.load_from_db().await?;
    if loaded_count > 0 {
        tracing::info!("Loaded {} contract mappings from database", loaded_count);
    }

    if config.mode == ExtractionMode::Event {
        let (reg_erc20, reg_erc721, reg_erc1155) = contracts_from_registry(&engine_db).await?;
        let before = (
            all_erc20_addresses.len(),
            all_erc721_addresses.len(),
            all_erc1155_addresses.len(),
        );
        extend_unique(&mut all_erc20_addresses, reg_erc20);
        extend_unique(&mut all_erc721_addresses, reg_erc721);
        extend_unique(&mut all_erc1155_addresses, reg_erc1155);

        let added_from_registry = (
            all_erc20_addresses.len().saturating_sub(before.0),
            all_erc721_addresses.len().saturating_sub(before.1),
            all_erc1155_addresses.len().saturating_sub(before.2),
        );
        if added_from_registry.0 + added_from_registry.1 + added_from_registry.2 > 0 {
            tracing::info!(
                target: "torii_tokens",
                erc20 = added_from_registry.0,
                erc721 = added_from_registry.1,
                erc1155 = added_from_registry.2,
                "Loaded event-mode contracts from registry"
            );
        }

        if all_erc20_addresses.is_empty()
            && all_erc721_addresses.is_empty()
            && all_erc1155_addresses.is_empty()
        {
            let identified =
                bootstrap_registry_for_event_mode(provider.clone(), &engine_db, &registry, &config)
                    .await?;
            if identified > 0 {
                let (boot_erc20, boot_erc721, boot_erc1155) =
                    contracts_from_registry(&engine_db).await?;
                extend_unique(&mut all_erc20_addresses, boot_erc20);
                extend_unique(&mut all_erc721_addresses, boot_erc721);
                extend_unique(&mut all_erc1155_addresses, boot_erc1155);
            }
        }

        if all_erc20_addresses.is_empty()
            && all_erc721_addresses.is_empty()
            && all_erc1155_addresses.is_empty()
        {
            anyhow::bail!(
                "Event mode could not resolve any contracts after registry lookup/bootstrap. Provide explicit --erc20/--erc721/--erc1155 or widen bootstrap with --event-bootstrap-blocks."
            );
        }
    }

    let extractor: Box<dyn Extractor> = match config.mode {
        ExtractionMode::BlockRange => {
            tracing::info!("Using Block Range mode (single global cursor)");
            tracing::info!("  Batch size: {} blocks", config.batch_size);

            let extractor_config = BlockRangeConfig {
                rpc_url: config.rpc_url.clone(),
                from_block: config.from_block,
                to_block: config.to_block,
                batch_size: config.batch_size,
                retry_policy: RetryPolicy::default(),
                rpc_parallelism: config.rpc_parallelism,
            };
            Box::new(BlockRangeExtractor::new(provider.clone(), extractor_config))
        }
        ExtractionMode::Event => {
            tracing::info!("Using Event mode (per-contract cursors)");
            tracing::info!("  Chunk size: {} events", config.event_chunk_size);
            tracing::info!(
                "  Block batch size: {} blocks",
                config.event_block_batch_size
            );

            let mut event_configs = Vec::new();
            let to_block = config.to_block.unwrap_or(u64::MAX);

            for addr in &all_erc20_addresses {
                event_configs.push(ContractEventConfig {
                    address: *addr,
                    from_block: config.from_block,
                    to_block,
                });
            }

            for addr in &all_erc721_addresses {
                event_configs.push(ContractEventConfig {
                    address: *addr,
                    from_block: config.from_block,
                    to_block,
                });
            }

            for addr in &all_erc1155_addresses {
                event_configs.push(ContractEventConfig {
                    address: *addr,
                    from_block: config.from_block,
                    to_block,
                });
            }

            tracing::info!(
                "  Configured {} contracts for event extraction",
                event_configs.len()
            );

            let extractor_config = EventExtractorConfig {
                contracts: event_configs,
                chunk_size: config.event_chunk_size,
                block_batch_size: config.event_block_batch_size,
                retry_policy: RetryPolicy::default(),
                ignore_saved_state: false,
                rpc_parallelism: config.rpc_parallelism,
            };
            Box::new(EventExtractor::new(provider.clone(), extractor_config))
        }
        ExtractionMode::GlobalEvent => {
            tracing::info!("Using Global Event mode (single getEvents cursor)");
            tracing::info!("  Chunk size: {} events", config.event_chunk_size);
            tracing::info!(
                "  Block batch size: {} blocks",
                config.event_block_batch_size
            );

            let extractor_config = GlobalEventExtractorConfig {
                from_block: config.from_block,
                to_block: config.to_block.unwrap_or(u64::MAX),
                chunk_size: config.event_chunk_size,
                block_batch_size: config.event_block_batch_size,
                retry_policy: RetryPolicy::default(),
                ignore_saved_state: false,
                rpc_parallelism: config.rpc_parallelism,
            };
            Box::new(GlobalEventExtractor::new(
                provider.clone(),
                extractor_config,
            ))
        }
    };

    tracing::info!("Extractor configured");

    let mut reflection_builder = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET);

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .database_root(&config.db_dir)
        .cycle_interval(config.cycle_interval)
        .etl_concurrency(EtlConcurrencyConfig {
            max_prefetch_batches: config.max_prefetch_batches,
        })
        .command_bus_queue_size(config.metadata_queue_capacity)
        .engine_database_url(db_setup.engine_url.clone())
        .with_extractor(extractor)
        .with_contract_identifier(registry);

    let mut enabled_types: Vec<&str> = Vec::new();
    let mut erc20_grpc_service: Option<Erc20Service> = None;
    let mut erc721_grpc_service: Option<Erc721Service> = None;
    let mut erc1155_grpc_service: Option<Erc1155Service> = None;
    let mut token_uri_services = Vec::new();

    // Global extraction modes create all token infra for runtime auto-discovery.
    let is_global_mode =
        config.mode == ExtractionMode::BlockRange || config.mode == ExtractionMode::GlobalEvent;
    let create_erc20 = is_global_mode || !all_erc20_addresses.is_empty();
    let create_erc721 = is_global_mode || !all_erc721_addresses.is_empty();
    let create_erc1155 = is_global_mode || !all_erc1155_addresses.is_empty();

    if create_erc20 {
        enabled_types.push("ERC20");

        let storage = Arc::new(Erc20Storage::new(&db_setup.erc20_url).await?);
        tracing::info!("ERC20 database initialized: {}", db_setup.erc20_url);

        let decoder = Arc::new(Erc20Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc20Service::new(storage.clone());
        torii_config =
            torii_config.with_command_handler(Box::new(Erc20MetadataCommandHandler::new(
                provider.clone(),
                storage.clone(),
                config.metadata_max_retries,
            )));
        let sink = Box::new(
            Erc20Sink::new(storage)
                .with_grpc_service(grpc_service.clone())
                .with_balance_tracking(provider.clone())
                .with_metadata_pipeline(
                    config.metadata_parallelism,
                    config.metadata_queue_capacity,
                    config.metadata_max_retries,
                ),
        );
        torii_config = torii_config.add_sink_boxed(sink);

        erc20_grpc_service = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC20_DESCRIPTOR_SET);

        let erc20_decoder_id = DecoderId::new("erc20");
        for address in &all_erc20_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc20_decoder_id]);
        }

        if all_erc20_addresses.is_empty() {
            tracing::info!("ERC20 configured for auto-discovery");
        } else {
            tracing::info!(
                "ERC20 configured with {} explicit contracts (plus auto-discovery in block-range mode)",
                all_erc20_addresses.len()
            );
        }
    }

    if create_erc721 {
        enabled_types.push("ERC721");

        let storage = Arc::new(Erc721Storage::new(&db_setup.erc721_url).await?);
        tracing::info!("ERC721 database initialized: {}", db_setup.erc721_url);

        let decoder = Arc::new(Erc721Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc721Service::new(storage.clone());
        torii_config =
            torii_config.with_command_handler(Box::new(Erc721MetadataCommandHandler::new(
                provider.clone(),
                storage.clone(),
                config.metadata_max_retries,
            )));
        let mut sink = Erc721Sink::new(storage).with_grpc_service(grpc_service.clone());
        if effective_metadata_mode == MetadataMode::Inline {
            let (token_uri_sender, token_uri_service) = TokenUriService::spawn_with_image_cache(
                Arc::new(MetadataFetcher::new(provider.clone())),
                sink.storage().clone(),
                config.metadata_queue_capacity,
                config.metadata_parallelism.max(1),
                Some(Path::new("./data").join("image-cache")),
                4,
            );
            token_uri_services.push(token_uri_service);
            sink = sink
                .with_metadata_commands()
                .with_token_uri_sender(token_uri_sender);
        } else {
            tracing::info!("ERC721 metadata fetching disabled for throughput (deferred mode)");
        }
        let sink = Box::new(sink);
        torii_config = torii_config.add_sink_boxed(sink);

        erc721_grpc_service = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC721_DESCRIPTOR_SET);

        let erc721_decoder_id = DecoderId::new("erc721");
        for address in &all_erc721_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc721_decoder_id]);
        }

        if all_erc721_addresses.is_empty() {
            tracing::info!("ERC721 configured for auto-discovery");
        } else {
            tracing::info!(
                "ERC721 configured with {} explicit contracts (plus auto-discovery in block-range mode)",
                all_erc721_addresses.len()
            );
        }
    }

    if create_erc1155 {
        enabled_types.push("ERC1155");

        let storage = Arc::new(Erc1155Storage::new(&db_setup.erc1155_url).await?);
        tracing::info!("ERC1155 database initialized: {}", db_setup.erc1155_url);

        let decoder = Arc::new(Erc1155Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc1155Service::new(storage.clone());
        torii_config = torii_config.with_command_handler(Box::new(
            Erc1155MetadataCommandHandler::new(provider.clone(), storage.clone()),
        ));
        let mut sink = Erc1155Sink::new(storage)
            .with_grpc_service(grpc_service.clone())
            .with_balance_tracking(provider.clone());
        if effective_metadata_mode == MetadataMode::Inline {
            let (token_uri_sender, token_uri_service) = TokenUriService::spawn_with_image_cache(
                Arc::new(MetadataFetcher::new(provider.clone())),
                sink.storage().clone(),
                config.metadata_queue_capacity,
                config.metadata_parallelism.max(1),
                Some(Path::new("./data").join("image-cache")),
                4,
            );
            token_uri_services.push(token_uri_service);
            sink = sink
                .with_metadata_commands()
                .with_token_uri_sender(token_uri_sender);
        } else {
            tracing::info!("ERC1155 metadata fetching disabled for throughput (deferred mode)");
        }
        let sink = Box::new(sink);
        torii_config = torii_config.add_sink_boxed(sink);

        erc1155_grpc_service = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC1155_DESCRIPTOR_SET);

        let erc1155_decoder_id = DecoderId::new("erc1155");
        for address in &all_erc1155_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc1155_decoder_id]);
        }

        if all_erc1155_addresses.is_empty() {
            tracing::info!("ERC1155 configured for auto-discovery");
        } else {
            tracing::info!(
                "ERC1155 configured with {} explicit contracts (plus auto-discovery in block-range mode)",
                all_erc1155_addresses.len()
            );
        }
    }

    let reflection = reflection_builder
        .build_v1()
        .expect("Failed to build gRPC reflection service")
        .accept_compressed(CompressionEncoding::Gzip);

    let erc20_server = erc20_grpc_service
        .map(|service| Erc20Server::new(service).accept_compressed(CompressionEncoding::Gzip));
    let erc721_server = erc721_grpc_service
        .map(|service| Erc721Server::new(service).accept_compressed(CompressionEncoding::Gzip));
    let erc1155_server = erc1155_grpc_service
        .map(|service| Erc1155Server::new(service).accept_compressed(CompressionEncoding::Gzip));

    let mut grpc_builder = tonic::transport::Server::builder();
    let grpc_router = match (erc20_server, erc721_server, erc1155_server) {
        (Some(erc20), Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(erc20))
            .add_service(tonic_web::enable(erc721))
            .add_service(tonic_web::enable(erc1155))
            .add_service(reflection.clone()),
        (Some(erc20), Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(erc20))
            .add_service(tonic_web::enable(erc721))
            .add_service(reflection.clone()),
        (Some(erc20), None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(erc20))
            .add_service(tonic_web::enable(erc1155))
            .add_service(reflection.clone()),
        (None, Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(erc721))
            .add_service(tonic_web::enable(erc1155))
            .add_service(reflection.clone()),
        (Some(erc20), None, None) => grpc_builder
            .add_service(tonic_web::enable(erc20))
            .add_service(reflection.clone()),
        (None, Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(erc721))
            .add_service(reflection.clone()),
        (None, None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(erc1155))
            .add_service(reflection.clone()),
        (None, None, None) => {
            // No token services, just reflection
            grpc_builder.add_service(reflection)
        }
    };

    let torii_config = torii_config
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true)
        .build();

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("Enabled token types: {}", enabled_types.join(", "));
    tracing::info!(
        "ETL concurrency: prefetch_batches={} cycle_interval={}s rpc_parallelism={} metadata_parallelism={} metadata_queue_capacity={} metadata_max_retries={}",
        config.max_prefetch_batches,
        config.cycle_interval,
        config.rpc_parallelism,
        config.metadata_parallelism,
        config.metadata_queue_capacity,
        config.metadata_max_retries,
    );
    tracing::info!("gRPC service available at localhost:{}", config.port);
    tracing::info!("  - torii.Torii (EventBus subscriptions)");

    if create_erc20 {
        tracing::info!("  - torii.sinks.erc20.Erc20 (ERC20 queries and subscriptions)");
    }
    if create_erc721 {
        tracing::info!("  - torii.sinks.erc721.Erc721 (ERC721 queries and subscriptions)");
    }
    if create_erc1155 {
        tracing::info!("  - torii.sinks.erc1155.Erc1155 (ERC1155 queries and subscriptions)");
    }

    torii::run(torii_config)
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;

    drop(token_uri_services);

    tracing::info!("Torii shutdown complete");

    #[cfg(feature = "profiling")]
    {
        if let Ok(report) = guard.report().build() {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let db_backend = if db_setup.erc20_backend == DbBackend::Postgres {
                "postgres"
            } else {
                "sqlite"
            };
            let mode = match config.mode {
                ExtractionMode::BlockRange => "block-range",
                ExtractionMode::Event => "event",
                ExtractionMode::GlobalEvent => "global-event",
            };
            let filename = format!("flamegraph-torii-tokens-{mode}-{db_backend}-{ts}.svg");
            let file = std::fs::File::create(&filename).unwrap();
            report.flamegraph(file).unwrap();
            tracing::info!("Flamegraph generated: {}", filename);
        }
    }

    Ok(())
}
