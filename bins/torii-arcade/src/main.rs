mod config;

use anyhow::{Error, Result};
use clap::Parser;
use config::{Config, MetadataMode};
use starknet::core::types::Felt;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::codec::CompressionEncoding;
use tonic_reflection::server::Builder as ReflectionBuilder;
use torii::axum::Router;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{
    ContractEventConfig, EventExtractor, EventExtractorConfig, Extractor, RetryPolicy,
};
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii::etl::{EngineDb, TypeId};
use torii::EtlConcurrencyConfig;
use torii_arcade_sink::proto::arcade::arcade_server::ArcadeServer;
use torii_arcade_sink::{ArcadeSink, FILE_DESCRIPTOR_SET as ARCADE_DESCRIPTOR_SET};
use torii_common::{MetadataFetcher, TokenUriService};
use torii_config_common::apply_observability_env;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::external_contract::{
    contract_type_from_decoder_ids, RegisterExternalContractCommandHandler, RegisteredContractType,
    SharedContractTypeRegistry, SharedDecoderRegistry,
};
use torii_dojo::store::DojoStoreTrait;
use torii_ecs_sink::proto::world::world_server::WorldServer;
use torii_ecs_sink::{EcsSink, FILE_DESCRIPTOR_SET as ECS_DESCRIPTOR_SET};
use torii_erc1155::proto::erc1155_server::Erc1155Server;
use torii_erc1155::{
    Erc1155Decoder, Erc1155MetadataCommandHandler, Erc1155Service, Erc1155Sink, Erc1155Storage,
    FILE_DESCRIPTOR_SET as ERC1155_DESCRIPTOR_SET,
};
use torii_erc20::proto::erc20_server::Erc20Server;
use torii_erc20::{
    Erc20Decoder, Erc20MetadataCommandHandler, Erc20Service, Erc20Sink, Erc20Storage,
    FILE_DESCRIPTOR_SET as ERC20_DESCRIPTOR_SET,
};
use torii_erc721::proto::erc721_server::Erc721Server;
use torii_erc721::{
    Erc721Decoder, Erc721MetadataCommandHandler, Erc721Service, Erc721Sink, Erc721Storage,
    FILE_DESCRIPTOR_SET as ERC721_DESCRIPTOR_SET,
};
use torii_introspect_sql_sink::{IntrospectDb, NamespaceMode};
use torii_pathfinder::extractor::PathfinderCombinedExtractor;
use torii_runtime_common::database::{validate_uniform_backends, DEFAULT_SQLITE_MAX_CONNECTIONS};
use torii_runtime_common::token_support::{resolve_installed_token_support, InstalledTokenSupport};
use torii_sql::{DbConnectionOptions, DbPool, DbPoolOptions, PoolExt};

type StarknetProvider =
    starknet::providers::jsonrpc::JsonRpcClient<starknet::providers::jsonrpc::HttpTransport>;

const TOKEN_COMMAND_QUEUE_SIZE: usize = 4096;
const TOKEN_METADATA_COMMAND_PARALLELISM: usize = 1;
const TOKEN_METADATA_MAX_RETRIES: u8 = 3;
const TOKEN_URI_FETCH_PARALLELISM: usize = 8;

fn build_contract_type_registry(
    dojo_event_contracts: &[Felt],
    erc20_addresses: &[Felt],
    erc721_addresses: &[Felt],
    erc1155_addresses: &[Felt],
) -> SharedContractTypeRegistry {
    let mut contract_types = HashMap::new();
    for &contract in dojo_event_contracts {
        contract_types.insert(contract, RegisteredContractType::World);
    }
    for &contract in erc20_addresses {
        contract_types.insert(contract, RegisteredContractType::Erc20);
    }
    for &contract in erc721_addresses {
        contract_types.insert(contract, RegisteredContractType::Erc721);
    }
    for &contract in erc1155_addresses {
        contract_types.insert(contract, RegisteredContractType::Erc1155);
    }
    Arc::new(RwLock::new(contract_types))
}

async fn load_persisted_contract_registries(
    engine_db: &EngineDb,
    decoder_registry: &SharedDecoderRegistry,
    contract_type_registry: &SharedContractTypeRegistry,
) -> Result<()> {
    let mappings = engine_db.get_all_contract_decoders().await?;
    let mut decoders = decoder_registry.write().await;
    let mut contract_types = contract_type_registry.write().await;

    for (contract, decoder_ids, _) in mappings {
        decoders.insert(contract, decoder_ids.clone());
        if let Some(contract_type) = contract_type_from_decoder_ids(&decoder_ids) {
            contract_types.insert(contract, contract_type);
        }
    }

    Ok(())
}

#[allow(clippy::fn_params_excessive_bools)]
fn installed_external_decoder_ids(
    enabled: bool,
    install_erc20: bool,
    install_erc721: bool,
    install_erc1155: bool,
) -> HashSet<DecoderId> {
    if !enabled {
        return HashSet::new();
    }

    let mut installed = HashSet::from([DecoderId::new("dojo-introspect")]);
    if install_erc20 {
        installed.insert(DecoderId::new("erc20"));
    }
    if install_erc721 {
        installed.insert(DecoderId::new("erc721"));
    }
    if install_erc1155 {
        installed.insert(DecoderId::new("erc1155"));
    }
    installed
}

struct ArcadeProjectionPipeline {
    sinks: Vec<Box<dyn Sink>>,
}

impl ArcadeProjectionPipeline {
    fn new(sinks: Vec<Box<dyn Sink>>) -> Self {
        Self { sinks }
    }

    fn abort_on_sink_failure(stage: &str, sink_name: &str, error: anyhow::Error) -> ! {
        tracing::error!(
            target: "torii_arcade",
            stage,
            sink = sink_name,
            error = %error,
            "Fatal arcade projection pipeline failure"
        );
        std::process::abort();
    }
}

#[torii::async_trait]
impl Sink for ArcadeProjectionPipeline {
    fn name(&self) -> &'static str {
        "arcade-projection-pipeline"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        let mut seen = HashSet::new();
        let mut interested = Vec::new();
        for type_id in self.sinks.iter().flat_map(|sink| sink.interested_types()) {
            if seen.insert(type_id) {
                interested.push(type_id);
            }
        }
        interested
    }

    async fn process(
        &self,
        envelopes: &[torii::etl::Envelope],
        batch: &torii::etl::extractor::ExtractionBatch,
    ) -> anyhow::Result<()> {
        for sink in &self.sinks {
            if let Err(error) = sink.process(envelopes, batch).await {
                Self::abort_on_sink_failure("process", sink.name(), error);
            }
        }
        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        let mut topics = Vec::new();
        for sink in &self.sinks {
            topics.extend(sink.topics());
        }
        topics
    }

    fn build_routes(&self) -> Router {
        let mut router = Router::new();
        for sink in &self.sinks {
            router = router.merge(sink.build_routes());
        }
        router
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        context: &SinkContext,
    ) -> anyhow::Result<()> {
        for sink in &mut self.sinks {
            sink.initialize(event_bus.clone(), context).await?;
        }
        Ok(())
    }
}

fn advertised_token_services(installed_token_support: InstalledTokenSupport) -> Vec<&'static str> {
    let mut services = Vec::new();
    if installed_token_support.erc20 {
        services.push("torii.sinks.erc20.Erc20");
    }
    if installed_token_support.erc721 {
        services.push("torii.sinks.erc721.Erc721");
    }
    if installed_token_support.erc1155 {
        services.push("torii.sinks.erc1155.Erc1155");
    }
    services
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    run_indexer(Config::parse()).await
}

async fn run_indexer(config: Config) -> Result<()> {
    tracing::info!("Starting Torii Arcade backend");
    apply_observability_env(config.observability);

    let db_dir = Path::new(&config.db_dir);
    std::fs::create_dir_all(db_dir)?;

    let storage_database_url = config.storage_database_url()?;
    let engine_database_url = config.engine_database_url();
    let (erc20_db_url, erc721_db_url, erc1155_db_url) = config.token_storage_urls()?;
    let backend = validate_uniform_backends(
        &[
            ("engine", &engine_database_url),
            ("storage", &storage_database_url),
            ("erc20", &erc20_db_url),
            ("erc721", &erc721_db_url),
            ("erc1155", &erc1155_db_url),
        ],
        "torii-arcade does not support mixed storage backends in one runtime; configure all databases as either SQLite or PostgreSQL",
    ).map_err(|err| anyhow::anyhow!(err))?;

    let provider = starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    );
    let provider = Arc::new(provider);

    let dojo_event_contracts = config.dojo_event_contract_addresses()?;
    let introspect_contracts = config.introspect_contract_addresses()?;
    let historical_models = config.historical_models();
    let mut erc20_addresses = config.erc20_addresses()?;
    let erc721_addresses = config.erc721_addresses()?;
    let erc1155_addresses = config.erc1155_addresses()?;

    if config.include_well_known {
        for (address, name) in Config::well_known_erc20_contracts() {
            if !erc20_addresses.contains(&address) {
                tracing::info!("Adding well-known ERC20: {} at {:#x}", name, address);
                erc20_addresses.push(address);
            }
        }
    }

    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("Primary world: {}", config.world_address);
    tracing::info!("Dojo event contracts: {}", dojo_event_contracts.len());
    tracing::info!("Dojo introspect contracts: {}", introspect_contracts.len());
    tracing::info!("ERC20 contracts: {}", erc20_addresses.len());
    tracing::info!("ERC721 contracts: {}", erc721_addresses.len());
    tracing::info!("ERC1155 contracts: {}", erc1155_addresses.len());
    tracing::info!(
        "Total configured targets: {}",
        dojo_event_contracts.len()
            + erc20_addresses.len()
            + erc721_addresses.len()
            + erc1155_addresses.len()
    );
    tracing::info!("From block: {}", config.from_block);
    if let Some(to_block) = config.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Database directory: {}", config.db_dir);
    tracing::info!("Engine database URL: {}", engine_database_url);
    tracing::info!("Storage database URL: {}", storage_database_url);
    tracing::info!("ERC20 storage URL: {}", erc20_db_url);
    tracing::info!("ERC721 storage URL: {}", erc721_db_url);
    tracing::info!("ERC1155 storage URL: {}", erc1155_db_url);
    tracing::info!("Database backend: {:?}", backend);
    tracing::info!("Metadata mode: {:?}", config.metadata_mode);
    tracing::info!(
        "ETL concurrency: prefetch_batches={} cycle_interval={}s rpc_parallelism={}",
        config.max_prefetch_batches,
        config.cycle_interval,
        config.rpc_parallelism,
    );
    tracing::info!(
        "Saved state handling: {}",
        if config.ignore_saved_state {
            "ignoring persisted extractor state"
        } else {
            "resuming from persisted extractor state when available"
        }
    );
    tracing::info!(
        "Observability: {}",
        if config.observability {
            "enabled"
        } else {
            "disabled"
        }
    );
    tracing::info!(
        "External contract indexing: {}",
        if config.index_external_contracts {
            "enabled"
        } else {
            "disabled"
        }
    );
    tracing::info!("Historical models tracked: {}", historical_models.len());

    let excluded_dojo_contracts: Vec<Felt> = dojo_event_contracts
        .iter()
        .copied()
        .filter(|contract| !introspect_contracts.contains(contract))
        .collect();

    let registry_engine_db = Arc::new(
        EngineDb::new(torii::etl::engine_db::EngineDbConfig {
            path: engine_database_url.clone(),
        })
        .await?,
    );
    let decoder_registry: SharedDecoderRegistry = Arc::new(RwLock::new(HashMap::new()));
    let contract_type_registry = build_contract_type_registry(
        &dojo_event_contracts,
        &erc20_addresses,
        &erc721_addresses,
        &erc1155_addresses,
    );
    load_persisted_contract_registries(
        registry_engine_db.as_ref(),
        &decoder_registry,
        &contract_type_registry,
    )
    .await?;

    let installed_token_support = resolve_installed_token_support(
        config.index_external_contracts,
        InstalledTokenSupport {
            erc20: !erc20_addresses.is_empty(),
            erc721: !erc721_addresses.is_empty(),
            erc1155: !erc1155_addresses.is_empty(),
        },
    );
    let install_erc20 = installed_token_support.erc20;
    let install_erc721 = installed_token_support.erc721;
    let install_erc1155 = installed_token_support.erc1155;
    let installed_external_decoders = installed_external_decoder_ids(
        config.index_external_contracts,
        install_erc20,
        install_erc721,
        install_erc1155,
    );

    let extractor = build_extractor(
        provider.clone(),
        &dojo_event_contracts,
        &erc20_addresses,
        &erc721_addresses,
        &erc1155_addresses,
        &config,
    )?;
    let conn_options =
        DbConnectionOptions::from_str(&engine_database_url).map_err(anyhow::Error::msg)?;
    let max_connections = match config.max_db_connections {
        Some(n) => n,
        None => match &conn_options {
            DbConnectionOptions::Postgres(_) => 10,
            DbConnectionOptions::Sqlite(ops) if ops.is_in_memory() => 1,
            DbConnectionOptions::Sqlite(_) => DEFAULT_SQLITE_MAX_CONNECTIONS,
        },
    };
    let pool_options = DbPoolOptions::new().max_connections(max_connections);
    let pool = pool_options.connect_any_with(conn_options).await?;
    if let DbPool::Sqlite(pool) = &pool {
        pool.execute_queries([
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA foreign_keys=ON",
        ])
        .await?;
    }
    let dojo_decoder = DojoDecoder::new(pool.clone(), provider.clone());
    let introspect_sink = IntrospectDb::new(pool, NamespaceMode::Address);

    dojo_decoder.initialize().await?;
    dojo_decoder.load_tables(&[]).await?;
    introspect_sink.initialize_introspect_sql_sink().await?;

    let ecs_sink = EcsSink::new(
        &storage_database_url,
        config.max_db_connections,
        Some(erc20_db_url.as_str()),
        Some(erc721_db_url.as_str()),
        Some(erc1155_db_url.as_str()),
        contract_type_registry.clone(),
        config.from_block,
        config.index_external_contracts,
        installed_external_decoders.clone(),
    )
    .await?;
    let ecs_grpc_service = ecs_sink.get_grpc_service_impl();
    let arcade_sink = ArcadeSink::new(
        &storage_database_url,
        &erc721_db_url,
        config.max_db_connections,
    )
    .await?;
    let arcade_grpc_service = arcade_sink.get_grpc_service_impl();

    let mut reflection_builder = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(ECS_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(ARCADE_DESCRIPTOR_SET);

    let arcade_projection_pipeline =
        ArcadeProjectionPipeline::new(vec![Box::new(introspect_sink), Box::new(arcade_sink)]);

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .database_root(&config.db_dir)
        .command_bus_queue_size(TOKEN_COMMAND_QUEUE_SIZE)
        .cycle_interval(config.cycle_interval)
        .etl_concurrency(EtlConcurrencyConfig {
            max_prefetch_batches: config.max_prefetch_batches,
        })
        .engine_database_url(engine_database_url)
        .with_extractor(extractor)
        .add_decoder(Arc::new(dojo_decoder))
        .add_sink_boxed(Box::new(ecs_sink))
        .add_sink_boxed(Box::new(arcade_projection_pipeline));

    if let Some(tls) = config.tls_config()? {
        torii_config = torii_config.with_tls(tls);
    }

    if config.index_external_contracts {
        torii_config = torii_config
            .with_registry_cache(decoder_registry.clone())
            .with_command_handler(Box::new(RegisterExternalContractCommandHandler::new(
                registry_engine_db.clone(),
                decoder_registry.clone(),
                contract_type_registry.clone(),
            )));
    }

    if !excluded_dojo_contracts.is_empty() {
        torii_config = torii_config.blacklist_contracts(excluded_dojo_contracts.clone());
    }

    let dojo_decoder_id = DecoderId::new("dojo-introspect");
    for contract in &introspect_contracts {
        torii_config = torii_config.map_contract(*contract, vec![dojo_decoder_id]);
    }

    for contract in &excluded_dojo_contracts {
        tracing::warn!(
            target: "torii_arcade",
            contract = format!("{contract:#x}"),
            "Dojo contract is excluded from introspect decoding by default"
        );
    }

    let mut erc20_grpc_service: Option<Erc20Service> = None;
    let mut erc721_grpc_service: Option<Erc721Service> = None;
    let mut erc1155_grpc_service: Option<Erc1155Service> = None;
    let mut token_uri_services = Vec::new();

    if install_erc20 {
        let storage = Arc::new(Erc20Storage::new(&erc20_db_url).await?);
        let grpc_service = Erc20Service::new(storage.clone());
        let sink = Box::new(
            Erc20Sink::new(storage.clone())
                .with_grpc_service(grpc_service.clone())
                .with_balance_tracking(provider.clone())
                .with_metadata_pipeline(
                    TOKEN_METADATA_COMMAND_PARALLELISM,
                    TOKEN_COMMAND_QUEUE_SIZE,
                    TOKEN_METADATA_MAX_RETRIES,
                ),
        );
        torii_config = torii_config
            .add_decoder(Arc::new(Erc20Decoder::new()))
            .add_sink_boxed(sink)
            .with_command_handler(Box::new(Erc20MetadataCommandHandler::new(
                provider.clone(),
                storage,
                TOKEN_METADATA_MAX_RETRIES,
            )));
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC20_DESCRIPTOR_SET);
        let decoder_id = DecoderId::new("erc20");
        for address in &erc20_addresses {
            torii_config = torii_config.map_contract(*address, vec![decoder_id]);
        }
        erc20_grpc_service = Some(grpc_service);
    }

    if install_erc721 {
        let storage = Arc::new(Erc721Storage::new(&erc721_db_url).await?);
        let grpc_service = Erc721Service::new(storage.clone());
        let mut sink = Erc721Sink::new(storage.clone()).with_grpc_service(grpc_service.clone());
        if config.metadata_mode == MetadataMode::Inline {
            let (token_uri_sender, token_uri_service) = TokenUriService::spawn_with_image_cache(
                Arc::new(MetadataFetcher::new(provider.clone())),
                storage.clone(),
                TOKEN_COMMAND_QUEUE_SIZE,
                TOKEN_URI_FETCH_PARALLELISM,
                Some(Path::new("./data").join("image-cache")),
                4,
            );
            token_uri_services.push(token_uri_service);
            sink = sink
                .with_metadata_commands()
                .with_token_uri_sender(token_uri_sender);
        }
        torii_config = torii_config
            .add_decoder(Arc::new(Erc721Decoder::new()))
            .add_sink_boxed(Box::new(sink))
            .with_command_handler(Box::new(Erc721MetadataCommandHandler::new(
                provider.clone(),
                storage,
                TOKEN_METADATA_MAX_RETRIES,
            )));
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC721_DESCRIPTOR_SET);
        let decoder_id = DecoderId::new("erc721");
        for address in &erc721_addresses {
            torii_config = torii_config.map_contract(*address, vec![decoder_id]);
        }
        erc721_grpc_service = Some(grpc_service);
    }

    if install_erc1155 {
        let storage = Arc::new(Erc1155Storage::new(&erc1155_db_url).await?);
        let grpc_service = Erc1155Service::new(storage.clone());
        let mut sink = Erc1155Sink::new(storage.clone())
            .with_grpc_service(grpc_service.clone())
            .with_balance_tracking(provider.clone());
        if config.metadata_mode == MetadataMode::Inline {
            let (token_uri_sender, token_uri_service) = TokenUriService::spawn_with_image_cache(
                Arc::new(MetadataFetcher::new(provider.clone())),
                storage.clone(),
                TOKEN_COMMAND_QUEUE_SIZE,
                TOKEN_URI_FETCH_PARALLELISM,
                Some(Path::new("./data").join("image-cache")),
                4,
            );
            token_uri_services.push(token_uri_service);
            sink = sink
                .with_metadata_commands()
                .with_token_uri_sender(token_uri_sender);
        }
        torii_config = torii_config
            .add_decoder(Arc::new(Erc1155Decoder::new()))
            .add_sink_boxed(Box::new(sink))
            .with_command_handler(Box::new(Erc1155MetadataCommandHandler::new(
                provider.clone(),
                storage,
            )));
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC1155_DESCRIPTOR_SET);
        let decoder_id = DecoderId::new("erc1155");
        for address in &erc1155_addresses {
            torii_config = torii_config.map_contract(*address, vec![decoder_id]);
        }
        erc1155_grpc_service = Some(grpc_service);
    }

    let reflection = reflection_builder
        .build_v1()
        .expect("failed to build Arcade reflection service")
        .accept_compressed(CompressionEncoding::Gzip);

    let world_server =
        WorldServer::new((*ecs_grpc_service).clone()).accept_compressed(CompressionEncoding::Gzip);
    let arcade_server = ArcadeServer::new((*arcade_grpc_service).clone())
        .accept_compressed(CompressionEncoding::Gzip);
    let erc20_server = erc20_grpc_service
        .map(|service| Erc20Server::new(service).accept_compressed(CompressionEncoding::Gzip));
    let erc721_server = erc721_grpc_service
        .map(|service| Erc721Server::new(service).accept_compressed(CompressionEncoding::Gzip));
    let erc1155_server = erc1155_grpc_service
        .map(|service| Erc1155Server::new(service).accept_compressed(CompressionEncoding::Gzip));

    let mut grpc_builder = tonic::transport::Server::builder().accept_http1(true);
    let grpc_router = match (erc20_server, erc721_server, erc1155_server) {
        (Some(erc20), Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(world_server.clone()))
            .add_service(tonic_web::enable(arcade_server.clone()))
            .add_service(tonic_web::enable(erc20))
            .add_service(tonic_web::enable(erc721))
            .add_service(tonic_web::enable(erc1155))
            .add_service(tonic_web::enable(reflection.clone())),
        (Some(erc20), Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(world_server.clone()))
            .add_service(tonic_web::enable(arcade_server.clone()))
            .add_service(tonic_web::enable(erc20))
            .add_service(tonic_web::enable(erc721))
            .add_service(tonic_web::enable(reflection.clone())),
        (Some(erc20), None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(world_server.clone()))
            .add_service(tonic_web::enable(arcade_server.clone()))
            .add_service(tonic_web::enable(erc20))
            .add_service(tonic_web::enable(erc1155))
            .add_service(tonic_web::enable(reflection.clone())),
        (None, Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(world_server.clone()))
            .add_service(tonic_web::enable(arcade_server.clone()))
            .add_service(tonic_web::enable(erc721))
            .add_service(tonic_web::enable(erc1155))
            .add_service(tonic_web::enable(reflection.clone())),
        (Some(erc20), None, None) => grpc_builder
            .add_service(tonic_web::enable(world_server.clone()))
            .add_service(tonic_web::enable(arcade_server.clone()))
            .add_service(tonic_web::enable(erc20))
            .add_service(tonic_web::enable(reflection.clone())),
        (None, Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(world_server.clone()))
            .add_service(tonic_web::enable(arcade_server.clone()))
            .add_service(tonic_web::enable(erc721))
            .add_service(tonic_web::enable(reflection.clone())),
        (None, None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(world_server.clone()))
            .add_service(tonic_web::enable(arcade_server.clone()))
            .add_service(tonic_web::enable(erc1155))
            .add_service(tonic_web::enable(reflection.clone())),
        (None, None, None) => grpc_builder
            .add_service(tonic_web::enable(world_server))
            .add_service(tonic_web::enable(arcade_server))
            .add_service(tonic_web::enable(reflection)),
    };

    let torii_config = torii_config
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true)
        .build();

    tracing::info!("Arcade backend configured, starting ETL pipeline...");
    tracing::info!("gRPC service available on port {}", config.port);
    tracing::info!("  - torii.Torii");
    tracing::info!("  - world.World");
    tracing::info!("  - arcade.v1.Arcade");
    for service in advertised_token_services(installed_token_support) {
        tracing::info!("  - {}", service);
    }

    torii::run(torii_config)
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;

    drop(token_uri_services);

    Ok(())
}

fn build_extractor(
    provider: Arc<StarknetProvider>,
    dojo_contracts: &[Felt],
    erc20_addresses: &[Felt],
    erc721_addresses: &[Felt],
    erc1155_addresses: &[Felt],
    config: &Config,
) -> Result<Box<dyn Extractor>> {
    let to_block = config.to_block.unwrap_or(u64::MAX);
    let mut contracts = Vec::new();
    let mut seen = HashSet::new();

    append_unique_contract_configs(
        &mut contracts,
        &mut seen,
        dojo_contracts,
        config.from_block,
        to_block,
    );
    append_unique_contract_configs(
        &mut contracts,
        &mut seen,
        erc20_addresses,
        config.from_block,
        to_block,
    );
    append_unique_contract_configs(
        &mut contracts,
        &mut seen,
        erc721_addresses,
        config.from_block,
        to_block,
    );
    append_unique_contract_configs(
        &mut contracts,
        &mut seen,
        erc1155_addresses,
        config.from_block,
        to_block,
    );
    let event_extractor = EventExtractor::new(
        provider,
        EventExtractorConfig {
            contracts: contracts.clone(),
            chunk_size: config.event_chunk_size,
            block_batch_size: config.event_block_batch_size,
            retry_policy: RetryPolicy::default(),
            ignore_saved_state: config.ignore_saved_state,
            rpc_parallelism: config.rpc_parallelism,
        },
    );
    #[allow(clippy::single_match_else)]
    match config.pathfinder_path() {
        Some(path) => {
            tracing::info!(
                "Using Pathfinder for event extraction with endpoint: {}",
                path.display()
            );
            PathfinderCombinedExtractor::new(
                path,
                config.event_block_batch_size,
                config.from_block,
                config.to_block.unwrap_or(u64::MAX),
                event_extractor,
            )
            .map(|e| Box::new(e) as Box<dyn Extractor>)
            .map_err(Error::new)
        }
        None => {
            tracing::info!("Using direct RPC for event extraction");
            Ok(Box::new(event_extractor))
        }
    }
}

fn append_unique_contract_configs(
    configs: &mut Vec<ContractEventConfig>,
    seen: &mut HashSet<Felt>,
    addresses: &[Felt],
    from_block: u64,
    to_block: u64,
) {
    for &address in addresses {
        if seen.insert(address) {
            configs.push(ContractEventConfig {
                address,
                from_block,
                to_block,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::advertised_token_services;
    use torii_runtime_common::token_support::InstalledTokenSupport;

    #[test]
    fn advertised_token_services_include_installed_services_without_explicit_targets() {
        let services = advertised_token_services(InstalledTokenSupport {
            erc20: true,
            erc721: true,
            erc1155: true,
        });

        assert_eq!(
            services,
            vec![
                "torii.sinks.erc20.Erc20",
                "torii.sinks.erc721.Erc721",
                "torii.sinks.erc1155.Erc1155",
            ]
        );
    }

    #[test]
    fn advertised_token_services_skip_uninstalled_services() {
        let services = advertised_token_services(InstalledTokenSupport {
            erc20: false,
            erc721: true,
            erc1155: false,
        });

        assert_eq!(services, vec!["torii.sinks.erc721.Erc721"]);
    }
}
