//! Unified synthetic token profiler for ERC20, ERC721, and ERC1155.
//!
//! Profiles the full ingestion pipeline (extract -> decode -> sink -> DB) for
//! all token types without external RPC dependencies.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use torii::command::{Command, CommandHandler};
use torii::etl::decoder::ContractFilter;
use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::extractor::SyntheticExtractor;
use torii::etl::sink::Sink;
use torii::etl::{Decoder, DecoderContext};
use torii_common::{TokenUriResult, TokenUriStore};
use torii_erc1155::handlers::{FetchErc1155MetadataCommand, RefreshErc1155TokenUriCommand};
use torii_erc1155::{
    Erc1155Decoder, Erc1155Sink, Erc1155Storage, SyntheticErc1155Config, SyntheticErc1155Extractor,
};
use torii_erc20::handlers::FetchErc20MetadataCommand;
use torii_erc20::{
    Erc20Decoder, Erc20Sink, Erc20Storage, SyntheticErc20Config, SyntheticErc20Extractor,
};
use torii_erc721::handlers::{FetchErc721MetadataCommand, RefreshErc721TokenUriCommand};
use torii_erc721::{
    Erc721Decoder, Erc721Sink, Erc721Storage, SyntheticErc721Config, SyntheticErc721Extractor,
};
use torii_runtime_common::sink::{drop_postgres_schemas, initialize_sink_with_command_handlers};

const SYNTH_COMMAND_QUEUE_SIZE: usize = 4096;
const SYNTH_METADATA_COMMAND_PARALLELISM: usize = 1;
const SYNTH_METADATA_MAX_RETRIES: u8 = 1;

struct SyntheticErc20MetadataCommandHandler {
    storage: Arc<Erc20Storage>,
}

impl SyntheticErc20MetadataCommandHandler {
    fn new(storage: Arc<Erc20Storage>) -> Self {
        Self { storage }
    }
}

struct SyntheticErc721MetadataCommandHandler {
    storage: Arc<Erc721Storage>,
}

impl SyntheticErc721MetadataCommandHandler {
    fn new(storage: Arc<Erc721Storage>) -> Self {
        Self { storage }
    }
}

struct SyntheticErc721TokenUriCommandHandler {
    storage: Arc<Erc721Storage>,
}

impl SyntheticErc721TokenUriCommandHandler {
    fn new(storage: Arc<Erc721Storage>) -> Self {
        Self { storage }
    }
}

struct SyntheticErc1155MetadataCommandHandler {
    storage: Arc<Erc1155Storage>,
}

impl SyntheticErc1155MetadataCommandHandler {
    fn new(storage: Arc<Erc1155Storage>) -> Self {
        Self { storage }
    }
}

struct SyntheticErc1155TokenUriCommandHandler {
    storage: Arc<Erc1155Storage>,
}

impl SyntheticErc1155TokenUriCommandHandler {
    fn new(storage: Arc<Erc1155Storage>) -> Self {
        Self { storage }
    }
}

fn token_suffix(token: impl std::fmt::LowerHex) -> String {
    let token_hex = format!("{token:#x}");
    let trimmed = token_hex.trim_start_matches("0x");
    let start = trimmed.len().saturating_sub(6);
    trimmed[start..].to_uppercase()
}

fn synthetic_token_uri_metadata(standard: &str, token_id: impl std::fmt::Display) -> String {
    format!(
        r#"{{"name":"Synthetic {standard} #{token_id}","description":"Synthetic metadata for command bus profiling","attributes":[{{"trait_type":"standard","value":"{standard}"}},{{"trait_type":"token_id","value":"{token_id}"}}]}}"#
    )
}

#[torii::async_trait]
impl CommandHandler for SyntheticErc20MetadataCommandHandler {
    fn supports(&self, command: &dyn Command) -> bool {
        command.as_any().is::<FetchErc20MetadataCommand>()
    }

    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<FetchErc20MetadataCommand>()
            .map_err(|_| anyhow::anyhow!("synthetic ERC20 handler received unexpected command"))?;
        let command = *command;
        let suffix = token_suffix(command.token);
        let name = format!("Synthetic ERC20 {suffix}");
        let symbol = format!("S{suffix}");
        self.storage
            .upsert_token_metadata(command.token, Some(&name), Some(&symbol), Some(18), None)
            .await
    }
}

#[torii::async_trait]
impl CommandHandler for SyntheticErc721MetadataCommandHandler {
    fn supports(&self, command: &dyn Command) -> bool {
        command.as_any().is::<FetchErc721MetadataCommand>()
    }

    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<FetchErc721MetadataCommand>()
            .map_err(|_| anyhow::anyhow!("synthetic ERC721 handler received unexpected command"))?;
        let command = *command;
        let suffix = token_suffix(command.token);
        let name = format!("Synthetic ERC721 {suffix}");
        let symbol = format!("N{suffix}");
        self.storage
            .upsert_token_metadata(
                command.token,
                Some(&name),
                Some(&symbol),
                Some(10_000u64.into()),
            )
            .await
    }
}

#[torii::async_trait]
impl CommandHandler for SyntheticErc721TokenUriCommandHandler {
    fn supports(&self, command: &dyn Command) -> bool {
        command.as_any().is::<RefreshErc721TokenUriCommand>()
    }

    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<RefreshErc721TokenUriCommand>()
            .map_err(|_| {
                anyhow::anyhow!("synthetic ERC721 URI handler received unexpected command")
            })?;
        let command = *command;

        self.storage
            .store_token_uri(&TokenUriResult {
                contract: command.contract,
                token_id: command.token_id,
                uri: Some(format!(
                    "synthetic://erc721/{:#x}/{}",
                    command.contract, command.token_id
                )),
                metadata_json: Some(synthetic_token_uri_metadata("ERC721", command.token_id)),
            })
            .await
    }
}

#[torii::async_trait]
impl CommandHandler for SyntheticErc1155MetadataCommandHandler {
    fn supports(&self, command: &dyn Command) -> bool {
        command.as_any().is::<FetchErc1155MetadataCommand>()
    }

    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<FetchErc1155MetadataCommand>()
            .map_err(|_| {
                anyhow::anyhow!("synthetic ERC1155 handler received unexpected command")
            })?;
        let command = *command;
        let suffix = token_suffix(command.token);
        let name = format!("Synthetic ERC1155 {suffix}");
        let symbol = format!("M{suffix}");
        self.storage
            .upsert_token_metadata(
                command.token,
                Some(&name),
                Some(&symbol),
                Some(10_000u64.into()),
            )
            .await
    }
}

#[torii::async_trait]
impl CommandHandler for SyntheticErc1155TokenUriCommandHandler {
    fn supports(&self, command: &dyn Command) -> bool {
        command.as_any().is::<RefreshErc1155TokenUriCommand>()
    }

    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<RefreshErc1155TokenUriCommand>()
            .map_err(|_| {
                anyhow::anyhow!("synthetic ERC1155 URI handler received unexpected command")
            })?;
        let command = *command;

        self.storage
            .store_token_uri(&TokenUriResult {
                contract: command.contract,
                token_id: command.token_id,
                uri: Some(format!(
                    "synthetic://erc1155/{:#x}/{}",
                    command.contract, command.token_id
                )),
                metadata_json: Some(synthetic_token_uri_metadata("ERC1155", command.token_id)),
            })
            .await
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "torii-tokens-synth")]
#[command(about = "Unified synthetic token profiler for ERC20, ERC721, and ERC1155")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    /// Profile ERC20 token ingestion
    Erc20(Erc20ProfileConfig),
    /// Profile ERC721 token ingestion
    Erc721(Erc721ProfileConfig),
    /// Profile ERC1155 token ingestion
    Erc1155(Erc1155ProfileConfig),
    /// Profile all token types together
    All(AllProfileConfig),
}

#[derive(Parser, Debug, Clone)]
struct Erc20ProfileConfig {
    #[command(flatten)]
    common: CommonConfig,

    #[command(flatten)]
    erc20: Erc20SpecificConfig,
}

#[derive(Parser, Debug, Clone)]
struct Erc721ProfileConfig {
    #[command(flatten)]
    common: CommonConfig,

    #[command(flatten)]
    erc721: Erc721SpecificConfig,
}

#[derive(Parser, Debug, Clone)]
struct Erc1155ProfileConfig {
    #[command(flatten)]
    common: CommonConfig,

    #[command(flatten)]
    erc1155: Erc1155SpecificConfig,
}

#[derive(Parser, Debug, Clone)]
struct AllProfileConfig {
    #[command(flatten)]
    common: CommonConfig,

    #[command(flatten)]
    erc20: Erc20SpecificConfig,

    #[command(flatten)]
    erc721: Erc721SpecificConfig,

    #[command(flatten)]
    erc1155: Erc1155SpecificConfig,
}

#[derive(Parser, Debug, Clone)]
struct CommonConfig {
    /// PostgreSQL URL for storage.
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://torii:torii@localhost:5432/torii"
    )]
    db_url: String,

    /// Starting block number for synthetic generation.
    #[arg(long, default_value_t = 1_000_000)]
    from_block: u64,

    /// Total synthetic blocks to generate.
    #[arg(long, default_value_t = 200)]
    block_count: u64,

    /// Transactions generated per block.
    #[arg(long, default_value_t = 1_000)]
    tx_per_block: usize,

    /// Blocks generated per extract cycle.
    #[arg(long, default_value_t = 1)]
    blocks_per_batch: u64,

    /// Number of token contracts used in the workload.
    #[arg(long, default_value_t = 16)]
    token_count: usize,

    /// Number of wallets used in the workload.
    #[arg(long, default_value_t = 20_000)]
    wallet_count: usize,

    /// Deterministic seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Output root for run artifacts.
    #[arg(long, default_value = "perf/runs")]
    output_root: PathBuf,

    /// Drop and recreate schemas before each run.
    #[arg(
        long,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        action = clap::ArgAction::Set
    )]
    reset_schema: bool,
}

#[derive(Parser, Debug, Clone)]
struct Erc20SpecificConfig {
    /// Approval ratio in basis points (2000 = 20%).
    #[arg(long, default_value_t = 2_000)]
    approval_ratio_bps: u16,
}

#[derive(Parser, Debug, Clone)]
struct Erc721SpecificConfig {
    /// Approval ratio in basis points.
    #[arg(long, default_value_t = 500)]
    approval_ratio_bps: u16,

    /// ApprovalForAll ratio in basis points.
    #[arg(long, default_value_t = 200)]
    approval_for_all_ratio_bps: u16,

    /// MetadataUpdate ratio in basis points.
    #[arg(long, default_value_t = 50)]
    metadata_update_ratio_bps: u16,

    /// BatchMetadataUpdate ratio in basis points.
    #[arg(long, default_value_t = 20)]
    batch_metadata_update_ratio_bps: u16,

    /// Maximum token ID for NFTs.
    #[arg(long, default_value_t = 10_000)]
    max_token_id: u64,
}

#[derive(Parser, Debug, Clone)]
struct Erc1155SpecificConfig {
    /// TransferSingle ratio in basis points.
    #[arg(long, default_value_t = 6_000)]
    transfer_single_ratio_bps: u16,

    /// TransferBatch ratio in basis points.
    #[arg(long, default_value_t = 2_000)]
    transfer_batch_ratio_bps: u16,

    /// Minimum batch size for TransferBatch.
    #[arg(long, default_value_t = 1)]
    min_batch_size: usize,

    /// Maximum batch size for TransferBatch.
    #[arg(long, default_value_t = 5)]
    max_batch_size: usize,

    /// ApprovalForAll ratio in basis points.
    #[arg(long, default_value_t = 500)]
    approval_for_all_ratio_bps: u16,

    /// URI ratio in basis points.
    #[arg(long, default_value_t = 100)]
    uri_ratio_bps: u16,

    /// Number of unique token IDs per contract.
    #[arg(long, default_value_t = 100)]
    token_id_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct StageSample {
    extract_ms: f64,
    decode_ms: f64,
    sink_ms: f64,
    commit_ms: f64,
    loop_total_ms: f64,
    events: usize,
    envelopes: usize,
    transactions: usize,
    blocks: usize,
}

#[derive(Debug, Serialize)]
struct RunReport {
    run_id: String,
    token_type: String,
    started_at_utc: String,
    finished_at_utc: String,
    duration_ms: u128,
    totals: Totals,
    throughput: Throughput,
    stage_latency_ms: StageLatency,
}

#[derive(Debug, Serialize)]
struct Totals {
    cycles: usize,
    blocks: usize,
    transactions: usize,
    events: usize,
    envelopes: usize,
}

#[derive(Debug, Serialize)]
struct Throughput {
    blocks_per_sec: f64,
    tx_per_sec: f64,
    events_per_sec: f64,
    envelopes_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct StageLatency {
    extract: Stats,
    decode: Stats,
    sink: Stats,
    commit: Stats,
    loop_total: Stats,
}

#[derive(Debug, Serialize)]
struct Stats {
    total_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Erc20(config) => run_erc20_profile(config).await?,
        Commands::Erc721(config) => run_erc721_profile(config).await?,
        Commands::Erc1155(config) => run_erc1155_profile(config).await?,
        Commands::All(config) => run_all_profile(config).await?,
    }

    Ok(())
}

async fn run_erc20_profile(config: Erc20ProfileConfig) -> Result<()> {
    let run_id = generate_run_id();
    let started_wall = chrono::Utc::now();
    let run_start = Instant::now();

    tracing::info!(
        run_id = %run_id,
        token_type = "erc20",
        from_block = config.common.from_block,
        block_count = config.common.block_count,
        tx_per_block = config.common.tx_per_block,
        "Starting ERC20 synthetic profile run"
    );

    if config.common.reset_schema {
        reset_erc20_schema(&config.common.db_url).await?;
    }

    let output_dir = config.common.output_root.join(&run_id);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;

    let storage = Arc::new(Erc20Storage::new(&config.common.db_url).await?);

    let mut sink = Erc20Sink::new(storage.clone()).with_metadata_pipeline(
        SYNTH_METADATA_COMMAND_PARALLELISM,
        SYNTH_COMMAND_QUEUE_SIZE,
        SYNTH_METADATA_MAX_RETRIES,
    );
    let sink_runtime = initialize_sink_with_command_handlers(
        &mut sink,
        output_dir.clone(),
        vec![Box::new(SyntheticErc20MetadataCommandHandler::new(
            storage.clone(),
        ))],
        SYNTH_COMMAND_QUEUE_SIZE,
    )
    .await?;

    let synthetic_cfg = SyntheticErc20Config {
        from_block: config.common.from_block,
        block_count: config.common.block_count,
        tx_per_block: config.common.tx_per_block,
        blocks_per_batch: config.common.blocks_per_batch,
        approval_ratio_bps: config.erc20.approval_ratio_bps,
        token_count: config.common.token_count,
        wallet_count: config.common.wallet_count,
        seed: config.common.seed,
    };

    let mut extractor = SyntheticErc20Extractor::new(synthetic_cfg)?;

    let engine_db = EngineDb::new(EngineDbConfig {
        path: "sqlite::memory:".to_string(),
    })
    .await?;
    let engine_db = Arc::new(engine_db);

    let decoder = Arc::new(Erc20Decoder::new()) as Arc<dyn Decoder>;
    let decoder_context =
        DecoderContext::new(vec![decoder], engine_db.clone(), ContractFilter::new());

    let mut samples = Vec::new();
    let mut cursor = None;

    loop {
        let cycle_start = Instant::now();

        let extract_start = Instant::now();
        let batch = extractor.extract(cursor.clone()).await?;
        let extract_ms = ms(extract_start.elapsed());

        if batch.is_empty() {
            if extractor.is_finished() {
                break;
            }
            continue;
        }

        let decode_start = Instant::now();
        let envelopes = decoder_context.decode_events(&batch.events).await?;
        let decode_ms = ms(decode_start.elapsed());

        let sink_start = Instant::now();
        sink.process(&envelopes, &batch).await?;
        let sink_ms = ms(sink_start.elapsed());

        let commit_start = Instant::now();
        if let Some(ref new_cursor) = batch.cursor {
            cursor = Some(new_cursor.clone());
        }
        let commit_ms = ms(commit_start.elapsed());

        let loop_total_ms = ms(cycle_start.elapsed());

        samples.push(StageSample {
            extract_ms,
            decode_ms,
            sink_ms,
            commit_ms,
            loop_total_ms,
            events: batch.events.len(),
            envelopes: envelopes.len(),
            transactions: batch.transactions.len(),
            blocks: batch.blocks.len(),
        });
    }

    let finished_wall = chrono::Utc::now();
    let run_elapsed = run_start.elapsed();

    let report = build_report(
        "erc20",
        &run_id,
        started_wall,
        finished_wall,
        run_elapsed,
        &samples,
    );

    write_artifacts(&output_dir, &report)?;
    sink_runtime.shutdown().await;

    tracing::info!(
        run_id = %run_id,
        output = %output_dir.display(),
        duration_ms = report.duration_ms,
        cycles = report.totals.cycles,
        events = report.totals.events,
        "ERC20 synthetic profile run complete"
    );

    Ok(())
}

async fn run_erc721_profile(config: Erc721ProfileConfig) -> Result<()> {
    let run_id = generate_run_id();
    let started_wall = chrono::Utc::now();
    let run_start = Instant::now();

    tracing::info!(
        run_id = %run_id,
        token_type = "erc721",
        from_block = config.common.from_block,
        block_count = config.common.block_count,
        tx_per_block = config.common.tx_per_block,
        "Starting ERC721 synthetic profile run"
    );

    if config.common.reset_schema {
        reset_erc721_schema(&config.common.db_url).await?;
    }

    let output_dir = config.common.output_root.join(&run_id);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;

    let storage = Arc::new(Erc721Storage::new(&config.common.db_url).await?);

    let mut sink = Erc721Sink::new(storage.clone())
        .with_metadata_commands()
        .with_token_uri_commands();
    let sink_runtime = initialize_sink_with_command_handlers(
        &mut sink,
        output_dir.clone(),
        vec![
            Box::new(SyntheticErc721MetadataCommandHandler::new(storage.clone())),
            Box::new(SyntheticErc721TokenUriCommandHandler::new(storage.clone())),
        ],
        SYNTH_COMMAND_QUEUE_SIZE,
    )
    .await?;

    let synthetic_cfg = SyntheticErc721Config {
        from_block: config.common.from_block,
        block_count: config.common.block_count,
        tx_per_block: config.common.tx_per_block,
        blocks_per_batch: config.common.blocks_per_batch,
        approval_ratio_bps: config.erc721.approval_ratio_bps,
        approval_for_all_ratio_bps: config.erc721.approval_for_all_ratio_bps,
        metadata_update_ratio_bps: config.erc721.metadata_update_ratio_bps,
        batch_metadata_update_ratio_bps: config.erc721.batch_metadata_update_ratio_bps,
        token_count: config.common.token_count,
        wallet_count: config.common.wallet_count,
        max_token_id: config.erc721.max_token_id,
        seed: config.common.seed,
    };

    let mut extractor = SyntheticErc721Extractor::new(synthetic_cfg)?;

    let engine_db = EngineDb::new(EngineDbConfig {
        path: "sqlite::memory:".to_string(),
    })
    .await?;
    let engine_db = Arc::new(engine_db);

    let decoder = Arc::new(Erc721Decoder::new()) as Arc<dyn Decoder>;
    let decoder_context =
        DecoderContext::new(vec![decoder], engine_db.clone(), ContractFilter::new());

    let mut samples = Vec::new();
    let mut cursor = None;

    loop {
        let cycle_start = Instant::now();

        let extract_start = Instant::now();
        let batch = extractor.extract(cursor.clone()).await?;
        let extract_ms = ms(extract_start.elapsed());

        if batch.is_empty() {
            if extractor.is_finished() {
                break;
            }
            continue;
        }

        let decode_start = Instant::now();
        let envelopes = decoder_context.decode_events(&batch.events).await?;
        let decode_ms = ms(decode_start.elapsed());

        let sink_start = Instant::now();
        sink.process(&envelopes, &batch).await?;
        let sink_ms = ms(sink_start.elapsed());

        let commit_start = Instant::now();
        if let Some(ref new_cursor) = batch.cursor {
            cursor = Some(new_cursor.clone());
        }
        let commit_ms = ms(commit_start.elapsed());

        let loop_total_ms = ms(cycle_start.elapsed());

        samples.push(StageSample {
            extract_ms,
            decode_ms,
            sink_ms,
            commit_ms,
            loop_total_ms,
            events: batch.events.len(),
            envelopes: envelopes.len(),
            transactions: batch.transactions.len(),
            blocks: batch.blocks.len(),
        });
    }

    let finished_wall = chrono::Utc::now();
    let run_elapsed = run_start.elapsed();

    let report = build_report(
        "erc721",
        &run_id,
        started_wall,
        finished_wall,
        run_elapsed,
        &samples,
    );

    write_artifacts(&output_dir, &report)?;
    sink_runtime.shutdown().await;

    tracing::info!(
        run_id = %run_id,
        output = %output_dir.display(),
        duration_ms = report.duration_ms,
        cycles = report.totals.cycles,
        events = report.totals.events,
        "ERC721 synthetic profile run complete"
    );

    Ok(())
}

async fn run_erc1155_profile(config: Erc1155ProfileConfig) -> Result<()> {
    let run_id = generate_run_id();
    let started_wall = chrono::Utc::now();
    let run_start = Instant::now();

    tracing::info!(
        run_id = %run_id,
        token_type = "erc1155",
        from_block = config.common.from_block,
        block_count = config.common.block_count,
        tx_per_block = config.common.tx_per_block,
        "Starting ERC1155 synthetic profile run"
    );

    if config.common.reset_schema {
        reset_erc1155_schema(&config.common.db_url).await?;
    }

    let output_dir = config.common.output_root.join(&run_id);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;

    let storage = Arc::new(Erc1155Storage::new(&config.common.db_url).await?);

    let mut sink = Erc1155Sink::new(storage.clone())
        .with_metadata_commands()
        .with_token_uri_commands();
    let sink_runtime = initialize_sink_with_command_handlers(
        &mut sink,
        output_dir.clone(),
        vec![
            Box::new(SyntheticErc1155MetadataCommandHandler::new(storage.clone())),
            Box::new(SyntheticErc1155TokenUriCommandHandler::new(storage.clone())),
        ],
        SYNTH_COMMAND_QUEUE_SIZE,
    )
    .await?;

    let synthetic_cfg = SyntheticErc1155Config {
        from_block: config.common.from_block,
        block_count: config.common.block_count,
        tx_per_block: config.common.tx_per_block,
        blocks_per_batch: config.common.blocks_per_batch,
        transfer_single_ratio_bps: config.erc1155.transfer_single_ratio_bps,
        transfer_batch_ratio_bps: config.erc1155.transfer_batch_ratio_bps,
        min_batch_size: config.erc1155.min_batch_size,
        max_batch_size: config.erc1155.max_batch_size,
        approval_for_all_ratio_bps: config.erc1155.approval_for_all_ratio_bps,
        uri_ratio_bps: config.erc1155.uri_ratio_bps,
        token_count: config.common.token_count,
        token_id_count: config.erc1155.token_id_count,
        wallet_count: config.common.wallet_count,
        seed: config.common.seed,
    };

    let mut extractor = SyntheticErc1155Extractor::new(synthetic_cfg)?;

    let engine_db = EngineDb::new(EngineDbConfig {
        path: "sqlite::memory:".to_string(),
    })
    .await?;
    let engine_db = Arc::new(engine_db);

    let decoder = Arc::new(Erc1155Decoder::new()) as Arc<dyn Decoder>;
    let decoder_context =
        DecoderContext::new(vec![decoder], engine_db.clone(), ContractFilter::new());

    let mut samples = Vec::new();
    let mut cursor = None;

    loop {
        let cycle_start = Instant::now();

        let extract_start = Instant::now();
        let batch = extractor.extract(cursor.clone()).await?;
        let extract_ms = ms(extract_start.elapsed());

        if batch.is_empty() {
            if extractor.is_finished() {
                break;
            }
            continue;
        }

        let decode_start = Instant::now();
        let envelopes = decoder_context.decode_events(&batch.events).await?;
        let decode_ms = ms(decode_start.elapsed());

        let sink_start = Instant::now();
        sink.process(&envelopes, &batch).await?;
        let sink_ms = ms(sink_start.elapsed());

        let commit_start = Instant::now();
        if let Some(ref new_cursor) = batch.cursor {
            cursor = Some(new_cursor.clone());
        }
        let commit_ms = ms(commit_start.elapsed());

        let loop_total_ms = ms(cycle_start.elapsed());

        samples.push(StageSample {
            extract_ms,
            decode_ms,
            sink_ms,
            commit_ms,
            loop_total_ms,
            events: batch.events.len(),
            envelopes: envelopes.len(),
            transactions: batch.transactions.len(),
            blocks: batch.blocks.len(),
        });
    }

    let finished_wall = chrono::Utc::now();
    let run_elapsed = run_start.elapsed();

    let report = build_report(
        "erc1155",
        &run_id,
        started_wall,
        finished_wall,
        run_elapsed,
        &samples,
    );

    write_artifacts(&output_dir, &report)?;
    sink_runtime.shutdown().await;

    tracing::info!(
        run_id = %run_id,
        output = %output_dir.display(),
        duration_ms = report.duration_ms,
        cycles = report.totals.cycles,
        events = report.totals.events,
        "ERC1155 synthetic profile run complete"
    );

    Ok(())
}

async fn run_all_profile(config: AllProfileConfig) -> Result<()> {
    tracing::info!("Running all token type profiles sequentially");

    let erc20_config = Erc20ProfileConfig {
        common: config.common.clone(),
        erc20: config.erc20,
    };
    run_erc20_profile(erc20_config).await?;

    let erc721_config = Erc721ProfileConfig {
        common: config.common.clone(),
        erc721: config.erc721,
    };
    run_erc721_profile(erc721_config).await?;

    let erc1155_config = Erc1155ProfileConfig {
        common: config.common,
        erc1155: config.erc1155,
    };
    run_erc1155_profile(erc1155_config).await?;

    tracing::info!("All token type profiles complete");
    Ok(())
}

async fn reset_erc20_schema(db_url: &str) -> Result<()> {
    drop_postgres_schemas(db_url, &["erc20"], "torii_tokens_synth").await
}

async fn reset_erc721_schema(db_url: &str) -> Result<()> {
    drop_postgres_schemas(db_url, &["erc721"], "torii_tokens_synth").await
}

async fn reset_erc1155_schema(db_url: &str) -> Result<()> {
    drop_postgres_schemas(db_url, &["erc1155"], "torii_tokens_synth").await
}

fn build_report(
    token_type: &str,
    run_id: &str,
    started_wall: chrono::DateTime<chrono::Utc>,
    finished_wall: chrono::DateTime<chrono::Utc>,
    elapsed: Duration,
    samples: &[StageSample],
) -> RunReport {
    let total_blocks: usize = samples.iter().map(|s| s.blocks).sum();
    let total_txs: usize = samples.iter().map(|s| s.transactions).sum();
    let total_events: usize = samples.iter().map(|s| s.events).sum();
    let total_envelopes: usize = samples.iter().map(|s| s.envelopes).sum();

    let elapsed_secs = elapsed.as_secs_f64().max(0.001);

    RunReport {
        run_id: run_id.to_string(),
        token_type: token_type.to_string(),
        started_at_utc: started_wall.to_rfc3339(),
        finished_at_utc: finished_wall.to_rfc3339(),
        duration_ms: elapsed.as_millis(),
        totals: Totals {
            cycles: samples.len(),
            blocks: total_blocks,
            transactions: total_txs,
            events: total_events,
            envelopes: total_envelopes,
        },
        throughput: Throughput {
            blocks_per_sec: total_blocks as f64 / elapsed_secs,
            tx_per_sec: total_txs as f64 / elapsed_secs,
            events_per_sec: total_events as f64 / elapsed_secs,
            envelopes_per_sec: total_envelopes as f64 / elapsed_secs,
        },
        stage_latency_ms: StageLatency {
            extract: stats(samples.iter().map(|s| s.extract_ms).collect()),
            decode: stats(samples.iter().map(|s| s.decode_ms).collect()),
            sink: stats(samples.iter().map(|s| s.sink_ms).collect()),
            commit: stats(samples.iter().map(|s| s.commit_ms).collect()),
            loop_total: stats(samples.iter().map(|s| s.loop_total_ms).collect()),
        },
    }
}

fn stats(values: Vec<f64>) -> Stats {
    if values.is_empty() {
        return Stats {
            total_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
        };
    }

    let mut sorted = values.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let total_ms: f64 = values.iter().sum();
    let max_ms = sorted.last().copied().unwrap_or(0.0);
    let p50_ms = percentile(&sorted, 50.0);
    let p95_ms = percentile(&sorted, 95.0);
    let p99_ms = percentile(&sorted, 99.0);

    Stats {
        total_ms,
        p50_ms,
        p95_ms,
        p99_ms,
        max_ms,
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn write_artifacts(output_dir: &Path, report: &RunReport) -> Result<()> {
    let json_path = output_dir.join("report.json");
    let json = serde_json::to_string_pretty(report)?;
    fs::write(&json_path, json)?;

    Ok(())
}

fn generate_run_id() -> String {
    let now_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    format!("run_{now_epoch}")
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}
