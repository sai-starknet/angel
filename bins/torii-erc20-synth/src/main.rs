use anyhow::{Context, Result};
use clap::Parser;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use torii::command::{Command, CommandHandler};
use torii::etl::decoder::ContractFilter;
use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::extractor::{Extractor, SyntheticErc20Config, SyntheticErc20Extractor};
use torii::etl::sink::Sink;
use torii::etl::{Decoder, DecoderContext};
use torii_erc20::handlers::FetchErc20MetadataCommand;
use torii_erc20::{Erc20Decoder, Erc20Sink, Erc20Storage};
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

    fn suffix(token: impl std::fmt::LowerHex) -> String {
        let token_hex = format!("{token:#x}");
        let trimmed = token_hex.trim_start_matches("0x");
        let start = trimmed.len().saturating_sub(6);
        trimmed[start..].to_uppercase()
    }
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

        let suffix = Self::suffix(command.token);
        let name = format!("Synthetic ERC20 {suffix}");
        let symbol = format!("S{suffix}");

        self.storage
            .upsert_token_metadata(command.token, Some(&name), Some(&symbol), Some(18), None)
            .await
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "torii-erc20-synth")]
#[command(about = "Deterministic end-to-end synthetic ERC20 ingestion profiler")]
struct Config {
    /// PostgreSQL URL for ERC20 storage.
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

    /// Approval ratio in basis points (2000 = 20%).
    #[arg(long, default_value_t = 2_000)]
    approval_ratio_bps: u16,

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

    /// Drop and recreate `erc20` schema before each run.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    reset_schema: bool,
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
    started_at_utc: String,
    finished_at_utc: String,
    duration_ms: u128,
    config: ConfigSnapshot,
    totals: Totals,
    throughput: Throughput,
    stage_latency_ms: StageLatency,
    stage_share_percent: HashMap<String, f64>,
    slowest_cycles: Vec<SlowCycle>,
    blocking_suspects: Vec<BlockingSuspect>,
    db: DbSummary,
}

#[derive(Debug, Serialize)]
struct ConfigSnapshot {
    from_block: u64,
    block_count: u64,
    tx_per_block: usize,
    blocks_per_batch: u64,
    approval_ratio_bps: u16,
    token_count: usize,
    wallet_count: usize,
    seed: u64,
    db_url_redacted: String,
    reset_schema: bool,
    pg_pool_size_env: Option<String>,
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
struct StageStats {
    total_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Serialize)]
struct StageLatency {
    extract: StageStats,
    decode: StageStats,
    sink: StageStats,
    commit: StageStats,
    loop_total: StageStats,
}

#[derive(Debug, Serialize)]
struct SlowCycle {
    cycle: usize,
    loop_total_ms: f64,
    dominant_stage: String,
    blocks: usize,
    transactions: usize,
    events: usize,
    envelopes: usize,
}

#[derive(Debug, Serialize)]
struct BlockingSuspect {
    stage: String,
    total_ms: f64,
    share_percent: f64,
}

#[derive(Debug, Serialize)]
struct DbSummary {
    transfers: u64,
    approvals: u64,
    unique_tokens: u64,
    latest_block: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "profiling")]
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .context("failed to start profiler")?;

    let cfg = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    if cfg.reset_schema {
        reset_erc20_schema(&cfg.db_url).await?;
    }

    let started_wall = chrono::Utc::now();
    let run_start = Instant::now();
    let run_id = started_wall.format("%Y%m%dT%H%M%SZ").to_string();

    let output_dir = cfg.output_root.join(&run_id);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;

    let storage = Arc::new(Erc20Storage::new(&cfg.db_url).await?);

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
        from_block: cfg.from_block,
        block_count: cfg.block_count,
        tx_per_block: cfg.tx_per_block,
        blocks_per_batch: cfg.blocks_per_batch,
        approval_ratio_bps: cfg.approval_ratio_bps,
        token_count: cfg.token_count,
        wallet_count: cfg.wallet_count,
        seed: cfg.seed,
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
        let batch = extractor.extract(cursor.clone(), &engine_db).await?;
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
            extractor.commit_cursor(new_cursor, &engine_db).await?;
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
        &cfg,
        &run_id,
        started_wall,
        finished_wall,
        run_elapsed,
        &samples,
        &storage,
    )
    .await?;

    write_artifacts(&output_dir, &report)?;

    #[cfg(feature = "profiling")]
    write_flamegraph(&output_dir, guard)?;

    sink_runtime.shutdown().await;

    tracing::info!(
        run_id = %run_id,
        output = %output_dir.display(),
        duration_ms = report.duration_ms,
        cycles = report.totals.cycles,
        events = report.totals.events,
        tx = report.totals.transactions,
        "Synthetic ERC20 run complete"
    );

    Ok(())
}

async fn reset_erc20_schema(db_url: &str) -> Result<()> {
    drop_postgres_schemas(db_url, &["erc20"], "torii_erc20_synth").await
}

async fn build_report(
    cfg: &Config,
    run_id: &str,
    started_wall: chrono::DateTime<chrono::Utc>,
    finished_wall: chrono::DateTime<chrono::Utc>,
    elapsed: Duration,
    samples: &[StageSample],
    storage: &Erc20Storage,
) -> Result<RunReport> {
    let total_blocks: usize = samples.iter().map(|s| s.blocks).sum();
    let total_txs: usize = samples.iter().map(|s| s.transactions).sum();
    let total_events: usize = samples.iter().map(|s| s.events).sum();
    let total_envelopes: usize = samples.iter().map(|s| s.envelopes).sum();

    let elapsed_secs = elapsed.as_secs_f64().max(0.001);

    let stage_latency = StageLatency {
        extract: stats(samples.iter().map(|s| s.extract_ms).collect()),
        decode: stats(samples.iter().map(|s| s.decode_ms).collect()),
        sink: stats(samples.iter().map(|s| s.sink_ms).collect()),
        commit: stats(samples.iter().map(|s| s.commit_ms).collect()),
        loop_total: stats(samples.iter().map(|s| s.loop_total_ms).collect()),
    };

    let mut stage_share_percent = HashMap::new();
    let loop_total = stage_latency.loop_total.total_ms.max(0.001);
    stage_share_percent.insert(
        "extract".to_string(),
        (stage_latency.extract.total_ms / loop_total) * 100.0,
    );
    stage_share_percent.insert(
        "decode".to_string(),
        (stage_latency.decode.total_ms / loop_total) * 100.0,
    );
    stage_share_percent.insert(
        "sink".to_string(),
        (stage_latency.sink.total_ms / loop_total) * 100.0,
    );
    stage_share_percent.insert(
        "commit".to_string(),
        (stage_latency.commit.total_ms / loop_total) * 100.0,
    );

    let mut blocking_suspects = vec![
        BlockingSuspect {
            stage: "extract".to_string(),
            total_ms: stage_latency.extract.total_ms,
            share_percent: *stage_share_percent.get("extract").unwrap_or(&0.0),
        },
        BlockingSuspect {
            stage: "decode".to_string(),
            total_ms: stage_latency.decode.total_ms,
            share_percent: *stage_share_percent.get("decode").unwrap_or(&0.0),
        },
        BlockingSuspect {
            stage: "sink".to_string(),
            total_ms: stage_latency.sink.total_ms,
            share_percent: *stage_share_percent.get("sink").unwrap_or(&0.0),
        },
        BlockingSuspect {
            stage: "commit".to_string(),
            total_ms: stage_latency.commit.total_ms,
            share_percent: *stage_share_percent.get("commit").unwrap_or(&0.0),
        },
    ];
    blocking_suspects.sort_by(|a, b| {
        b.share_percent
            .partial_cmp(&a.share_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut indexed: Vec<(usize, &StageSample)> = samples.iter().enumerate().collect();
    indexed.sort_by(|a, b| {
        b.1.loop_total_ms
            .partial_cmp(&a.1.loop_total_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let slowest_cycles = indexed
        .into_iter()
        .take(5)
        .map(|(idx, sample)| SlowCycle {
            cycle: idx + 1,
            loop_total_ms: sample.loop_total_ms,
            dominant_stage: dominant_stage(sample),
            blocks: sample.blocks,
            transactions: sample.transactions,
            events: sample.events,
            envelopes: sample.envelopes,
        })
        .collect();

    let db = DbSummary {
        transfers: storage.get_transfer_count().await?,
        approvals: storage.get_approval_count().await?,
        unique_tokens: storage.get_token_count().await?,
        latest_block: storage.get_latest_block().await?,
    };

    Ok(RunReport {
        run_id: run_id.to_string(),
        started_at_utc: started_wall.to_rfc3339(),
        finished_at_utc: finished_wall.to_rfc3339(),
        duration_ms: elapsed.as_millis(),
        config: ConfigSnapshot {
            from_block: cfg.from_block,
            block_count: cfg.block_count,
            tx_per_block: cfg.tx_per_block,
            blocks_per_batch: cfg.blocks_per_batch,
            approval_ratio_bps: cfg.approval_ratio_bps,
            token_count: cfg.token_count,
            wallet_count: cfg.wallet_count,
            seed: cfg.seed,
            db_url_redacted: redact_db_url(&cfg.db_url),
            reset_schema: cfg.reset_schema,
            pg_pool_size_env: std::env::var("TORII_ERC20_PG_POOL_SIZE").ok(),
        },
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
        stage_latency_ms: stage_latency,
        stage_share_percent,
        slowest_cycles,
        blocking_suspects,
        db,
    })
}

fn write_artifacts(output_dir: &Path, report: &RunReport) -> Result<()> {
    let json_path = output_dir.join("report.json");
    let md_path = output_dir.join("report.md");
    let env_path = output_dir.join("context.env");

    let json = serde_json::to_string_pretty(report)?;
    fs::write(&json_path, json)?;

    fs::write(&md_path, render_markdown(report))?;

    let now_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();

    let env = format!(
        "run_id={}\nstarted_at={}\nfinished_at={}\nduration_ms={}\ncreated_epoch={}\n",
        report.run_id, report.started_at_utc, report.finished_at_utc, report.duration_ms, now_epoch
    );
    fs::write(&env_path, env)?;

    Ok(())
}

#[cfg(feature = "profiling")]
fn write_flamegraph(output_dir: &Path, guard: pprof::ProfilerGuard<'_>) -> Result<()> {
    if let Ok(report) = guard.report().build() {
        let file = fs::File::create(output_dir.join("flamegraph.svg"))?;
        report.flamegraph(file)?;
    }
    Ok(())
}

fn render_markdown(report: &RunReport) -> String {
    let stage = &report.stage_latency_ms;
    let suspects = report
        .blocking_suspects
        .iter()
        .map(|s| {
            format!(
                "- `{}`: {:.2}% ({:.2}ms)",
                s.stage, s.share_percent, s.total_ms
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "# ERC20 Synthetic Ingestion Report\n\nrun_id: `{}`\n\n## Totals\n- cycles: {}\n- blocks: {}\n- transactions: {}\n- events: {}\n- envelopes: {}\n\n## Throughput\n- blocks/sec: {:.2}\n- tx/sec: {:.2}\n- events/sec: {:.2}\n- envelopes/sec: {:.2}\n\n## Stage Latency (ms)\n| Stage | Total | p50 | p95 | p99 | max |\n|---|---:|---:|---:|---:|---:|\n| extract | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n| decode | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n| sink | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n| commit | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n| loop_total | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n\n## Blocking Suspects\n{}\n\n## DB Summary\n- transfers: {}\n- approvals: {}\n- unique_tokens: {}\n- latest_block: {:?}\n",
        report.run_id,
        report.totals.cycles,
        report.totals.blocks,
        report.totals.transactions,
        report.totals.events,
        report.totals.envelopes,
        report.throughput.blocks_per_sec,
        report.throughput.tx_per_sec,
        report.throughput.events_per_sec,
        report.throughput.envelopes_per_sec,
        stage.extract.total_ms,
        stage.extract.p50_ms,
        stage.extract.p95_ms,
        stage.extract.p99_ms,
        stage.extract.max_ms,
        stage.decode.total_ms,
        stage.decode.p50_ms,
        stage.decode.p95_ms,
        stage.decode.p99_ms,
        stage.decode.max_ms,
        stage.sink.total_ms,
        stage.sink.p50_ms,
        stage.sink.p95_ms,
        stage.sink.p99_ms,
        stage.sink.max_ms,
        stage.commit.total_ms,
        stage.commit.p50_ms,
        stage.commit.p95_ms,
        stage.commit.p99_ms,
        stage.commit.max_ms,
        stage.loop_total.total_ms,
        stage.loop_total.p50_ms,
        stage.loop_total.p95_ms,
        stage.loop_total.p99_ms,
        stage.loop_total.max_ms,
        suspects,
        report.db.transfers,
        report.db.approvals,
        report.db.unique_tokens,
        report.db.latest_block,
    )
}

fn stats(mut values: Vec<f64>) -> StageStats {
    if values.is_empty() {
        return StageStats {
            total_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
        };
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let total: f64 = values.iter().sum();
    let max = *values.last().unwrap_or(&0.0);

    StageStats {
        total_ms: total,
        p50_ms: percentile(&values, 0.50),
        p95_ms: percentile(&values, 0.95),
        p99_ms: percentile(&values, 0.99),
        max_ms: max,
    }
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * pct).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn dominant_stage(s: &StageSample) -> String {
    let mut stages = [
        ("extract", s.extract_ms),
        ("decode", s.decode_ms),
        ("sink", s.sink_ms),
        ("commit", s.commit_ms),
    ];
    stages.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    stages[0].0.to_string()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

fn redact_db_url(db_url: &str) -> String {
    if let Some((scheme, rest)) = db_url.split_once("://") {
        let host_part = rest.rsplit('@').next().unwrap_or(rest);
        return format!("{scheme}://***@{host_part}");
    }
    "***".to_string()
}
