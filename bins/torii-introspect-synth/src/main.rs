use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use clap::Parser;
use dojo_introspect::events::{ModelWithSchemaRegistered, StoreSetRecord, StoreUpdateMember};
use dojo_introspect::selector::compute_selector_from_namespace_and_name;
use dojo_introspect::serde::primitive;
use dojo_introspect::{DojoIntrospectResult, DojoSchema, DojoSchemaFetcher};
use introspect_types::utils::string_to_cairo_serialize_byte_array;
use introspect_types::CairoEventInfo;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use starknet_types_raw::Felt;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use torii::command::CommandBus;
use torii::etl::decoder::{ContractFilter, DecoderContext};
use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::extractor::{BlockContext, ExtractionBatch, Extractor, TransactionContext};
use torii::etl::sink::{EventBus, Sink, SinkContext};
use torii::etl::Decoder;
use torii::grpc::SubscriptionManager;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::store::DojoStoreTrait;
use torii_introspect_sql_sink::IntrospectPgDb;
use torii_types::event::StarknetEvent;

const EXTRACTOR_TYPE: &str = "synthetic_introspect";
const STATE_KEY: &str = "last_block";
const NAMESPACE: &str = "synthetic";
const MODEL_NAME: &str = "position";
const TABLE_SCHEMA: &str = "synthetic";
const TABLE_NAME: &str = "synthetic-position";
const FROM_ADDRESS: Felt = Felt::from_hex_unchecked("0x100");

#[derive(Parser, Debug, Clone, Serialize)]
#[command(name = "torii-introspect-synth")]
#[command(about = "Deterministic end-to-end synthetic Dojo introspect verification runner")]
struct Config {
    /// PostgreSQL URL for the introspect sink.
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
    #[arg(long, default_value_t = 50)]
    block_count: u64,

    /// Records generated per block.
    #[arg(long, default_value_t = 250)]
    records_per_block: usize,

    /// Blocks generated per extract cycle.
    #[arg(long, default_value_t = 1)]
    blocks_per_batch: u64,

    /// Deterministic seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Output root for run artifacts.
    #[arg(long, default_value = "perf/synthetic-runs")]
    output_root: PathBuf,

    /// Drop and recreate the synthetic introspect state before each run.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    reset_schema: bool,
}

#[derive(Debug, Serialize)]
struct Summary {
    run_id: String,
    duration_ms: u128,
    config: Config,
    total_events: usize,
    total_envelopes: usize,
    total_rows: u64,
    verified_entity_id_hex: String,
    verified_score: i64,
}

#[derive(Debug, Clone)]
struct SyntheticIntrospectConfig {
    from_block: u64,
    block_count: u64,
    records_per_block: usize,
    blocks_per_batch: u64,
    seed: u64,
}

impl SyntheticIntrospectConfig {
    fn validate(&self) -> Result<()> {
        ensure!(self.block_count > 0, "block_count must be > 0");
        ensure!(self.records_per_block > 0, "records_per_block must be > 0");
        ensure!(self.blocks_per_batch > 0, "blocks_per_batch must be > 0");
        Ok(())
    }

    fn to_block_inclusive(&self) -> u64 {
        self.from_block + self.block_count - 1
    }
}

struct SyntheticIntrospectExtractor {
    config: SyntheticIntrospectConfig,
    current_block: u64,
    finished: bool,
    initialized: bool,
}

impl SyntheticIntrospectExtractor {
    fn new(config: SyntheticIntrospectConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            current_block: config.from_block,
            config,
            finished: false,
            initialized: false,
        })
    }

    fn to_block_inclusive(&self) -> u64 {
        self.config.to_block_inclusive()
    }

    async fn initialize(&mut self, cursor: Option<String>, engine_db: &EngineDb) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        if let Some(cursor_str) = cursor {
            self.current_block = Self::parse_cursor(&cursor_str)?.saturating_add(1);
        } else if let Some(saved_state) = engine_db
            .get_extractor_state(EXTRACTOR_TYPE, STATE_KEY)
            .await?
        {
            let block = saved_state
                .parse::<u64>()
                .context("invalid saved synthetic introspect block")?;
            self.current_block = block.saturating_add(1);
        }

        if self.current_block > self.to_block_inclusive() {
            self.finished = true;
        }

        self.initialized = true;
        Ok(())
    }

    fn parse_cursor(cursor: &str) -> Result<u64> {
        let block = cursor
            .strip_prefix("synthetic_introspect:block:")
            .context("invalid cursor format, expected synthetic_introspect:block:<n>")?;
        block
            .parse::<u64>()
            .context("invalid synthetic introspect block cursor")
    }

    fn make_cursor(block: u64) -> String {
        format!("synthetic_introspect:block:{block}")
    }

    fn table_id(&self) -> Felt {
        compute_selector_from_namespace_and_name(NAMESPACE, MODEL_NAME).into()
    }

    fn score_selector(&self) -> Felt {
        Felt::selector("score")
    }

    fn model_registration_event(&self, block_number: u64) -> StarknetEvent {
        let mut keys = vec![ModelWithSchemaRegistered::SELECTOR.into()];
        keys.extend(
            string_to_cairo_serialize_byte_array(MODEL_NAME)
                .into_iter()
                .map(Felt::from),
        );
        keys.extend(
            string_to_cairo_serialize_byte_array(NAMESPACE)
                .into_iter()
                .map(Felt::from),
        );

        StarknetEvent {
            from_address: FROM_ADDRESS,
            keys,
            data: encode_legacy_schema(),
            block_number,
            transaction_hash: tx_hash_for(block_number, 0),
        }
    }

    fn entity_id_for(&self, block_number: u64, record_index: usize) -> Felt {
        Felt::from(
            block_number
                .saturating_mul(1_000_000)
                .saturating_add(record_index as u64)
                .saturating_add(self.config.seed),
        )
    }

    fn owner_for(&self, block_number: u64, record_index: usize) -> Felt {
        Felt::from(
            0x0100_0000_u64
                + ((block_number - self.config.from_block) * self.config.records_per_block as u64
                    + record_index as u64
                    + self.config.seed)
                    % 100_000,
        )
    }

    fn build_set_record_event(
        &self,
        block_number: u64,
        record_index: usize,
        entity_id: Felt,
        owner: Felt,
        initial_score: Felt,
    ) -> StarknetEvent {
        let mut data = encode_array([owner]);
        data.extend(encode_array([initial_score]));

        StarknetEvent {
            from_address: FROM_ADDRESS,
            keys: vec![StoreSetRecord::SELECTOR.into(), self.table_id(), entity_id],
            data,
            block_number,
            transaction_hash: tx_hash_for(block_number, record_tx_index(record_index, false)),
        }
    }

    fn build_update_member_event(
        &self,
        block_number: u64,
        record_index: usize,
        entity_id: Felt,
        final_score: Felt,
    ) -> StarknetEvent {
        StarknetEvent {
            from_address: FROM_ADDRESS,
            keys: vec![
                StoreUpdateMember::SELECTOR.into(),
                self.table_id(),
                entity_id,
                self.score_selector(),
            ],
            data: encode_array([final_score]),
            block_number,
            transaction_hash: tx_hash_for(block_number, record_tx_index(record_index, true)),
        }
    }

    fn build_block_batch(&self, start_block: u64, end_block: u64) -> ExtractionBatch {
        let blocks_in_batch = (end_block - start_block + 1) as usize;
        let registration_events = usize::from(start_block <= self.config.from_block);
        let total_events =
            registration_events + blocks_in_batch * self.config.records_per_block * 2;

        let mut events = Vec::with_capacity(total_events);
        let mut blocks = HashMap::with_capacity(blocks_in_batch);
        let mut transactions = HashMap::with_capacity(total_events);

        for block_number in start_block..=end_block {
            blocks.insert(
                block_number,
                Arc::new(BlockContext {
                    number: block_number,
                    hash: block_hash_for(block_number),
                    parent_hash: block_hash_for(block_number.saturating_sub(1)),
                    timestamp: 1_700_000_000 + block_number * 12,
                }),
            );

            if block_number == self.config.from_block {
                let event = self.model_registration_event(block_number);
                transactions.insert(
                    event.transaction_hash,
                    Arc::new(TransactionContext {
                        hash: event.transaction_hash,
                        block_number,
                        sender_address: Some(FROM_ADDRESS),
                        calldata: Vec::new(),
                    }),
                );
                events.push(event);
            }

            for record_index in 0..self.config.records_per_block {
                let entity_id = self.entity_id_for(block_number, record_index);
                let owner = self.owner_for(block_number, record_index);
                let initial_score_value = 100_u64
                    + ((block_number - self.config.from_block)
                        * self.config.records_per_block as u64
                        + record_index as u64)
                        % 10_000;
                let initial_score = Felt::from(initial_score_value);
                let final_score = Felt::from(initial_score_value + 1);

                let set_event = self.build_set_record_event(
                    block_number,
                    record_index,
                    entity_id,
                    owner,
                    initial_score,
                );
                transactions.insert(
                    set_event.transaction_hash,
                    Arc::new(TransactionContext {
                        hash: set_event.transaction_hash,
                        block_number,
                        sender_address: Some(FROM_ADDRESS),
                        calldata: vec![self.table_id(), entity_id, owner, initial_score],
                    }),
                );
                events.push(set_event);

                let update_event = self.build_update_member_event(
                    block_number,
                    record_index,
                    entity_id,
                    final_score,
                );
                transactions.insert(
                    update_event.transaction_hash,
                    Arc::new(TransactionContext {
                        hash: update_event.transaction_hash,
                        block_number,
                        sender_address: Some(FROM_ADDRESS),
                        calldata: vec![self.table_id(), entity_id, final_score],
                    }),
                );
                events.push(update_event);
            }
        }

        ExtractionBatch {
            events,
            blocks,
            transactions,
            declared_classes: Vec::new(),
            deployed_contracts: Vec::new(),
            cursor: Some(Self::make_cursor(end_block)),
            chain_head: Some(self.to_block_inclusive()),
        }
    }
}

#[async_trait]
impl Extractor for SyntheticIntrospectExtractor {
    fn set_start_block(&mut self, start_block: u64) {
        self.current_block = start_block.max(self.current_block);
    }

    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        self.initialize(cursor, engine_db).await?;

        if self.finished {
            return Ok(ExtractionBatch::empty());
        }

        let end_block =
            (self.current_block + self.config.blocks_per_batch - 1).min(self.to_block_inclusive());
        let batch = self.build_block_batch(self.current_block, end_block);
        self.current_block = end_block + 1;
        self.finished = self.current_block > self.to_block_inclusive();
        Ok(batch)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    async fn commit_cursor(&mut self, cursor: &str, engine_db: &EngineDb) -> Result<()> {
        let block = Self::parse_cursor(cursor)?;
        engine_db
            .set_extractor_state(EXTRACTOR_TYPE, STATE_KEY, &block.to_string())
            .await?;
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct NeverFetchSchema;

#[async_trait]
impl DojoSchemaFetcher for NeverFetchSchema {
    async fn schema(
        &self,
        contract_address: starknet::core::types::Felt,
    ) -> DojoIntrospectResult<DojoSchema> {
        panic!("provider fetch should not be called in synthetic introspect run: {contract_address:#x}");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    let run_id = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let output_dir = config.output_root.join(&run_id);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.db_url)
        .await?;
    if config.reset_schema {
        reset_schema(&pool).await?;
    }

    let started = Instant::now();
    let mut sink = IntrospectPgDb::new(pool.clone(), TABLE_SCHEMA);
    let command_bus = CommandBus::new(Vec::new(), 1)?;
    sink.initialize(
        Arc::new(EventBus::new(Arc::new(SubscriptionManager::new()))),
        &SinkContext {
            database_root: output_dir.clone(),
            command_bus: command_bus.sender(),
        },
    )
    .await?;

    let engine_db = Arc::new(
        EngineDb::new(EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await?,
    );

    let decoder = DojoDecoder::new(pool.clone(), NeverFetchSchema);
    decoder.initialize().await?;
    decoder.load_tables(&[]).await?;
    let decoder: Arc<dyn Decoder> = Arc::new(decoder);
    let decoder_context =
        DecoderContext::new(vec![decoder], engine_db.clone(), ContractFilter::new());

    let mut extractor = SyntheticIntrospectExtractor::new(SyntheticIntrospectConfig {
        from_block: config.from_block,
        block_count: config.block_count,
        records_per_block: config.records_per_block,
        blocks_per_batch: config.blocks_per_batch,
        seed: config.seed,
    })?;

    let mut cursor = None;
    let mut total_events = 0usize;
    let mut total_envelopes = 0usize;

    loop {
        let batch = extractor.extract(cursor.clone(), &engine_db).await?;
        if batch.is_empty() {
            if extractor.is_finished() {
                break;
            }
            continue;
        }

        total_events += batch.events.len();
        let envelopes = decoder_context.decode_events(&batch.events).await?;
        total_envelopes += envelopes.len();
        sink.process(&envelopes, &batch).await?;

        if let Some(ref new_cursor) = batch.cursor {
            extractor.commit_cursor(new_cursor, &engine_db).await?;
            cursor = Some(new_cursor.clone());
        }
    }

    let verification = verify_run(&pool, &config).await?;
    let summary = Summary {
        run_id: run_id.clone(),
        duration_ms: started.elapsed().as_millis(),
        config: config.clone(),
        total_events,
        total_envelopes,
        total_rows: verification.total_rows,
        verified_entity_id_hex: verification.verified_entity_id_hex,
        verified_score: verification.verified_score,
    };

    fs::write(
        output_dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )
    .with_context(|| format!("failed to write summary for run {run_id}"))?;

    tracing::info!(
        total_events,
        total_envelopes,
        total_rows = summary.total_rows,
        "Synthetic introspect run completed"
    );

    Ok(())
}

struct Verification {
    total_rows: u64,
    verified_entity_id_hex: String,
    verified_score: i64,
}

async fn reset_schema(pool: &PgPool) -> Result<()> {
    sqlx::query(&format!(
        r#"DROP SCHEMA IF EXISTS "{TABLE_SCHEMA}" CASCADE"#
    ))
    .execute(pool)
    .await?;
    sqlx::query(r"DROP SCHEMA IF EXISTS dojo CASCADE")
        .execute(pool)
        .await?;
    sqlx::query(r"DROP SCHEMA IF EXISTS introspect CASCADE")
        .execute(pool)
        .await?;
    sqlx::query(&format!(r#"DROP TABLE IF EXISTS "{TABLE_NAME}""#))
        .execute(pool)
        .await?;
    Ok(())
}

async fn verify_run(pool: &PgPool, config: &Config) -> Result<Verification> {
    let dojo_table_rows: i64 = sqlx::query_scalar(
        r"
        SELECT COUNT(*)::BIGINT
        FROM dojo.tables
        WHERE name = $1
        ",
    )
    .bind(TABLE_NAME)
    .fetch_one(pool)
    .await?;
    ensure!(
        dojo_table_rows == 1,
        "expected exactly one Dojo table row for {TABLE_NAME}, got {dojo_table_rows}"
    );

    let dojo_column_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM dojo.columns
        WHERE "table" = $1
        "#,
    )
    .bind(
        compute_selector_from_namespace_and_name(NAMESPACE, MODEL_NAME)
            .to_bytes_be()
            .to_vec(),
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        dojo_column_rows == 2,
        "expected 2 Dojo columns for {TABLE_NAME}, got {dojo_column_rows}"
    );

    let introspect_migrations_exist: bool = sqlx::query_scalar(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'introspect' AND table_name = '_sqlx_migrations'
        )
        ",
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        introspect_migrations_exist,
        "expected introspect._sqlx_migrations to exist"
    );

    let synthetic_table_exists: bool = sqlx::query_scalar(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        ",
    )
    .bind(TABLE_SCHEMA)
    .bind(TABLE_NAME)
    .fetch_one(pool)
    .await?;
    ensure!(
        synthetic_table_exists,
        "expected {TABLE_SCHEMA}.{TABLE_NAME} to exist"
    );

    let total_rows: i64 = sqlx::query_scalar(&format!(
        r#"SELECT COUNT(*)::BIGINT FROM "{TABLE_SCHEMA}"."{TABLE_NAME}""#
    ))
    .fetch_one(pool)
    .await?;
    let expected_rows = (config.block_count as usize * config.records_per_block) as i64;
    ensure!(
        total_rows == expected_rows,
        "expected {expected_rows} rows in {TABLE_SCHEMA}.{TABLE_NAME}, got {total_rows}"
    );

    let last_block = config.from_block + config.block_count - 1;
    let last_record = config.records_per_block - 1;
    let verified_entity_id = Felt::from(
        last_block
            .saturating_mul(1_000_000)
            .saturating_add(last_record as u64)
            .saturating_add(config.seed),
    );
    let expected_score = 100_i64
        + (((config.block_count - 1) as usize * config.records_per_block + last_record) % 10_000)
            as i64
        + 1;

    let row = sqlx::query(&format!(
        r#"
        SELECT encode("entity_id", 'hex') AS entity_id_hex, "score"::bigint AS score
        FROM "{TABLE_SCHEMA}"."{TABLE_NAME}"
        WHERE "entity_id" = $1
        "#
    ))
    .bind(verified_entity_id.to_be_bytes().to_vec())
    .fetch_one(pool)
    .await?;

    let verified_entity_id_hex: String = row.try_get("entity_id_hex")?;
    let verified_score: i64 = row.try_get("score")?;
    ensure!(
        verified_score == expected_score,
        "expected entity {verified_entity_id:#x} to have score {expected_score}, got {verified_score}"
    );

    Ok(Verification {
        total_rows: total_rows as u64,
        verified_entity_id_hex,
        verified_score,
    })
}

fn encode_legacy_schema() -> Vec<Felt> {
    let mut schema = vec![Felt::from_short_ascii_str_unchecked(MODEL_NAME)];
    schema.extend(encode_attributes(&[]));
    schema.extend(encode_columns());
    schema
}

fn encode_columns() -> Vec<Felt> {
    let mut columns = vec![Felt::from(2_u8)];
    columns.extend(encode_column(
        "owner",
        &["key"],
        primitive::CONTRACT_ADDRESS_FELT.into(),
    ));
    columns.extend(encode_column("score", &[], primitive::U32_FELT.into()));
    columns
}

fn encode_column(name: &str, attributes: &[&str], primitive_type: Felt) -> Vec<Felt> {
    let mut column = vec![Felt::from_short_ascii_str_unchecked(name)];
    column.extend(encode_attributes(attributes));
    column.extend(vec![Felt::ZERO, primitive_type]);
    column
}

fn encode_attributes(attributes: &[&str]) -> Vec<Felt> {
    let mut encoded = vec![Felt::from(attributes.len() as u32)];
    encoded.extend(
        attributes
            .iter()
            .map(|attribute| Felt::from_short_ascii_str_unchecked(attribute)),
    );
    encoded
}

fn encode_array<const N: usize>(values: [Felt; N]) -> Vec<Felt> {
    let mut encoded = vec![Felt::from(N as u32)];
    encoded.extend(values);
    encoded
}

fn block_hash_for(block_number: u64) -> Felt {
    Felt::from(0x0200_0000_u64 + block_number)
}

fn tx_hash_for(block_number: u64, tx_index: usize) -> Felt {
    Felt::from(
        block_number
            .saturating_mul(1_000_000)
            .saturating_add(tx_index as u64),
    )
}

fn record_tx_index(record_index: usize, is_update: bool) -> usize {
    let base = record_index * 2 + 1;
    if is_update {
        base + 1
    } else {
        base
    }
}
