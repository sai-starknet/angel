use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sqlx::any::AnyPoolOptions;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Any, ConnectOptions, Pool, QueryBuilder, Row};
use starknet_types_raw::Felt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use torii::axum::Router;
use torii::etl::extractor::ExtractionBatch;
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii::etl::TypeId;
use torii_runtime_common::database::DEFAULT_SQLITE_MAX_CONNECTIONS;
use torii_sql::DbBackend;

pub const DEFAULT_API_QUERY_URL: &str = "https://api.cartridge.gg/query";
pub const CONTROLLERS_TABLE: &str = "controllers";
pub const CONTROLLERS_STATE_TABLE: &str = "torii_controller_sync_state";

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_secs(2);
const CONTROLLERS_TYPE: TypeId = TypeId::new("controllers.sync");
const CONTROLLER_PROCESSING_BATCH_SIZE: usize = 10_000;
const SQLITE_CONTROLLER_UPSERT_BATCH_SIZE: usize = 199;
const POSTGRES_CONTROLLER_UPSERT_BATCH_SIZE: usize = 10_000;
#[derive(Debug, Clone, Deserialize)]
struct ControllerAccount {
    username: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ControllerNode {
    address: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    account: ControllerAccount,
}

#[derive(Debug, Clone, Deserialize)]
struct ControllerEdge {
    node: ControllerNode,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ControllersConnection {
    edges: Vec<ControllerEdge>,
}

#[derive(Debug, Clone, Deserialize)]
struct ControllersData {
    controllers: ControllersConnection,
}

#[derive(Debug, Clone, Deserialize)]
struct GraphQlError {
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ControllersResponse {
    data: Option<ControllersData>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, Clone)]
struct StoredController {
    id: String,
    address: String,
    username: String,
    deployed_at: String,
    updated_at: i64,
}

impl TryFrom<ControllerNode> for StoredController {
    type Error = anyhow::Error;

    fn try_from(value: ControllerNode) -> Result<Self> {
        let created_at = DateTime::parse_from_rfc3339(&value.created_at)
            .with_context(|| {
                format!(
                    "invalid controller createdAt returned by API: {}",
                    value.created_at
                )
            })?
            .with_timezone(&Utc);
        let felt_addr = Felt::from_str(&value.address).with_context(|| {
            format!(
                "invalid controller address returned by API: {}",
                value.address
            )
        })?;
        let normalized = format!("{felt_addr:#066x}");
        Ok(Self {
            id: normalized.clone(),
            address: normalized,
            username: value.account.username,
            deployed_at: created_at.to_rfc3339(),
            updated_at: Utc::now().timestamp(),
        })
    }
}

struct ControllersStore {
    pool: Pool<Any>,
    backend: DbBackend,
}

impl ControllersStore {
    async fn new(database_url: &str, max_connections: Option<u32>) -> Result<Self> {
        sqlx::any::install_default_drivers();

        let backend = DbBackend::from_str(database_url).map_err(|e| anyhow!(e))?;
        let database_url = match backend {
            DbBackend::Postgres => database_url.to_string(),
            DbBackend::Sqlite => sqlite_url(database_url)?,
        };

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections.unwrap_or(if backend == DbBackend::Sqlite {
                DEFAULT_SQLITE_MAX_CONNECTIONS
            } else {
                5
            }))
            .connect(&database_url)
            .await?;

        Ok(Self { pool, backend })
    }

    async fn initialize(&self) -> Result<()> {
        if self.backend == DbBackend::Sqlite {
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&self.pool)
                .await
                .ok();
            sqlx::query("PRAGMA synchronous=NORMAL")
                .execute(&self.pool)
                .await
                .ok();
            sqlx::query("PRAGMA foreign_keys=ON")
                .execute(&self.pool)
                .await
                .ok();
        }

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {CONTROLLERS_TABLE} (
                id TEXT PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL,
                deployed_at TEXT NOT NULL,
                updated_at BIGINT NOT NULL
            )"
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {CONTROLLERS_STATE_TABLE} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                synced_until BIGINT
            )"
        ))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn load_synced_until(&self) -> Result<Option<i64>> {
        let row = sqlx::query(&format!(
            "SELECT synced_until FROM {CONTROLLERS_STATE_TABLE} WHERE id = 1"
        ))
        .fetch_optional(&self.pool)
        .await?;

        Ok(row
            .and_then(|row| row.try_get::<Option<i64>, _>(0).ok())
            .flatten())
    }

    async fn is_empty(&self) -> Result<bool> {
        let row =
            sqlx::query_scalar::<Any, i64>(&format!("SELECT COUNT(*) FROM {CONTROLLERS_TABLE}"))
                .fetch_one(&self.pool)
                .await?;

        Ok(row == 0)
    }

    async fn upsert_controllers_and_store_synced_until(
        &self,
        controllers: &[StoredController],
        synced_until: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        if !controllers.is_empty() {
            let upsert_prefix = format!(
                "INSERT INTO {CONTROLLERS_TABLE} (id, address, username, deployed_at, updated_at) "
            );
            let upsert_suffix = " ON CONFLICT(address) DO UPDATE SET \
                id = excluded.id, \
                username = excluded.username, \
                deployed_at = excluded.deployed_at, \
                updated_at = excluded.updated_at";

            for chunk in controllers.chunks(self.controller_upsert_batch_size()) {
                let mut builder = QueryBuilder::<Any>::new(&upsert_prefix);
                builder.push_values(chunk, |mut builder, controller| {
                    builder
                        .push_bind(&controller.id)
                        .push_bind(&controller.address)
                        .push_bind(&controller.username)
                        .push_bind(&controller.deployed_at)
                        .push_bind(controller.updated_at);
                });
                builder.push(upsert_suffix);
                builder.build().execute(&mut *tx).await?;
            }
        }

        let progress_sql = match self.backend {
            DbBackend::Sqlite => format!(
                "INSERT INTO {CONTROLLERS_STATE_TABLE} (id, synced_until) VALUES (?1, ?2)
                 ON CONFLICT(id) DO UPDATE SET synced_until = excluded.synced_until"
            ),
            DbBackend::Postgres => format!(
                "INSERT INTO {CONTROLLERS_STATE_TABLE} (id, synced_until) VALUES ($1, $2)
                 ON CONFLICT(id) DO UPDATE SET synced_until = excluded.synced_until"
            ),
        };

        sqlx::query(&progress_sql)
            .bind(1_i32)
            .bind(synced_until)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    fn controller_upsert_batch_size(&self) -> usize {
        match self.backend {
            DbBackend::Sqlite => SQLITE_CONTROLLER_UPSERT_BATCH_SIZE,
            DbBackend::Postgres => POSTGRES_CONTROLLER_UPSERT_BATCH_SIZE,
        }
    }
}

pub struct ControllersSink {
    store: Arc<ControllersStore>,
    api_url: String,
    client: Client,
}

impl ControllersSink {
    pub async fn new(
        database_url: &str,
        max_connections: Option<u32>,
        api_url: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            store: Arc::new(ControllersStore::new(database_url, max_connections).await?),
            api_url: api_url.unwrap_or_else(|| DEFAULT_API_QUERY_URL.to_string()),
            client: Client::new(),
        })
    }

    fn batch_time_window(batch: &ExtractionBatch) -> Option<(i64, i64)> {
        let mut timestamps = batch.blocks.values().map(|block| block.timestamp as i64);
        let first = timestamps.next()?;
        let (min_ts, max_ts) = timestamps.fold((first, first), |(min_ts, max_ts), ts| {
            (min_ts.min(ts), max_ts.max(ts))
        });
        Some((min_ts, max_ts))
    }

    fn build_query(lower_bound: DateTime<Utc>, upper_bound: DateTime<Utc>) -> String {
        format!(
            r#"
query {{
  controllers(where:{{
    createdAtGT:"{}",
    createdAtLTE:"{}"
  }}, orderBy:{{
    field:CREATED_AT,
    direction:ASC
  }}) {{
    edges {{
      node {{
        address
        createdAt
        account {{
          username
        }}
      }}
    }}
  }}
}}"#,
            lower_bound.to_rfc3339(),
            upper_bound.to_rfc3339(),
        )
    }

    async fn fetch_controllers(
        &self,
        lower_bound: DateTime<Utc>,
        upper_bound: DateTime<Utc>,
    ) -> Result<Vec<ControllerNode>> {
        let query = Self::build_query(lower_bound, upper_bound);
        let mut attempts = 0;

        loop {
            attempts += 1;
            let result = self
                .client
                .post(&self.api_url)
                .json(&json!({ "query": query }))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    let body: ControllersResponse = response.json().await?;
                    if let Some(errors) = body.errors {
                        let msg = errors
                            .into_iter()
                            .map(|error| error.message)
                            .collect::<Vec<_>>()
                            .join("; ");
                        return Err(anyhow!("controller GraphQL query failed: {msg}"));
                    }

                    return Ok(body
                        .data
                        .unwrap_or(ControllersData {
                            controllers: ControllersConnection::default(),
                        })
                        .controllers
                        .edges
                        .into_iter()
                        .map(|edge| edge.node)
                        .collect());
                }
                Ok(response) if attempts < MAX_RETRIES => {
                    let error_text = response.text().await.unwrap_or_default();
                    tracing::warn!(
                        target: "torii::sinks::controllers",
                        attempt = attempts,
                        max_retries = MAX_RETRIES,
                        error_text,
                        "Controller fetch failed, retrying"
                    );
                }
                Ok(response) => {
                    let error_text = response.text().await.unwrap_or_default();
                    return Err(anyhow!("controller API returned error: {error_text}"));
                }
                Err(error) if attempts < MAX_RETRIES => {
                    tracing::warn!(
                        target: "torii::sinks::controllers",
                        attempt = attempts,
                        max_retries = MAX_RETRIES,
                        error = %error,
                        "Controller fetch transport error, retrying"
                    );
                }
                Err(error) => return Err(error.into()),
            }

            let backoff = INITIAL_BACKOFF * (1 << (attempts - 1));
            tokio::time::sleep(backoff).await;
        }
    }

    async fn sync_batch(&self, batch: &ExtractionBatch) -> Result<()> {
        let Some((batch_from_ts, batch_to_ts)) = Self::batch_time_window(batch) else {
            return Ok(());
        };

        let synced_until = self.store.load_synced_until().await?;
        if synced_until.is_some_and(|ts| ts >= batch_to_ts) {
            return Ok(());
        }

        let effective_start = synced_until.unwrap_or_else(|| batch_from_ts.saturating_sub(1));
        let lower_bound = utc_timestamp(effective_start)?;
        let upper_bound = utc_timestamp(batch_to_ts)?;
        let controllers = self.fetch_controllers(lower_bound, upper_bound).await?;
        let stored_controllers = controllers
            .into_iter()
            .map(StoredController::try_from)
            .collect::<Result<Vec<_>>>()?;
        let stored = stored_controllers.len() as u64;

        for chunk in stored_controllers.chunks(CONTROLLER_PROCESSING_BATCH_SIZE) {
            self.store
                .upsert_controllers_and_store_synced_until(chunk, batch_to_ts)
                .await?;
        }

        if stored_controllers.is_empty() {
            self.store
                .upsert_controllers_and_store_synced_until(&[], batch_to_ts)
                .await?;
        }

        tracing::info!(
            target: "torii::sinks::controllers",
            stored,
            batch_from_ts,
            batch_to_ts,
            synced_until,
            "Synchronized controllers for batch window"
        );
        ::metrics::counter!("torii_controller_sync_batches_total", "status" => "ok").increment(1);
        ::metrics::counter!("torii_controller_synced_total").increment(stored);

        Ok(())
    }

    async fn full_sync_from_api(&self) -> Result<()> {
        let lower_bound = utc_timestamp(0)?;
        let upper_bound = Utc::now();
        let controllers = self.fetch_controllers(lower_bound, upper_bound).await?;
        let stored_controllers = controllers
            .into_iter()
            .map(StoredController::try_from)
            .collect::<Result<Vec<_>>>()?;
        let stored = stored_controllers.len() as u64;

        for chunk in stored_controllers.chunks(CONTROLLER_PROCESSING_BATCH_SIZE) {
            self.store
                .upsert_controllers_and_store_synced_until(chunk, upper_bound.timestamp())
                .await?;
        }

        if stored_controllers.is_empty() {
            self.store
                .upsert_controllers_and_store_synced_until(&[], upper_bound.timestamp())
                .await?;
        }

        tracing::info!(
            target: "torii::sinks::controllers",
            stored,
            synced_until = upper_bound.timestamp(),
            "Completed initial full controller sync"
        );
        ::metrics::counter!("torii_controller_sync_batches_total", "status" => "ok").increment(1);
        ::metrics::counter!("torii_controller_synced_total").increment(stored);

        Ok(())
    }
}

#[async_trait]
impl Sink for ControllersSink {
    fn name(&self) -> &'static str {
        "controllers"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![CONTROLLERS_TYPE]
    }

    async fn process(
        &self,
        _envelopes: &[torii::etl::Envelope],
        batch: &ExtractionBatch,
    ) -> Result<()> {
        match self.sync_batch(batch).await {
            Ok(()) => Ok(()),
            Err(error) => {
                ::metrics::counter!("torii_controller_sync_batches_total", "status" => "error")
                    .increment(1);
                Err(error)
            }
        }
    }

    fn topics(&self) -> Vec<TopicInfo> {
        Vec::new()
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> Result<()> {
        self.store.initialize().await?;
        if self.store.is_empty().await? {
            self.full_sync_from_api().await?;
        }
        Ok(())
    }
}

fn utc_timestamp(timestamp: i64) -> Result<DateTime<Utc>> {
    Utc.timestamp_opt(timestamp, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid UTC timestamp: {timestamp}"))
}

fn sqlite_url(path: &str) -> Result<String> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return Ok("sqlite::memory:".to_string());
    }
    if path.starts_with("sqlite:") {
        return Ok(path.to_string());
    }
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{path}"))
        .or_else(|_| Ok::<_, sqlx::Error>(SqliteConnectOptions::new().filename(path)))?;
    if let Some(parent) = options
        .get_filename()
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(options.to_url_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use sqlx::query_scalar;
    use tokio::net::TcpListener;
    use torii::axum::extract::State;
    use torii::axum::routing::post;
    use torii::axum::{Json, Router};
    use torii::command::CommandBus;
    use torii::grpc::SubscriptionManager;

    async fn initialize_sink(sink: &mut ControllersSink) {
        let event_bus = Arc::new(EventBus::new(Arc::new(SubscriptionManager::new())));
        let command_bus = CommandBus::new(Vec::new(), 1).unwrap();
        let context = SinkContext {
            database_root: ".".into(),
            command_bus: command_bus.sender(),
        };
        Sink::initialize(sink, event_bus, &context).await.unwrap();
    }

    #[derive(Clone)]
    struct TestGraphQlState {
        body: Value,
    }

    async fn graphql_handler(
        State(state): State<TestGraphQlState>,
        Json(_payload): Json<Value>,
    ) -> Json<Value> {
        Json(state.body)
    }

    async fn spawn_graphql_server(body: Value) -> Result<String> {
        let state = TestGraphQlState { body };
        let app = Router::new()
            .route("/query", post(graphql_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            torii::axum::serve(listener, app)
                .await
                .expect("test server should run");
        });
        Ok(format!("http://{addr}/query"))
    }

    fn make_batch(from_ts: u64, to_ts: u64) -> ExtractionBatch {
        let mut batch = ExtractionBatch::empty();
        batch.add_block_context(1, Felt::ONE, Felt::ZERO, from_ts);
        batch.add_block_context(2, Felt::TWO, Felt::ONE, to_ts);
        batch
    }

    #[tokio::test]
    async fn controllers_sink_persists_rows_and_progress() {
        let api_url = spawn_graphql_server(json!({
            "data": {
                "controllers": {
                    "edges": [
                        {
                            "node": {
                                "address": "0x123",
                                "createdAt": "2024-03-20T12:00:00Z",
                                "account": { "username": "test_user" }
                            }
                        }
                    ]
                }
            }
        }))
        .await
        .unwrap();

        let sink = ControllersSink::new(":memory:", Some(1), Some(api_url))
            .await
            .unwrap();
        sink.store.initialize().await.unwrap();

        sink.process(&[], &make_batch(1_710_936_000, 1_710_936_100))
            .await
            .unwrap();

        let controller_count: i64 =
            query_scalar(&format!("SELECT COUNT(*) FROM {CONTROLLERS_TABLE}"))
                .fetch_one(&sink.store.pool)
                .await
                .unwrap();
        assert_eq!(controller_count, 1);

        let username: String =
            query_scalar(&format!("SELECT username FROM {CONTROLLERS_TABLE} LIMIT 1"))
                .fetch_one(&sink.store.pool)
                .await
                .unwrap();
        assert_eq!(username, "test_user");

        let synced_until: i64 = query_scalar(&format!(
            "SELECT synced_until FROM {CONTROLLERS_STATE_TABLE} WHERE id = 1"
        ))
        .fetch_one(&sink.store.pool)
        .await
        .unwrap();
        assert_eq!(synced_until, 1_710_936_100);
    }

    #[tokio::test]
    async fn controllers_sink_upserts_existing_address() {
        let api_url = spawn_graphql_server(json!({
            "data": {
                "controllers": {
                    "edges": [
                        {
                            "node": {
                                "address": "0x123",
                                "createdAt": "2024-03-20T12:00:00Z",
                                "account": { "username": "user_one" }
                            }
                        }
                    ]
                }
            }
        }))
        .await
        .unwrap();

        let sink = ControllersSink::new(":memory:", Some(1), Some(api_url))
            .await
            .unwrap();
        sink.store.initialize().await.unwrap();

        let controller = StoredController::try_from(ControllerNode {
            address: "0x123".to_string(),
            created_at: "2024-03-19T12:00:00Z".to_string(),
            account: ControllerAccount {
                username: "old_name".to_string(),
            },
        })
        .unwrap();
        sink.store
            .upsert_controllers_and_store_synced_until(&[controller], 0)
            .await
            .unwrap();

        sink.process(&[], &make_batch(1_710_936_000, 1_710_936_100))
            .await
            .unwrap();

        let controller_count: i64 =
            query_scalar(&format!("SELECT COUNT(*) FROM {CONTROLLERS_TABLE}"))
                .fetch_one(&sink.store.pool)
                .await
                .unwrap();
        assert_eq!(controller_count, 1);

        let username: String =
            query_scalar(&format!("SELECT username FROM {CONTROLLERS_TABLE} LIMIT 1"))
                .fetch_one(&sink.store.pool)
                .await
                .unwrap();
        assert_eq!(username, "user_one");
    }

    #[tokio::test]
    async fn initialize_runs_full_sync_when_table_is_empty() {
        let api_url = spawn_graphql_server(json!({
            "data": {
                "controllers": {
                    "edges": [
                        {
                            "node": {
                                "address": "0x456",
                                "createdAt": "2024-03-20T12:00:00Z",
                                "account": { "username": "boot_user" }
                            }
                        }
                    ]
                }
            }
        }))
        .await
        .unwrap();

        let mut sink = ControllersSink::new(":memory:", Some(1), Some(api_url))
            .await
            .unwrap();
        initialize_sink(&mut sink).await;

        let controller_count: i64 =
            query_scalar(&format!("SELECT COUNT(*) FROM {CONTROLLERS_TABLE}"))
                .fetch_one(&sink.store.pool)
                .await
                .unwrap();
        assert_eq!(controller_count, 1);

        let username: String =
            query_scalar(&format!("SELECT username FROM {CONTROLLERS_TABLE} LIMIT 1"))
                .fetch_one(&sink.store.pool)
                .await
                .unwrap();
        assert_eq!(username, "boot_user");

        let synced_until: i64 = query_scalar(&format!(
            "SELECT synced_until FROM {CONTROLLERS_STATE_TABLE} WHERE id = 1"
        ))
        .fetch_one(&sink.store.pool)
        .await
        .unwrap();
        assert!(synced_until > 0);
    }

    #[tokio::test]
    async fn store_batches_large_controller_upserts() {
        let sink = ControllersSink::new(":memory:", Some(1), None)
            .await
            .unwrap();
        sink.store.initialize().await.unwrap();

        let controllers = (0..250)
            .map(|i| {
                StoredController::try_from(ControllerNode {
                    address: format!("{:#x}", i + 1),
                    created_at: "2024-03-20T12:00:00Z".to_string(),
                    account: ControllerAccount {
                        username: format!("user_{i}"),
                    },
                })
                .unwrap()
            })
            .collect::<Vec<_>>();

        sink.store
            .upsert_controllers_and_store_synced_until(&controllers, 123)
            .await
            .unwrap();

        let count: i64 = query_scalar(&format!("SELECT COUNT(*) FROM {CONTROLLERS_TABLE}"))
            .fetch_one(&sink.store.pool)
            .await
            .unwrap();
        assert_eq!(count, 250);

        let synced_until: i64 = query_scalar(&format!(
            "SELECT synced_until FROM {CONTROLLERS_STATE_TABLE} WHERE id = 1"
        ))
        .fetch_one(&sink.store.pool)
        .await
        .unwrap();
        assert_eq!(synced_until, 123);
    }
}
