//! Simplified Engine database for demo.
//!
//! Tracks basic state and statistics for the Torii engine.
//! This will be enhanced with actual Torii features in the future.

use anyhow::{Context, Result};
use sqlx::any::AnyPoolOptions;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Any, ConnectOptions, Pool, Row};
use starknet_types_raw::Felt;
use std::collections::HashMap;
use std::str::FromStr;

use crate::etl::decoder::DecoderId;

/// Embedded SQL schemas
const SQLITE_SCHEMA_SQL: &str = include_str!("../../sql/engine_schema.sql");
const POSTGRES_SCHEMA_SQL: &str = include_str!("../../sql/engine_schema_postgres.sql");

/// Engine database configuration
#[derive(Debug, Clone)]
pub struct EngineDbConfig {
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbBackend {
    Sqlite,
    Postgres,
}

/// Engine database for tracking state
pub struct EngineDb {
    pool: Pool<Any>,
    backend: DbBackend,
}

impl EngineDb {
    /// Create a new engine database
    pub async fn new(config: EngineDbConfig) -> Result<Self> {
        sqlx::any::install_default_drivers();

        let backend =
            if config.path.starts_with("postgres://") || config.path.starts_with("postgresql://") {
                DbBackend::Postgres
            } else {
                DbBackend::Sqlite
            };

        let (database_url, sqlite_parent_dir) = match backend {
            DbBackend::Postgres => (config.path.clone(), None),
            DbBackend::Sqlite => {
                if is_sqlite_memory_path(&config.path) {
                    ("sqlite::memory:".to_string(), None)
                } else {
                    let sqlite_options = sqlite_connect_options(&config.path)?;
                    let parent_dir = sqlite_options
                        .get_filename()
                        .parent()
                        .filter(|parent| !parent.as_os_str().is_empty())
                        .map(std::path::Path::to_path_buf);

                    (sqlite_options.to_url_lossy().to_string(), parent_dir)
                }
            }
        };

        if let Some(parent) = sqlite_parent_dir {
            tokio::fs::create_dir_all(&parent)
                .await
                .context(format!("Failed to create directory: {}", parent.display()))?;
        }

        tracing::debug!(
            target: "torii::etl::engine_db",
            "Connecting to database: {}",
            database_url
        );

        let max_connections = if backend == DbBackend::Sqlite && database_url == "sqlite::memory:" {
            1
        } else {
            5
        };

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .connect(&database_url)
            .await
            .context("Failed to connect to engine database")?;

        let db = Self { pool, backend };

        // Initialize schema
        db.init_schema().await?;

        Ok(db)
    }

    fn sql<'a>(&self, sqlite: &'a str, postgres: &'a str) -> &'a str {
        match self.backend {
            DbBackend::Sqlite => sqlite,
            DbBackend::Postgres => postgres,
        }
    }

    fn table<'a>(&self, sqlite: &'a str, postgres: &'a str) -> &'a str {
        self.sql(sqlite, postgres)
    }

    /// Initialize database with backend tuning and schema
    async fn init_schema(&self) -> Result<()> {
        // Apply SQLite-only tuning.
        self.apply_pragmas().await?;

        // Load tables from SQL file
        self.load_schema_from_sql().await?;

        tracing::info!(target: "torii::etl::engine_db", "Engine database schema initialized");

        Ok(())
    }

    /// Apply SQLite PRAGMAs for performance
    async fn apply_pragmas(&self) -> Result<()> {
        if self.backend != DbBackend::Sqlite {
            return Ok(());
        }

        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA synchronous=NORMAL")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&self.pool)
            .await?;

        tracing::debug!(target: "torii::etl::engine_db", "Applied SQLite PRAGMAs");

        Ok(())
    }

    /// Load schema from SQL file
    async fn load_schema_from_sql(&self) -> Result<()> {
        let schema_sql = match self.backend {
            DbBackend::Sqlite => SQLITE_SCHEMA_SQL,
            DbBackend::Postgres => POSTGRES_SCHEMA_SQL,
        };

        for statement in schema_sql.split(';') {
            let statement = statement.trim();

            // Skip empty statements
            if statement.is_empty() {
                continue;
            }

            // Remove comment lines
            let sql_lines: Vec<&str> = statement
                .lines()
                .filter(|line| {
                    let trimmed = line.trim();
                    !trimmed.is_empty() && !trimmed.starts_with("--")
                })
                .collect();

            if sql_lines.is_empty() {
                continue;
            }

            let clean_sql = sql_lines.join("\n");

            tracing::debug!(
                target: "torii::etl::engine_db",
                "Executing SQL: {}",
                clean_sql.lines().next().unwrap_or("")
            );

            sqlx::query(&clean_sql)
                .execute(&self.pool)
                .await
                .context(format!(
                    "Failed to execute SQL: {}",
                    clean_sql.lines().next().unwrap_or("")
                ))?;
        }

        tracing::debug!(target: "torii::etl::engine_db", "Schema loaded successfully");
        Ok(())
    }

    /// Get the current head (block number and event count)
    pub async fn get_head(&self) -> Result<(u64, u64)> {
        let table = self.table("head", "engine.head");
        let row = sqlx::query(&format!(
            "SELECT block_number, event_count FROM {table} WHERE id = 'main'"
        ))
        .fetch_one(&self.pool)
        .await?;

        let block_number: i64 = row.get(0);
        let event_count: i64 = row.get(1);

        Ok((block_number as u64, event_count as u64))
    }

    /// Update the head (increment block and event count)
    pub async fn update_head(&self, block_number: u64, events_processed: u64) -> Result<()> {
        let table = self.table("head", "engine.head");
        let sql = match self.backend {
            DbBackend::Sqlite => {
                format!("UPDATE {table} SET block_number = ?, event_count = event_count + ? WHERE id = 'main'")
            }
            DbBackend::Postgres => {
                format!("UPDATE {table} SET block_number = $1, event_count = event_count + $2 WHERE id = 'main'")
            }
        };

        sqlx::query(&sql)
            .bind(block_number as i64)
            .bind(events_processed as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get a stat value
    pub async fn get_stat(&self, key: &str) -> Result<Option<String>> {
        let table = self.table("stats", "engine.stats");
        let sql = match self.backend {
            DbBackend::Sqlite => format!("SELECT value FROM {table} WHERE key = ?"),
            DbBackend::Postgres => format!("SELECT value FROM {table} WHERE key = $1"),
        };

        let row = sqlx::query(&sql)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.get(0)))
    }

    /// Set a stat value
    pub async fn set_stat(&self, key: &str, value: &str) -> Result<()> {
        let table = self.table("stats", "engine.stats");
        let sql = match self.backend {
            DbBackend::Sqlite => {
                format!("INSERT OR REPLACE INTO {table} (key, value) VALUES (?, ?)")
            }
            DbBackend::Postgres => {
                format!(
                    "INSERT INTO {table} (key, value) VALUES ($1, $2) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value"
                )
            }
        };

        sqlx::query(&sql)
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get engine statistics as a JSON-friendly struct
    pub async fn get_stats(&self) -> Result<EngineStats> {
        let (block_number, event_count) = self.get_head().await?;
        let start_time = self.get_stat("start_time").await?.unwrap_or_default();

        Ok(EngineStats {
            current_block: block_number,
            total_events: event_count,
            start_time,
        })
    }

    /// Get extractor state value
    ///
    /// # Arguments
    /// * `extractor_type` - Type of extractor (e.g., "block_range", "contract_events")
    /// * `state_key` - State key (e.g., "last_block", "contract:0x123...")
    ///
    /// # Returns
    /// The state value if it exists, None otherwise
    pub async fn get_extractor_state(
        &self,
        extractor_type: &str,
        state_key: &str,
    ) -> Result<Option<String>> {
        let table = self.table("extractor_state", "engine.extractor_state");
        let sql = match self.backend {
            DbBackend::Sqlite => {
                format!(
                    "SELECT state_value FROM {table} WHERE extractor_type = ? AND state_key = ?"
                )
            }
            DbBackend::Postgres => {
                format!(
                    "SELECT state_value FROM {table} WHERE extractor_type = $1 AND state_key = $2"
                )
            }
        };

        let row = sqlx::query(&sql)
            .bind(extractor_type)
            .bind(state_key)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.get(0)))
    }

    /// Set extractor state value
    ///
    /// # Arguments
    /// * `extractor_type` - Type of extractor (e.g., "block_range", "contract_events")
    /// * `state_key` - State key (e.g., "last_block", "contract:0x123...")
    /// * `state_value` - State value to store
    pub async fn set_extractor_state(
        &self,
        extractor_type: &str,
        state_key: &str,
        state_value: &str,
    ) -> Result<()> {
        let table = self.table("extractor_state", "engine.extractor_state");

        let sql = match self.backend {
            DbBackend::Sqlite => format!(
                "INSERT INTO {table} (extractor_type, state_key, state_value, updated_at) \
                 VALUES (?, ?, ?, strftime('%s', 'now')) \
                 ON CONFLICT(extractor_type, state_key) \
                 DO UPDATE SET state_value = excluded.state_value, updated_at = strftime('%s', 'now')"
            ),
            DbBackend::Postgres => format!(
                "INSERT INTO {table} (extractor_type, state_key, state_value, updated_at) \
                 VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT) \
                 ON CONFLICT(extractor_type, state_key) \
                 DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT"
            ),
        };

        sqlx::query(&sql)
            .bind(extractor_type)
            .bind(state_key)
            .bind(state_value)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// List all extractor state rows for an extractor type.
    pub async fn get_all_extractor_states(
        &self,
        extractor_type: &str,
    ) -> Result<Vec<(String, String)>> {
        let table = self.table("extractor_state", "engine.extractor_state");
        let sql = match self.backend {
            DbBackend::Sqlite => {
                format!("SELECT state_key, state_value FROM {table} WHERE extractor_type = ?")
            }
            DbBackend::Postgres => {
                format!("SELECT state_key, state_value FROM {table} WHERE extractor_type = $1")
            }
        };

        let rows = sqlx::query(&sql)
            .bind(extractor_type)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
    }

    /// Delete extractor state
    ///
    /// # Arguments
    /// * `extractor_type` - Type of extractor
    /// * `state_key` - State key to delete
    pub async fn delete_extractor_state(
        &self,
        extractor_type: &str,
        state_key: &str,
    ) -> Result<()> {
        let table = self.table("extractor_state", "engine.extractor_state");
        let sql = match self.backend {
            DbBackend::Sqlite => {
                format!("DELETE FROM {table} WHERE extractor_type = ? AND state_key = ?")
            }
            DbBackend::Postgres => {
                format!("DELETE FROM {table} WHERE extractor_type = $1 AND state_key = $2")
            }
        };

        sqlx::query(&sql)
            .bind(extractor_type)
            .bind(state_key)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get block timestamps from cache
    ///
    /// # Arguments
    /// * `block_numbers` - Block numbers to look up
    ///
    /// # Returns
    /// HashMap of block_number -> timestamp for blocks found in cache
    pub async fn get_block_timestamps(
        &self,
        block_numbers: &[u64],
    ) -> Result<std::collections::HashMap<u64, u64>> {
        if block_numbers.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let table = self.table("block_timestamps", "engine.block_timestamps");
        let placeholders = match self.backend {
            DbBackend::Sqlite => vec!["?"; block_numbers.len()].join(", "),
            DbBackend::Postgres => (1..=block_numbers.len())
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(", "),
        };
        let sql = format!(
            "SELECT block_number, timestamp FROM {table} WHERE block_number IN ({placeholders})"
        );

        let mut query = sqlx::query(&sql);
        for block_num in block_numbers {
            query = query.bind(*block_num as i64);
        }

        let rows = query.fetch_all(&self.pool).await?;

        let mut result = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            let block_number: i64 = row.get(0);
            let timestamp: i64 = row.get(1);
            result.insert(block_number as u64, timestamp as u64);
        }

        Ok(result)
    }

    /// Insert block timestamps into cache
    ///
    /// # Arguments
    /// * `timestamps` - HashMap of block_number -> timestamp
    pub async fn insert_block_timestamps(
        &self,
        timestamps: &std::collections::HashMap<u64, u64>,
    ) -> Result<()> {
        if timestamps.is_empty() {
            return Ok(());
        }

        const CHUNK_SIZE: usize = 400;

        let mut tx = self.pool.begin().await?;

        let table = self.table("block_timestamps", "engine.block_timestamps");

        // Use chunked multi-row insert to reduce round-trips.
        let timestamp_items: Vec<(u64, u64)> = timestamps.iter().map(|(k, v)| (*k, *v)).collect();
        for chunk in timestamp_items.chunks(CHUNK_SIZE) {
            let mut sql = match self.backend {
                DbBackend::Sqlite => {
                    format!("INSERT OR IGNORE INTO {table} (block_number, timestamp) VALUES ")
                }
                DbBackend::Postgres => {
                    format!("INSERT INTO {table} (block_number, timestamp) VALUES ")
                }
            };

            let mut values = Vec::with_capacity(chunk.len());
            match self.backend {
                DbBackend::Sqlite => {
                    for _ in chunk {
                        values.push("(?, ?)".to_string());
                    }
                }
                DbBackend::Postgres => {
                    for (index, _) in chunk.iter().enumerate() {
                        let param = index * 2;
                        values.push(format!("(${}, ${})", param + 1, param + 2));
                    }
                }
            }
            sql.push_str(&values.join(", "));

            if self.backend == DbBackend::Postgres {
                sql.push_str(" ON CONFLICT (block_number) DO NOTHING");
            }

            let mut query = sqlx::query(&sql);
            for (block_number, timestamp) in chunk {
                query = query.bind(*block_number as i64).bind(*timestamp as i64);
            }

            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;

        Ok(())
    }

    /// Get a single block timestamp from cache
    ///
    /// # Arguments
    /// * `block_number` - Block number to look up
    ///
    /// # Returns
    /// Timestamp if found in cache, None otherwise
    pub async fn get_block_timestamp(&self, block_number: u64) -> Result<Option<u64>> {
        let table = self.table("block_timestamps", "engine.block_timestamps");
        let sql = match self.backend {
            DbBackend::Sqlite => format!("SELECT timestamp FROM {table} WHERE block_number = ?"),
            DbBackend::Postgres => {
                format!("SELECT timestamp FROM {table} WHERE block_number = $1")
            }
        };

        let timestamp: Option<i64> = sqlx::query_scalar(&sql)
            .bind(block_number as i64)
            .fetch_optional(&self.pool)
            .await?;

        Ok(timestamp.map(|ts| ts as u64))
    }

    // ===== Contract Decoder Persistence =====

    /// Get all contract decoder mappings from database.
    ///
    /// # Returns
    /// Vector of (contract_address, decoder_ids, identified_at_timestamp)
    pub async fn get_all_contract_decoders(&self) -> Result<Vec<(Felt, Vec<DecoderId>, i64)>> {
        let table = self.table("contract_decoders", "engine.contract_decoders");
        let rows = sqlx::query(&format!(
            "SELECT contract_address, decoder_ids, identified_at FROM {table}"
        ))
        .fetch_all(&self.pool)
        .await?;

        let mut results = Vec::new();
        for row in rows {
            let addr_hex: String = row.get(0);
            let decoder_ids_str: String = row.get(1);
            let identified_at: i64 = row.get(2);

            // Parse contract address
            let contract_address = Felt::from_hex(&addr_hex)
                .context(format!("Invalid contract address: {addr_hex}"))?;

            // Parse decoder IDs (comma-separated u64 values)
            let decoder_ids: Vec<DecoderId> = if decoder_ids_str.is_empty() {
                Vec::new()
            } else {
                decoder_ids_str
                    .split(',')
                    .filter_map(|s| s.trim().parse::<u64>().ok())
                    .map(DecoderId::from_u64)
                    .collect()
            };

            results.push((contract_address, decoder_ids, identified_at));
        }

        Ok(results)
    }

    /// Set decoder IDs for a contract.
    ///
    /// # Arguments
    /// * `contract` - Contract address
    /// * `decoder_ids` - List of decoder IDs (can be empty)
    pub async fn set_contract_decoders(
        &self,
        contract: Felt,
        decoder_ids: &[DecoderId],
    ) -> Result<()> {
        let addr_hex = format!("{contract:#x}");
        let decoder_ids_str: String = decoder_ids
            .iter()
            .map(|id| id.as_u64().to_string())
            .collect::<Vec<_>>()
            .join(",");

        let table = self.table("contract_decoders", "engine.contract_decoders");

        let sql = match self.backend {
            DbBackend::Sqlite => format!(
                "INSERT INTO {table} (contract_address, decoder_ids, identified_at) \
                 VALUES (?, ?, strftime('%s', 'now')) \
                 ON CONFLICT(contract_address) \
                 DO UPDATE SET decoder_ids = excluded.decoder_ids, identified_at = strftime('%s', 'now')"
            ),
            DbBackend::Postgres => format!(
                "INSERT INTO {table} (contract_address, decoder_ids, identified_at) \
                 VALUES ($1, $2, EXTRACT(EPOCH FROM NOW())::BIGINT) \
                 ON CONFLICT(contract_address) \
                 DO UPDATE SET decoder_ids = EXCLUDED.decoder_ids, identified_at = EXTRACT(EPOCH FROM NOW())::BIGINT"
            ),
        };

        sqlx::query(&sql)
            .bind(&addr_hex)
            .bind(&decoder_ids_str)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Set decoder IDs for multiple contracts in a single batch operation.
    ///
    /// # Arguments
    /// * `contracts` - HashMap of contract address to decoder IDs
    ///
    /// # Performance
    /// Uses a single INSERT with unnest() for PostgreSQL or a transaction with
    /// multiple INSERTs for SQLite, reducing N database round trips to 1.
    pub async fn set_contract_decoders_batch(
        &self,
        contracts: &HashMap<Felt, Vec<DecoderId>>,
    ) -> Result<()> {
        if contracts.is_empty() {
            return Ok(());
        }

        let table = self.table("contract_decoders", "engine.contract_decoders");
        let mut tx = self.pool.begin().await?;

        for (contract, decoder_ids) in contracts {
            let addr_hex = format!("{contract:#x}");
            let decoder_ids_str: String = decoder_ids
                .iter()
                .map(|id| id.as_u64().to_string())
                .collect::<Vec<_>>()
                .join(",");

            let sql = match self.backend {
                DbBackend::Sqlite => format!(
                    "INSERT INTO {table} (contract_address, decoder_ids, identified_at) \
                     VALUES (?, ?, strftime('%s', 'now')) \
                     ON CONFLICT(contract_address) \
                     DO UPDATE SET decoder_ids = excluded.decoder_ids, identified_at = strftime('%s', 'now')"
                ),
                DbBackend::Postgres => format!(
                    "INSERT INTO {table} (contract_address, decoder_ids, identified_at) \
                     VALUES ($1, $2, EXTRACT(EPOCH FROM NOW())::BIGINT) \
                     ON CONFLICT(contract_address) \
                     DO UPDATE SET decoder_ids = EXCLUDED.decoder_ids, identified_at = EXTRACT(EPOCH FROM NOW())::BIGINT"
                ),
            };

            sqlx::query(&sql)
                .bind(&addr_hex)
                .bind(&decoder_ids_str)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Get decoder IDs for a specific contract.
    ///
    /// # Arguments
    /// * `contract` - Contract address
    ///
    /// # Returns
    /// Some(decoder_ids) if found, None otherwise
    pub async fn get_contract_decoders(&self, contract: Felt) -> Result<Option<Vec<DecoderId>>> {
        let addr_hex = format!("{contract:#x}");
        let table = self.table("contract_decoders", "engine.contract_decoders");
        let sql = match self.backend {
            DbBackend::Sqlite => {
                format!("SELECT decoder_ids FROM {table} WHERE contract_address = ?")
            }
            DbBackend::Postgres => {
                format!("SELECT decoder_ids FROM {table} WHERE contract_address = $1")
            }
        };

        let row = sqlx::query(&sql)
            .bind(&addr_hex)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(r) => {
                let decoder_ids_str: String = r.get(0);
                let decoder_ids: Vec<DecoderId> = if decoder_ids_str.is_empty() {
                    Vec::new()
                } else {
                    decoder_ids_str
                        .split(',')
                        .filter_map(|s| s.trim().parse::<u64>().ok())
                        .map(DecoderId::from_u64)
                        .collect()
                };
                Ok(Some(decoder_ids))
            }
            None => Ok(None),
        }
    }
}

fn is_sqlite_memory_path(path: &str) -> bool {
    path == ":memory:"
        || path == "sqlite::memory:"
        || path == "sqlite://:memory:"
        || path.contains("mode=memory")
}

fn sqlite_connect_options(path: &str) -> Result<SqliteConnectOptions> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return SqliteConnectOptions::from_str("sqlite::memory:")
            .context("Failed to parse in-memory sqlite URL");
    }

    let options = if path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(path)
            .with_context(|| format!("Failed to parse sqlite URL: {path}"))?
    } else {
        SqliteConnectOptions::new().filename(path)
    };

    if path.starts_with("sqlite:") && path.contains("mode=") {
        Ok(options)
    } else {
        Ok(options.create_if_missing(true))
    }
}

/// Engine statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct EngineStats {
    pub current_block: u64,
    pub total_events: u64,
    pub start_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_engine_db_initialization() {
        let config = EngineDbConfig {
            path: ":memory:".to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();
        let (block, events) = db.get_head().await.unwrap();

        assert_eq!(block, 0);
        assert_eq!(events, 0);
    }

    #[tokio::test]
    async fn test_update_head() {
        let config = EngineDbConfig {
            path: ":memory:".to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();

        // Update head
        db.update_head(100, 50).await.unwrap();

        let (block, events) = db.get_head().await.unwrap();
        assert_eq!(block, 100);
        assert_eq!(events, 50);

        // Update again (events should accumulate)
        db.update_head(200, 30).await.unwrap();

        let (block, events) = db.get_head().await.unwrap();
        assert_eq!(block, 200);
        assert_eq!(events, 80); // 50 + 30
    }

    #[tokio::test]
    async fn test_stats() {
        let config = EngineDbConfig {
            path: ":memory:".to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();

        // Set custom stat
        db.set_stat("last_sync", "2026-01-08").await.unwrap();

        // Get stat
        let value = db.get_stat("last_sync").await.unwrap();
        assert_eq!(value, Some("2026-01-08".to_string()));

        // Get missing stat
        let missing = db.get_stat("nonexistent").await.unwrap();
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn test_get_all_extractor_states() {
        let config = EngineDbConfig {
            path: ":memory:".to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();
        db.set_extractor_state("event", "0x1", "block:10")
            .await
            .unwrap();
        db.set_extractor_state("event", "0x2", "block:20")
            .await
            .unwrap();
        db.set_extractor_state("other", "cursor", "block:30")
            .await
            .unwrap();

        let mut rows = db.get_all_extractor_states("event").await.unwrap();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("0x1".to_string(), "block:10".to_string()),
                ("0x2".to_string(), "block:20".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn test_engine_db_creates_file_backed_sqlite_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("nested").join("engine.db");
        let config = EngineDbConfig {
            path: db_path.to_string_lossy().to_string(),
        };

        let db = EngineDb::new(config).await.unwrap();
        let (block, events) = db.get_head().await.unwrap();

        assert_eq!(block, 0);
        assert_eq!(events, 0);
        assert!(db_path.exists());
    }
}
