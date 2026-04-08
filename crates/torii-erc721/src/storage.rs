//! SQLite storage for ERC721 NFT transfers, approvals, and ownership
//!
//! Uses binary (BLOB) storage for efficiency.
//! Tracks current NFT ownership state (who owns each token).

use crate::conversions::{blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob};
use anyhow::Result;
use rusqlite::{params, params_from_iter, Connection, ToSql};
use starknet::core::types::{Felt, U256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio_postgres::{types::ToSql as PgToSql, Client, NoTls};
use torii_common::{TokenUriResult, TokenUriStore};
use torii_sql::{DbPool, DbPoolOptions};

const SQLITE_MAX_BIND_VARS: usize = 900;
const SQLITE_TOKEN_BATCH_SIZE: usize = SQLITE_MAX_BIND_VARS;
const SQLITE_TOKEN_PAIR_BATCH_SIZE: usize = SQLITE_MAX_BIND_VARS / 2;

/// Storage for ERC721 NFT data
pub struct Erc721Storage {
    db: Erc721Db<DbPool>,
}

struct Erc721Db<Backend> {
    backend: Backend,
    runtime: StorageRuntime,
}

enum StorageRuntime {
    Sqlite(SqliteStorageRuntime),
    Postgres(PostgresStorageRuntime),
}

struct SqliteStorageRuntime {
    conn: Arc<Mutex<Connection>>,
}

struct PostgresStorageRuntime {
    conn: Arc<tokio::sync::Mutex<Client>>,
}

impl<Backend> Erc721Db<Backend> {
    fn new(backend: Backend, runtime: StorageRuntime) -> Self {
        Self { backend, runtime }
    }
}

impl StorageRuntime {
    fn backend(&self) -> StorageBackend {
        match self {
            Self::Sqlite(_) => StorageBackend::Sqlite,
            Self::Postgres(_) => StorageBackend::Postgres,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageBackend {
    Sqlite,
    Postgres,
}

/// NFT transfer data for batch insertion
pub struct NftTransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub from: Felt,
    pub to: Felt,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// NFT ownership data
pub struct NftOwnershipData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub owner: Felt,
    pub block_number: u64,
}

/// NFT approval data
pub struct NftApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub owner: Felt,
    pub approved: Felt,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Operator approval data
pub struct OperatorApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub owner: Felt,
    pub operator: Felt,
    pub approved: bool,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Cursor for paginated transfer queries
#[derive(Debug, Clone, Copy)]
pub struct TransferCursor {
    pub block_number: u64,
    pub id: i64,
}

/// Cursor for paginated ownership queries
#[derive(Debug, Clone, Copy)]
pub struct OwnershipCursor {
    pub block_number: u64,
    pub id: i64,
}

/// Aggregated facet count for one key/value pair.
pub struct AttributeFacetCount {
    pub key: String,
    pub value: String,
    pub count: u64,
}

/// Result of token ID search by attributes.
pub struct TokenAttributeQueryResult {
    pub token_ids: Vec<U256>,
    pub next_cursor_token_id: Option<U256>,
    pub total_hits: u64,
    pub facets: Vec<AttributeFacetCount>,
}

#[derive(Debug, Clone)]
struct ResolvedFacetFilter {
    key_id: i64,
    value_ids: Vec<i64>,
}

impl Erc721Storage {
    /// Create or open the database
    pub async fn new(db_path: &str) -> Result<Self> {
        let pool = Self::connect_pool(db_path).await?;
        Self::from_pool(db_path, pool).await
    }

    pub async fn connect_pool(db_path: &str) -> Result<DbPool> {
        Ok(DbPoolOptions::new()
            .connect_any(&db_pool_url(db_path))
            .await?)
    }

    pub async fn from_pool(db_path: &str, pool: DbPool) -> Result<Self> {
        let runtime = if matches!(pool, DbPool::Postgres(_)) {
            let (client, connection) = tokio_postgres::connect(db_path, NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(target: "torii_erc721::storage", error = %e, "PostgreSQL connection task failed");
                }
            });
            client.batch_execute(
                r"
                CREATE SCHEMA IF NOT EXISTS erc721;

                CREATE TABLE IF NOT EXISTS erc721.nft_ownership (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    block_number TEXT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp TEXT,
                    UNIQUE(token, token_id)
                );
                CREATE INDEX IF NOT EXISTS idx_nft_ownership_owner ON erc721.nft_ownership(owner);
                CREATE INDEX IF NOT EXISTS idx_nft_ownership_token ON erc721.nft_ownership(token);

                CREATE TABLE IF NOT EXISTS erc721.nft_transfers (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    from_addr BYTEA NOT NULL,
                    to_addr BYTEA NOT NULL,
                    block_number TEXT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp TEXT,
                    UNIQUE(token, tx_hash, token_id, from_addr, to_addr)
                );
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_token ON erc721.nft_transfers(token);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_from ON erc721.nft_transfers(from_addr);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_to ON erc721.nft_transfers(to_addr);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_block ON erc721.nft_transfers(block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_nft_transfers_token_id ON erc721.nft_transfers(token, token_id);

                CREATE TABLE IF NOT EXISTS erc721.nft_wallet_activity (
                    id BIGSERIAL PRIMARY KEY,
                    wallet_address BYTEA NOT NULL,
                    token BYTEA NOT NULL,
                    transfer_id BIGINT NOT NULL REFERENCES erc721.nft_transfers(id),
                    direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                    block_number TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_block ON erc721.nft_wallet_activity(wallet_address, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_token ON erc721.nft_wallet_activity(wallet_address, token, block_number DESC);

                CREATE TABLE IF NOT EXISTS erc721.nft_approvals (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    approved BYTEA NOT NULL,
                    block_number TEXT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp TEXT
                );

                CREATE TABLE IF NOT EXISTS erc721.nft_operators (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    operator BYTEA NOT NULL,
                    approved TEXT NOT NULL,
                    block_number TEXT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp TEXT,
                    UNIQUE(token, owner, operator)
                );

                CREATE TABLE IF NOT EXISTS erc721.token_metadata (
                    token BYTEA PRIMARY KEY,
                    name TEXT,
                    symbol TEXT,
                    total_supply BYTEA
                );

                CREATE TABLE IF NOT EXISTS erc721.token_uris (
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    uri TEXT,
                    metadata_json TEXT,
                    updated_at TEXT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT::TEXT),
                    PRIMARY KEY (token, token_id)
                );
                CREATE INDEX IF NOT EXISTS idx_token_uris_token ON erc721.token_uris(token);

                CREATE TABLE IF NOT EXISTS erc721.token_attributes (
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (token, token_id, key)
                );
                CREATE INDEX IF NOT EXISTS idx_token_attributes_token ON erc721.token_attributes(token);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_key ON erc721.token_attributes(key);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_key_value ON erc721.token_attributes(key, value);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_token_key_value ON erc721.token_attributes(token, key, value);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_token_token_id ON erc721.token_attributes(token, token_id);

                CREATE TABLE IF NOT EXISTS erc721.facet_keys (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    key_norm TEXT NOT NULL,
                    key_display TEXT NOT NULL,
                    UNIQUE(token, key_norm)
                );
                CREATE INDEX IF NOT EXISTS idx_facet_keys_token_key_norm ON erc721.facet_keys(token, key_norm);

                CREATE TABLE IF NOT EXISTS erc721.facet_values (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    facet_key_id BIGINT NOT NULL REFERENCES erc721.facet_keys(id) ON DELETE CASCADE,
                    value_norm TEXT NOT NULL,
                    value_display TEXT NOT NULL,
                    token_count TEXT NOT NULL DEFAULT '0',
                    UNIQUE(token, facet_key_id, value_norm)
                );
                CREATE INDEX IF NOT EXISTS idx_facet_values_token_key_value ON erc721.facet_values(token, facet_key_id, value_norm);

                CREATE TABLE IF NOT EXISTS erc721.facet_token_map (
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    facet_key_id BIGINT NOT NULL REFERENCES erc721.facet_keys(id) ON DELETE CASCADE,
                    facet_value_id BIGINT NOT NULL REFERENCES erc721.facet_values(id) ON DELETE CASCADE,
                    PRIMARY KEY (token, token_id, facet_key_id)
                );
                CREATE INDEX IF NOT EXISTS idx_facet_token_map_token_value_token_id ON erc721.facet_token_map(token, facet_value_id, token_id);
                CREATE INDEX IF NOT EXISTS idx_facet_token_map_token_token_id_key_value ON erc721.facet_token_map(token, token_id, facet_key_id, facet_value_id);
                ",
            ).await?;

            tracing::info!(target: "torii_erc721::storage", "PostgreSQL storage initialized");
            StorageRuntime::Postgres(PostgresStorageRuntime {
                conn: Arc::new(tokio::sync::Mutex::new(client)),
            })
        } else {
            let conn = Connection::open(db_path)?;

            // Enable WAL mode + Performance PRAGMAs
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA foreign_keys=ON;
                 PRAGMA cache_size=-64000;
                 PRAGMA temp_store=MEMORY;
                 PRAGMA mmap_size=268435456;
                 PRAGMA wal_autocheckpoint=10000;
                 PRAGMA page_size=4096;
                 PRAGMA busy_timeout=5000;",
            )?;

            tracing::info!(target: "torii_erc721::storage", "SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync");

            // NFT ownership (current state) - one owner per NFT
            conn.execute(
                "CREATE TABLE IF NOT EXISTS nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL,
                block_number TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp TEXT,
                UNIQUE(token, token_id)
            )",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_ownership_owner ON nft_ownership(owner)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_ownership_token ON nft_ownership(token)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_ownership_token_owner_token_id_ord
             ON nft_ownership(token, owner, length(token_id), token_id)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_ownership_owner_token_token_id_ord
             ON nft_ownership(owner, token, length(token_id), token_id)",
                [],
            )?;

            // Transfer history
            conn.execute(
                "CREATE TABLE IF NOT EXISTS nft_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                block_number TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp TEXT,
                UNIQUE(token, tx_hash, token_id, from_addr, to_addr)
            )",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_transfers_token ON nft_transfers(token)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_transfers_from ON nft_transfers(from_addr)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_transfers_to ON nft_transfers(to_addr)",
                [],
            )?;

            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_block ON nft_transfers(block_number DESC)",
            [],
        )?;

            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_token_id ON nft_transfers(token, token_id)",
            [],
        )?;

            // Wallet activity table for efficient OR queries
            conn.execute(
                "CREATE TABLE IF NOT EXISTS nft_wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number TEXT NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES nft_transfers(id)
            )",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_block
             ON nft_wallet_activity(wallet_address, block_number DESC)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_token
             ON nft_wallet_activity(wallet_address, token, block_number DESC)",
                [],
            )?;

            // Approvals (single token)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS nft_approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL,
                approved BLOB NOT NULL,
                block_number TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp TEXT
            )",
                [],
            )?;

            // Operator approvals (all tokens)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS nft_operators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                owner BLOB NOT NULL,
                operator BLOB NOT NULL,
                approved TEXT NOT NULL,
                block_number TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp TEXT,
                UNIQUE(token, owner, operator)
            )",
                [],
            )?;

            // Token metadata table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS token_metadata (
                token BLOB PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                total_supply BLOB
            )",
                [],
            )?;

            conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS token_uris (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                uri TEXT,
                metadata_json TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (token, token_id)
            );
            CREATE INDEX IF NOT EXISTS idx_token_uris_token ON token_uris(token);

            CREATE TABLE IF NOT EXISTS token_attributes (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (token, token_id, key),
                FOREIGN KEY (token, token_id) REFERENCES token_uris(token, token_id)
            );
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token ON token_attributes(token);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key ON token_attributes(key);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key_value ON token_attributes(key, value);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token_key_value ON token_attributes(token, key, value);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token_token_id ON token_attributes(token, token_id);

            CREATE TABLE IF NOT EXISTS facet_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                key_norm TEXT NOT NULL,
                key_display TEXT NOT NULL,
                UNIQUE(token, key_norm)
            );
            CREATE INDEX IF NOT EXISTS idx_facet_keys_token_key_norm ON facet_keys(token, key_norm);

            CREATE TABLE IF NOT EXISTS facet_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                facet_key_id INTEGER NOT NULL,
                value_norm TEXT NOT NULL,
                value_display TEXT NOT NULL,
                token_count TEXT NOT NULL DEFAULT '0',
                UNIQUE(token, facet_key_id, value_norm),
                FOREIGN KEY (facet_key_id) REFERENCES facet_keys(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_facet_values_token_key_value ON facet_values(token, facet_key_id, value_norm);

            CREATE TABLE IF NOT EXISTS facet_token_map (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                facet_key_id INTEGER NOT NULL,
                facet_value_id INTEGER NOT NULL,
                PRIMARY KEY (token, token_id, facet_key_id),
                FOREIGN KEY (facet_key_id) REFERENCES facet_keys(id) ON DELETE CASCADE,
                FOREIGN KEY (facet_value_id) REFERENCES facet_values(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_facet_token_map_token_value_token_id ON facet_token_map(token, facet_value_id, token_id);
            CREATE INDEX IF NOT EXISTS idx_facet_token_map_token_token_id_key_value ON facet_token_map(token, token_id, facet_key_id, facet_value_id);",
        )?;

            tracing::info!(target: "torii_erc721::storage", db_path = %db_path, "ERC721 database initialized");

            StorageRuntime::Sqlite(SqliteStorageRuntime {
                conn: Arc::new(Mutex::new(conn)),
            })
        };

        Ok(Self {
            db: Erc721Db::new(pool, runtime),
        })
    }

    pub fn pool(&self) -> &DbPool {
        &self.db.backend
    }

    fn is_postgres(&self) -> bool {
        self.db.runtime.backend() == StorageBackend::Postgres
    }

    fn sqlite_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        match &self.db.runtime {
            StorageRuntime::Sqlite(runtime) => Ok(runtime.conn.lock().unwrap()),
            StorageRuntime::Postgres(_) => Err(anyhow::anyhow!(
                "SQLite connection not initialized for ERC721 storage"
            )),
        }
    }

    #[cfg(test)]
    fn with_sqlite_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T> {
        let conn = self.sqlite_conn()?;
        Ok(f(&conn)?)
    }

    /// Insert multiple transfers and update ownership in a single transaction
    pub async fn insert_transfers_batch(&self, transfers: &[NftTransferData]) -> Result<usize> {
        if self.is_postgres() {
            return self.pg_insert_transfers_batch(transfers).await;
        }
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.sqlite_conn()?;
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut transfer_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO nft_transfers (token, token_id, from_addr, to_addr, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;

            let mut ownership_stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO nft_ownership (token, token_id, owner, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, COALESCE(?6, strftime('%s', 'now')))",
            )?;
            let mut wallet_both_stmt = tx.prepare_cached(
                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'both', ?4)",
            )?;
            let mut wallet_sent_stmt = tx.prepare_cached(
                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'sent', ?4)",
            )?;
            let mut wallet_received_stmt = tx.prepare_cached(
                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'received', ?4)",
            )?;

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let token_id_blob = u256_to_blob(transfer.token_id);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                // Insert transfer
                let rows = transfer_stmt.execute(params![
                    &token_blob,
                    &token_id_blob,
                    &from_blob,
                    &to_blob,
                    transfer.block_number.to_string(),
                    &tx_hash_blob,
                    transfer.timestamp.map(|t| t.to_string()),
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Update ownership (only if to is not zero address)
                    if transfer.to != Felt::ZERO {
                        ownership_stmt.execute(params![
                            &token_blob,
                            &token_id_blob,
                            &to_blob,
                            transfer.block_number.to_string(),
                            &tx_hash_blob,
                            transfer.timestamp.map(|t| t.to_string()),
                        ])?;
                    }

                    // Insert wallet activity records
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        wallet_both_stmt.execute(params![
                            &from_blob,
                            &token_blob,
                            transfer_id,
                            transfer.block_number.to_string()
                        ])?;
                    } else {
                        if transfer.from != Felt::ZERO {
                            wallet_sent_stmt.execute(params![
                                &from_blob,
                                &token_blob,
                                transfer_id,
                                transfer.block_number.to_string()
                            ])?;
                        }
                        if transfer.to != Felt::ZERO {
                            wallet_received_stmt.execute(params![
                                &to_blob,
                                &token_blob,
                                transfer_id,
                                transfer.block_number.to_string()
                            ])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Insert operator approvals in a single transaction
    pub async fn insert_operator_approvals_batch(
        &self,
        approvals: &[OperatorApprovalData],
    ) -> Result<usize> {
        if self.is_postgres() {
            return self.pg_insert_operator_approvals_batch(approvals).await;
        }
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.sqlite_conn()?;
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO nft_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;

            for approval in approvals {
                let token_blob = felt_to_blob(approval.token);
                let owner_blob = felt_to_blob(approval.owner);
                let operator_blob = felt_to_blob(approval.operator);
                let tx_hash_blob = felt_to_blob(approval.tx_hash);

                stmt.execute(params![
                    &token_blob,
                    &owner_blob,
                    &operator_blob,
                    (approval.approved as u64).to_string(),
                    approval.block_number.to_string(),
                    &tx_hash_blob,
                    approval.timestamp.map(|t| t.to_string()),
                ])?;

                inserted += 1;
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Get filtered transfers with cursor-based pagination
    pub async fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<NftTransferData>, Option<TransferCursor>)> {
        if self.is_postgres() {
            return self
                .pg_get_transfers_filtered(
                    wallet, from, to, tokens, token_ids, block_from, block_to, cursor, limit,
                )
                .await;
        }
        let conn = self.sqlite_conn()?;

        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM nft_wallet_activity wa
                 JOIN nft_transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND wa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            query.push_str(
                "SELECT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM nft_transfers t
                 WHERE 1=1",
            );

            if let Some(from_addr) = from {
                query.push_str(" AND t.from_addr = ?");
                params_vec.push(Box::new(felt_to_blob(from_addr)));
            }

            if let Some(to_addr) = to {
                query.push_str(" AND t.to_addr = ?");
                params_vec.push(Box::new(felt_to_blob(to_addr)));
            }

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND t.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        }

        if !token_ids.is_empty() {
            let placeholders: Vec<&str> = token_ids.iter().map(|_| "?").collect();
            query.push_str(&format!(" AND t.token_id IN ({})", placeholders.join(",")));
            for tid in token_ids {
                params_vec.push(Box::new(u256_to_blob(*tid)));
            }
        }

        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ?");
            params_vec.push(Box::new(block_min.to_string()));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ?");
            params_vec.push(Box::new(block_max.to_string()));
        }

        if let Some(c) = cursor {
            query.push_str(" AND (t.block_number < ? OR (t.block_number = ? AND t.id < ?))");
            params_vec.push(Box::new(c.block_number.to_string()));
            params_vec.push(Box::new(c.block_number.to_string()));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY t.block_number DESC, t.id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare_cached(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let token_id_bytes: Vec<u8> = row.get(2)?;
            let from_bytes: Vec<u8> = row.get(3)?;
            let to_bytes: Vec<u8> = row.get(4)?;
            let block_number_str: String = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp_str: Option<String> = row.get(7)?;

            Ok(NftTransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                block_number: block_number_str.parse::<u64>().unwrap_or(0),
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp: timestamp_str.and_then(|s| s.parse::<i64>().ok()),
            })
        })?;

        let transfers: Vec<NftTransferData> = rows.collect::<Result<_, _>>()?;

        let next_cursor = if transfers.len() == limit as usize {
            transfers.last().map(|t| TransferCursor {
                block_number: t.block_number,
                id: t.id.unwrap(),
            })
        } else {
            None
        };

        Ok((transfers, next_cursor))
    }

    /// Get current owner of a specific NFT
    pub async fn get_owner(&self, token: Felt, token_id: U256) -> Result<Option<Felt>> {
        if self.is_postgres() {
            return self.pg_get_owner(token, token_id).await;
        }
        let conn = self.sqlite_conn()?;

        let result: Result<Vec<u8>, _> = conn.query_row(
            "SELECT owner FROM nft_ownership WHERE token = ? AND token_id = ?",
            params![felt_to_blob(token), u256_to_blob(token_id)],
            |row| row.get(0),
        );

        match result {
            Ok(owner_bytes) => Ok(Some(blob_to_felt(&owner_bytes))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get ownership records filtered by owner
    pub async fn get_ownership_by_owner(
        &self,
        owner: Felt,
        tokens: &[Felt],
        cursor: Option<OwnershipCursor>,
        limit: u32,
    ) -> Result<(Vec<NftOwnershipData>, Option<OwnershipCursor>)> {
        if self.is_postgres() {
            return self
                .pg_get_ownership_by_owner(owner, tokens, cursor, limit)
                .await;
        }
        let conn = self.sqlite_conn()?;

        let mut query = String::from(
            "SELECT id, token, token_id, owner, block_number FROM nft_ownership WHERE owner = ?",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(felt_to_blob(owner))];

        if !tokens.is_empty() {
            let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
            query.push_str(&format!(" AND token IN ({})", placeholders.join(",")));
            for token in tokens {
                params_vec.push(Box::new(felt_to_blob(*token)));
            }
        }

        if let Some(c) = cursor {
            query.push_str(" AND (block_number < ? OR (block_number = ? AND id < ?))");
            params_vec.push(Box::new(c.block_number.to_string()));
            params_vec.push(Box::new(c.block_number.to_string()));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY block_number DESC, id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare_cached(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let token_id_bytes: Vec<u8> = row.get(2)?;
            let owner_bytes: Vec<u8> = row.get(3)?;
            let block_number_str: String = row.get(4)?;

            Ok(NftOwnershipData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                owner: blob_to_felt(&owner_bytes),
                block_number: block_number_str.parse::<u64>().unwrap_or(0),
            })
        })?;

        let ownership: Vec<NftOwnershipData> = rows.collect::<Result<_, _>>()?;

        let next_cursor = if ownership.len() == limit as usize {
            ownership.last().map(|o| OwnershipCursor {
                block_number: o.block_number,
                id: o.id.unwrap(),
            })
        } else {
            None
        };

        Ok((ownership, next_cursor))
    }

    /// Query token IDs by flattened metadata attributes.
    ///
    /// Filter semantics:
    /// - OR within a single key (`values`)
    /// - AND across keys (`filters`)
    pub async fn query_token_ids_by_attributes(
        &self,
        token: Felt,
        filters: &[(String, Vec<String>)],
        cursor_token_id: Option<U256>,
        limit: u32,
        include_facets: bool,
        facet_limit: u32,
    ) -> Result<TokenAttributeQueryResult> {
        let filters = Self::normalize_attribute_filters(filters);
        let page_limit = limit.clamp(1, 1000);
        let page_fetch = page_limit as i64 + 1;
        let facet_limit = facet_limit.clamp(1, 1000) as i64;

        if self.is_postgres() {
            let resolved_filters = self.pg_resolve_facet_filters(token, &filters).await?;
            let Some(resolved_filters) = resolved_filters else {
                return Ok(TokenAttributeQueryResult {
                    token_ids: Vec::new(),
                    next_cursor_token_id: None,
                    total_hits: 0,
                    facets: Vec::new(),
                });
            };
            return self
                .pg_query_token_ids_by_facets(
                    token,
                    &resolved_filters,
                    cursor_token_id,
                    page_fetch,
                    include_facets,
                    facet_limit,
                )
                .await;
        }

        let conn = self.sqlite_conn()?;
        let resolved_filters = self.sqlite_resolve_facet_filters(&conn, token, &filters)?;
        let Some(resolved_filters) = resolved_filters else {
            return Ok(TokenAttributeQueryResult {
                token_ids: Vec::new(),
                next_cursor_token_id: None,
                total_hits: 0,
                facets: Vec::new(),
            });
        };

        self.sqlite_query_token_ids_by_facets(
            &conn,
            token,
            &resolved_filters,
            cursor_token_id,
            page_fetch,
            include_facets,
            facet_limit,
        )
    }

    /// Get transfer count
    pub async fn get_transfer_count(&self) -> Result<u64> {
        if self.is_postgres() {
            return self.pg_get_transfer_count().await;
        }
        let conn = self.sqlite_conn()?;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM nft_transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get unique token contract count
    pub async fn get_token_count(&self) -> Result<u64> {
        if self.is_postgres() {
            return self.pg_get_token_count().await;
        }
        let conn = self.sqlite_conn()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token) FROM nft_transfers",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get unique NFT count
    pub async fn get_nft_count(&self) -> Result<u64> {
        if self.is_postgres() {
            return self.pg_get_nft_count().await;
        }
        let conn = self.sqlite_conn()?;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM nft_ownership", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get latest block number indexed
    pub async fn get_latest_block(&self) -> Result<Option<u64>> {
        if self.is_postgres() {
            return self.pg_get_latest_block().await;
        }
        let conn = self.sqlite_conn()?;
        let block: Option<String> = conn
            .query_row("SELECT MAX(block_number) FROM nft_transfers", [], |row| {
                row.get(0)
            })
            .ok()
            .flatten();
        Ok(block.and_then(|b| b.parse::<u64>().ok()))
    }

    // ===== Token Metadata Methods =====

    /// Check if metadata exists for a token
    pub async fn has_token_metadata(&self, token: Felt) -> Result<bool> {
        if self.is_postgres() {
            return self.pg_has_token_metadata(token).await;
        }
        let conn = self.sqlite_conn()?;
        let token_blob = felt_to_blob(token);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_metadata
             WHERE token = ?
               AND name IS NOT NULL
               AND TRIM(name) <> ''
               AND symbol IS NOT NULL
               AND TRIM(symbol) <> ''
               AND total_supply IS NOT NULL",
            params![&token_blob],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub async fn has_token_metadata_batch(&self, tokens: &[Felt]) -> Result<HashSet<Felt>> {
        if tokens.is_empty() {
            return Ok(HashSet::new());
        }
        if self.is_postgres() {
            return self.pg_has_token_metadata_batch(tokens).await;
        }

        let conn = self.sqlite_conn()?;
        let mut existing = HashSet::with_capacity(tokens.len());

        for chunk in tokens.chunks(SQLITE_TOKEN_BATCH_SIZE) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let query = format!(
                "SELECT token FROM token_metadata
                 WHERE token IN ({placeholders})
                   AND name IS NOT NULL
                   AND TRIM(name) <> ''
                   AND symbol IS NOT NULL
                   AND TRIM(symbol) <> ''
                   AND total_supply IS NOT NULL"
            );
            let token_blobs: Vec<Vec<u8>> =
                chunk.iter().map(|token| felt_to_blob(*token)).collect();
            let mut stmt = conn.prepare_cached(&query)?;
            let rows = stmt.query_map(params_from_iter(token_blobs.iter()), |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                Ok(blob_to_felt(&token_bytes))
            })?;

            for row in rows {
                existing.insert(row?);
            }
        }

        Ok(existing)
    }

    /// Insert or update token metadata
    pub async fn upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        if self.is_postgres() {
            return self
                .pg_upsert_token_metadata(token, name, symbol, total_supply)
                .await;
        }
        let conn = self.sqlite_conn()?;
        let token_blob = felt_to_blob(token);
        let supply_blob = total_supply.map(u256_to_blob);
        conn.execute(
            "INSERT INTO token_metadata (token, name, symbol, total_supply)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(token) DO UPDATE SET
                 name = COALESCE(excluded.name, token_metadata.name),
                 symbol = COALESCE(excluded.symbol, token_metadata.symbol),
                 total_supply = COALESCE(excluded.total_supply, token_metadata.total_supply)",
            params![&token_blob, name, symbol, supply_blob],
        )?;
        Ok(())
    }

    /// Get token metadata
    pub async fn get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<U256>)>> {
        if self.is_postgres() {
            return self.pg_get_token_metadata(token).await;
        }
        let conn = self.sqlite_conn()?;
        let token_blob = felt_to_blob(token);
        let result = conn
            .query_row(
                "SELECT name, symbol, total_supply FROM token_metadata WHERE token = ?",
                params![&token_blob],
                |row| {
                    let name: Option<String> = row.get(0)?;
                    let symbol: Option<String> = row.get(1)?;
                    let supply_bytes: Option<Vec<u8>> = row.get(2)?;
                    Ok((name, symbol, supply_bytes.map(|b| blob_to_u256(&b))))
                },
            )
            .ok();
        Ok(result)
    }

    /// Get all token metadata
    pub async fn get_all_token_metadata(
        &self,
    ) -> Result<Vec<(Felt, Option<String>, Option<String>, Option<U256>)>> {
        let conn = self.sqlite_conn()?;
        let mut stmt =
            conn.prepare_cached("SELECT token, name, symbol, total_supply FROM token_metadata")?;
        let rows = stmt.query_map([], |row| {
            let token_bytes: Vec<u8> = row.get(0)?;
            let name: Option<String> = row.get(1)?;
            let symbol: Option<String> = row.get(2)?;
            let supply_bytes: Option<Vec<u8>> = row.get(3)?;
            Ok((
                blob_to_felt(&token_bytes),
                name,
                symbol,
                supply_bytes.map(|b| blob_to_u256(&b)),
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get token metadata with cursor-based pagination.
    ///
    /// Returns at most `limit` rows and an optional next cursor token.
    pub async fn get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(Felt, Option<String>, Option<String>, Option<U256>)>,
        Option<Felt>,
    )> {
        if self.is_postgres() {
            return self.pg_get_token_metadata_paginated(cursor, limit).await;
        }
        let conn = self.sqlite_conn()?;
        let fetch_limit = limit.clamp(1, 1000) as usize + 1;

        let mut out = if let Some(cursor_token) = cursor {
            let cursor_blob = felt_to_blob(cursor_token);
            let mut stmt = conn.prepare_cached(
                "SELECT token, name, symbol, total_supply
                 FROM token_metadata
                 WHERE token > ?1
                 ORDER BY token ASC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![&cursor_blob, fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let supply_bytes: Option<Vec<u8>> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    supply_bytes.map(|b| blob_to_u256(&b)),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare_cached(
                "SELECT token, name, symbol, total_supply
                 FROM token_metadata
                 ORDER BY token ASC
                 LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let supply_bytes: Option<Vec<u8>> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    supply_bytes.map(|b| blob_to_u256(&b)),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let capped = limit.clamp(1, 1000) as usize;
        let next_cursor = if out.len() > capped {
            let next = out[capped].0;
            out.truncate(capped);
            Some(next)
        } else {
            None
        };

        Ok((out, next_cursor))
    }

    /// Returns true if a token URI row exists for `(token, token_id)`.
    pub async fn has_token_uri(&self, token: Felt, token_id: U256) -> Result<bool> {
        if self.is_postgres() {
            return self.pg_has_token_uri(token, token_id).await;
        }
        let conn = self.sqlite_conn()?;
        let token_blob = felt_to_blob(token);
        let token_id_blob = u256_to_blob(token_id);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_uris
             WHERE token = ?1
               AND token_id = ?2",
            params![&token_blob, &token_id_blob],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub async fn has_token_uri_batch(
        &self,
        tokens: &[(Felt, U256)],
    ) -> Result<HashSet<(Felt, U256)>> {
        if tokens.is_empty() {
            return Ok(HashSet::new());
        }
        if self.is_postgres() {
            return self.pg_has_token_uri_batch(tokens).await;
        }

        let conn = self.sqlite_conn()?;
        let mut existing = HashSet::with_capacity(tokens.len());

        for chunk in tokens.chunks(SQLITE_TOKEN_PAIR_BATCH_SIZE) {
            let predicates = vec!["(token = ? AND token_id = ?)"; chunk.len()].join(" OR ");
            let query = format!("SELECT token, token_id FROM token_uris WHERE {predicates}");
            let mut params_vec: Vec<Box<dyn ToSql>> = Vec::with_capacity(chunk.len() * 2);
            for (token, token_id) in chunk {
                params_vec.push(Box::new(felt_to_blob(*token)));
                params_vec.push(Box::new(u256_to_blob(*token_id)));
            }
            let params_refs: Vec<&dyn ToSql> =
                params_vec.iter().map(std::convert::AsRef::as_ref).collect();
            let mut stmt = conn.prepare_cached(&query)?;
            let rows = stmt.query_map(params_refs.as_slice(), |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let token_id_bytes: Vec<u8> = row.get(1)?;
                Ok((blob_to_felt(&token_bytes), blob_to_u256(&token_id_bytes)))
            })?;

            for row in rows {
                existing.insert(row?);
            }
        }

        Ok(existing)
    }

    /// Returns all token URI rows for a given contract.
    pub async fn get_token_uris_by_contract(
        &self,
        token: Felt,
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        if self.is_postgres() {
            return self.pg_get_token_uris_by_contract(token).await;
        }
        let conn = self.sqlite_conn()?;
        let token_blob = felt_to_blob(token);
        let mut stmt = conn.prepare_cached(
            "SELECT token_id, uri, metadata_json
             FROM token_uris
             WHERE token = ?1
             ORDER BY token_id ASC",
        )?;
        let rows = stmt.query_map(params![&token_blob], |row| {
            let token_id_bytes: Vec<u8> = row.get(0)?;
            let uri: Option<String> = row.get(1)?;
            let metadata_json: Option<String> = row.get(2)?;
            Ok((blob_to_u256(&token_id_bytes), uri, metadata_json))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub async fn get_token_uris_batch(
        &self,
        token: Felt,
        token_ids: &[U256],
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        if self.is_postgres() {
            return self.pg_get_token_uris_batch(token, token_ids).await;
        }

        let conn = self.sqlite_conn()?;
        let token_blob = felt_to_blob(token);
        let mut rows_out = Vec::new();
        for chunk in token_ids.chunks(SQLITE_TOKEN_BATCH_SIZE) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = format!(
                "SELECT token_id, uri, metadata_json
                 FROM token_uris
                 WHERE token = ?1 AND token_id IN ({placeholders})"
            );
            let mut params_vec: Vec<Box<dyn ToSql>> = Vec::with_capacity(chunk.len() + 1);
            params_vec.push(Box::new(token_blob.clone()));
            for token_id in chunk {
                params_vec.push(Box::new(u256_to_blob(*token_id)));
            }
            let params_refs: Vec<&dyn ToSql> =
                params_vec.iter().map(std::convert::AsRef::as_ref).collect();
            let mut stmt = conn.prepare_cached(&query)?;
            let rows = stmt.query_map(params_refs.as_slice(), |row| {
                let token_id_bytes: Vec<u8> = row.get(0)?;
                let uri: Option<String> = row.get(1)?;
                let metadata_json: Option<String> = row.get(2)?;
                Ok((blob_to_u256(&token_id_bytes), uri, metadata_json))
            })?;
            rows_out.extend(rows.collect::<std::result::Result<Vec<_>, _>>()?);
        }

        Ok(rows_out)
    }

    async fn pg_client(&self) -> Result<tokio::sync::MutexGuard<'_, Client>> {
        match &self.db.runtime {
            StorageRuntime::Postgres(runtime) => Ok(runtime.conn.lock().await),
            StorageRuntime::Sqlite(_) => {
                Err(anyhow::anyhow!("PostgreSQL connection not initialized"))
            }
        }
    }

    fn pg_next_param(
        params: &mut Vec<Box<dyn PgToSql + Sync + Send>>,
        value: impl PgToSql + Sync + Send + 'static,
    ) -> String {
        params.push(Box::new(value));
        format!("${}", params.len())
    }

    fn normalize_attribute_filters(
        filters: &[(String, Vec<String>)],
    ) -> Vec<(String, Vec<String>)> {
        filters
            .iter()
            .filter_map(|(key, values)| {
                let key = key.trim();
                if key.is_empty() {
                    return None;
                }

                let mut out_values: Vec<String> = Vec::new();
                for value in values {
                    let value = value.trim();
                    if value.is_empty() || out_values.iter().any(|v| v == value) {
                        continue;
                    }
                    out_values.push(value.to_owned());
                }

                if out_values.is_empty() {
                    None
                } else {
                    Some((key.to_owned(), out_values))
                }
            })
            .collect()
    }

    fn sqlite_resolve_facet_filters(
        &self,
        conn: &Connection,
        token: Felt,
        filters: &[(String, Vec<String>)],
    ) -> Result<Option<Vec<ResolvedFacetFilter>>> {
        let token_blob = felt_to_blob(token);
        let mut resolved = Vec::with_capacity(filters.len());

        for (key, values) in filters {
            let key_id: i64 = match conn.query_row(
                "SELECT id FROM facet_keys WHERE token = ?1 AND key_norm = ?2",
                params![&token_blob, key],
                |row| row.get(0),
            ) {
                Ok(id) => id,
                Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
                Err(error) => return Err(error.into()),
            };

            let placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = format!(
                "SELECT id FROM facet_values WHERE token = ? AND facet_key_id = ? AND value_norm IN ({placeholders})"
            );
            let mut params_vec: Vec<Box<dyn ToSql>> = Vec::with_capacity(values.len() + 2);
            params_vec.push(Box::new(token_blob.clone()));
            params_vec.push(Box::new(key_id));
            for value in values {
                params_vec.push(Box::new(value.clone()));
            }
            let params_refs: Vec<&dyn ToSql> =
                params_vec.iter().map(std::convert::AsRef::as_ref).collect();
            let mut stmt = conn.prepare_cached(&query)?;
            let rows = stmt.query_map(params_refs.as_slice(), |row| row.get::<_, i64>(0))?;
            let value_ids = rows.collect::<std::result::Result<Vec<_>, _>>()?;
            if value_ids.is_empty() {
                return Ok(None);
            }

            resolved.push(ResolvedFacetFilter { key_id, value_ids });
        }

        Ok(Some(resolved))
    }

    fn sqlite_build_candidate_query(
        token: Felt,
        resolved_filters: &[ResolvedFacetFilter],
    ) -> (String, Vec<Box<dyn ToSql>>) {
        let token_blob = felt_to_blob(token);
        if resolved_filters.is_empty() {
            return (
                "SELECT token_id FROM token_uris WHERE token = ?".to_owned(),
                vec![Box::new(token_blob)],
            );
        }

        let mut query = String::new();
        let mut params: Vec<Box<dyn ToSql>> = Vec::new();
        for (idx, filter) in resolved_filters.iter().enumerate() {
            if idx > 0 {
                query.push_str(" INTERSECT ");
            }
            query.push_str(
                "SELECT token_id FROM facet_token_map WHERE token = ? AND facet_key_id = ? AND facet_value_id IN (",
            );
            params.push(Box::new(token_blob.clone()));
            params.push(Box::new(filter.key_id));
            let placeholders = filter
                .value_ids
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&placeholders);
            query.push(')');
            for value_id in &filter.value_ids {
                params.push(Box::new(*value_id));
            }
        }

        (query, params)
    }

    fn sqlite_query_token_ids_by_facets(
        &self,
        conn: &Connection,
        token: Felt,
        resolved_filters: &[ResolvedFacetFilter],
        cursor_token_id: Option<U256>,
        page_fetch: i64,
        include_facets: bool,
        facet_limit: i64,
    ) -> Result<TokenAttributeQueryResult> {
        let (candidate_query, candidate_params_for_page) =
            Self::sqlite_build_candidate_query(token, resolved_filters);
        let (_, candidate_params_for_count) =
            Self::sqlite_build_candidate_query(token, resolved_filters);

        let mut page_query = format!(
            "WITH candidate_tokens AS ({candidate_query}) SELECT token_id FROM candidate_tokens"
        );
        let mut page_params = candidate_params_for_page;
        if let Some(cursor) = cursor_token_id {
            page_query.push_str(" WHERE token_id > ?");
            page_params.push(Box::new(u256_to_blob(cursor)));
        }
        page_query.push_str(" ORDER BY token_id ASC LIMIT ?");
        page_params.push(Box::new(page_fetch));

        let page_refs: Vec<&dyn ToSql> = page_params
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        let mut stmt = conn.prepare_cached(&page_query)?;
        let rows = stmt.query_map(page_refs.as_slice(), |row| row.get::<_, Vec<u8>>(0))?;
        let mut token_ids = rows
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|bytes| blob_to_u256(&bytes))
            .collect::<Vec<_>>();

        let page_limit = (page_fetch - 1) as usize;
        let next_cursor_token_id = if token_ids.len() > page_limit {
            let next = token_ids[page_limit];
            token_ids.truncate(page_limit);
            Some(next)
        } else {
            None
        };

        let count_query = format!(
            "WITH candidate_tokens AS ({candidate_query}) SELECT COUNT(*) FROM candidate_tokens"
        );
        let count_refs: Vec<&dyn ToSql> = candidate_params_for_count
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        let total_hits: i64 =
            conn.query_row(&count_query, count_refs.as_slice(), |row| row.get(0))?;

        let facets = if include_facets {
            let (_, candidate_params_for_facets) =
                Self::sqlite_build_candidate_query(token, resolved_filters);
            let facet_query = format!(
                "WITH candidate_tokens AS ({candidate_query})
                 SELECT fk.key_display, fv.value_display, COUNT(*) AS cnt
                 FROM facet_token_map m
                 JOIN candidate_tokens c ON c.token_id = m.token_id
                 JOIN facet_keys fk ON fk.id = m.facet_key_id
                 JOIN facet_values fv ON fv.id = m.facet_value_id
                 WHERE m.token = ?
                 GROUP BY fk.key_display, fv.value_display
                 ORDER BY cnt DESC, fk.key_display ASC, fv.value_display ASC
                 LIMIT ?"
            );
            let mut facet_params = candidate_params_for_facets;
            facet_params.push(Box::new(felt_to_blob(token)));
            facet_params.push(Box::new(facet_limit));
            let facet_refs: Vec<&dyn ToSql> = facet_params
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect();
            let mut stmt = conn.prepare_cached(&facet_query)?;
            let rows = stmt.query_map(facet_refs.as_slice(), |row| {
                Ok(AttributeFacetCount {
                    key: row.get(0)?,
                    value: row.get(1)?,
                    count: row.get::<_, i64>(2)? as u64,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        Ok(TokenAttributeQueryResult {
            token_ids,
            next_cursor_token_id,
            total_hits: total_hits as u64,
            facets,
        })
    }

    async fn pg_resolve_facet_filters(
        &self,
        token: Felt,
        filters: &[(String, Vec<String>)],
    ) -> Result<Option<Vec<ResolvedFacetFilter>>> {
        let client = self.pg_client().await?;
        let token_blob = felt_to_blob(token);
        let mut resolved = Vec::with_capacity(filters.len());

        for (key, values) in filters {
            let key_row = client
                .query_opt(
                    "SELECT id FROM erc721.facet_keys WHERE token = $1 AND key_norm = $2",
                    &[&token_blob, key],
                )
                .await?;
            let Some(key_row) = key_row else {
                return Ok(None);
            };
            let key_id: i64 = key_row.get(0);

            let rows = client
                .query(
                    "SELECT id FROM erc721.facet_values WHERE token = $1 AND facet_key_id = $2 AND value_norm = ANY($3::text[])",
                    &[&token_blob, &key_id, values],
                )
                .await?;
            let value_ids = rows
                .into_iter()
                .map(|row| row.get::<usize, i64>(0))
                .collect::<Vec<_>>();
            if value_ids.is_empty() {
                return Ok(None);
            }

            resolved.push(ResolvedFacetFilter { key_id, value_ids });
        }

        Ok(Some(resolved))
    }

    fn pg_build_candidate_query(
        token: Felt,
        resolved_filters: &[ResolvedFacetFilter],
    ) -> (String, Vec<Box<dyn PgToSql + Sync + Send>>) {
        let mut candidate_query = String::new();
        let mut candidate_params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();
        if resolved_filters.is_empty() {
            candidate_query.push_str("SELECT token_id FROM erc721.token_uris WHERE token = ");
            candidate_query.push_str(&Self::pg_next_param(
                &mut candidate_params,
                felt_to_blob(token),
            ));
        } else {
            for (idx, filter) in resolved_filters.iter().enumerate() {
                if idx > 0 {
                    candidate_query.push_str(" INTERSECT ");
                }
                candidate_query
                    .push_str("SELECT token_id FROM erc721.facet_token_map WHERE token = ");
                candidate_query.push_str(&Self::pg_next_param(
                    &mut candidate_params,
                    felt_to_blob(token),
                ));
                candidate_query.push_str(" AND facet_key_id = ");
                candidate_query
                    .push_str(&Self::pg_next_param(&mut candidate_params, filter.key_id));
                candidate_query.push_str(" AND facet_value_id = ANY(");
                candidate_query.push_str(&Self::pg_next_param(
                    &mut candidate_params,
                    filter.value_ids.clone(),
                ));
                candidate_query.push(')');
            }
        }

        (candidate_query, candidate_params)
    }

    async fn pg_insert_transfers_batch(&self, transfers: &[NftTransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let zero_blob = felt_to_blob(Felt::ZERO);
        let mut token_vec = Vec::with_capacity(transfers.len());
        let mut token_id_vec = Vec::with_capacity(transfers.len());
        let mut from_vec = Vec::with_capacity(transfers.len());
        let mut to_vec = Vec::with_capacity(transfers.len());
        let mut block_vec: Vec<String> = Vec::with_capacity(transfers.len());
        let mut tx_hash_vec = Vec::with_capacity(transfers.len());
        let mut ts_vec: Vec<String> = Vec::with_capacity(transfers.len());

        for transfer in transfers {
            token_vec.push(felt_to_blob(transfer.token));
            token_id_vec.push(u256_to_blob(transfer.token_id));
            from_vec.push(felt_to_blob(transfer.from));
            to_vec.push(felt_to_blob(transfer.to));
            block_vec.push(transfer.block_number.to_string());
            tx_hash_vec.push(felt_to_blob(transfer.tx_hash));
            ts_vec.push(
                transfer
                    .timestamp
                    .unwrap_or_else(|| chrono::Utc::now().timestamp())
                    .to_string(),
            );
        }

        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "WITH inserted AS (
                    INSERT INTO erc721.nft_transfers (token, token_id, from_addr, to_addr, block_number, tx_hash, timestamp)
                    SELECT i.token, i.token_id, i.from_addr, i.to_addr, i.block_number, i.tx_hash, i.timestamp
                    FROM unnest(
                        $1::bytea[],
                        $2::bytea[],
                        $3::bytea[],
                        $4::bytea[],
                        $5::text[],
                        $6::bytea[],
                        $7::text[]
                    ) AS i(token, token_id, from_addr, to_addr, block_number, tx_hash, timestamp)
                    ON CONFLICT (token, tx_hash, token_id, from_addr, to_addr) DO NOTHING
                    RETURNING id, token, token_id, from_addr, to_addr, block_number, tx_hash, timestamp
                ),
                _ownership AS (
                    INSERT INTO erc721.nft_ownership (token, token_id, owner, block_number, tx_hash, timestamp)
                    SELECT DISTINCT ON (token, token_id) token, token_id, to_addr, block_number, tx_hash, timestamp
                    FROM inserted
                    WHERE to_addr <> $8::bytea
                    ORDER BY token, token_id, id DESC
                    ON CONFLICT (token, token_id) DO UPDATE SET
                        owner = EXCLUDED.owner,
                        block_number = EXCLUDED.block_number,
                        tx_hash = EXCLUDED.tx_hash,
                        timestamp = EXCLUDED.timestamp
                ),
                _activity AS (
                    INSERT INTO erc721.nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                    SELECT from_addr, token, id, 'both', block_number
                    FROM inserted
                    WHERE from_addr <> $8::bytea AND to_addr <> $8::bytea AND from_addr = to_addr
                    UNION ALL
                    SELECT from_addr, token, id, 'sent', block_number
                    FROM inserted
                    WHERE from_addr <> $8::bytea AND from_addr <> to_addr
                    UNION ALL
                    SELECT to_addr, token, id, 'received', block_number
                    FROM inserted
                    WHERE to_addr <> $8::bytea AND from_addr <> to_addr
                )
                SELECT COUNT(*)::bigint FROM inserted",
                &[
                    &token_vec,
                    &token_id_vec,
                    &from_vec,
                    &to_vec,
                    &block_vec,
                    &tx_hash_vec,
                    &ts_vec,
                    &zero_blob,
                ],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as usize)
    }

    async fn pg_insert_operator_approvals_batch(
        &self,
        approvals: &[OperatorApprovalData],
    ) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut token_vec = Vec::with_capacity(approvals.len());
        let mut owner_vec = Vec::with_capacity(approvals.len());
        let mut operator_vec = Vec::with_capacity(approvals.len());
        let mut approved_vec: Vec<String> = Vec::with_capacity(approvals.len());
        let mut block_vec: Vec<String> = Vec::with_capacity(approvals.len());
        let mut tx_hash_vec = Vec::with_capacity(approvals.len());
        let mut ts_vec: Vec<String> = Vec::with_capacity(approvals.len());

        for approval in approvals {
            token_vec.push(felt_to_blob(approval.token));
            owner_vec.push(felt_to_blob(approval.owner));
            operator_vec.push(felt_to_blob(approval.operator));
            approved_vec.push((approval.approved as u64).to_string());
            block_vec.push(approval.block_number.to_string());
            tx_hash_vec.push(felt_to_blob(approval.tx_hash));
            ts_vec.push(
                approval
                    .timestamp
                    .unwrap_or_else(|| chrono::Utc::now().timestamp())
                    .to_string(),
            );
        }

        let client = self.pg_client().await?;
        client
            .execute(
                "INSERT INTO erc721.nft_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
                SELECT DISTINCT ON (token, owner, operator) i.token, i.owner, i.operator, i.approved, i.block_number, i.tx_hash, i.timestamp
                FROM unnest(
                    $1::bytea[],
                    $2::bytea[],
                    $3::bytea[],
                    $4::text[],
                    $5::text[],
                    $6::bytea[],
                    $7::text[]
                ) WITH ORDINALITY AS i(token, owner, operator, approved, block_number, tx_hash, timestamp, ord)
                ORDER BY token, owner, operator, ord DESC
                ON CONFLICT (token, owner, operator) DO UPDATE SET
                    approved = EXCLUDED.approved,
                    block_number = EXCLUDED.block_number,
                    tx_hash = EXCLUDED.tx_hash,
                    timestamp = EXCLUDED.timestamp",
                &[
                    &token_vec,
                    &owner_vec,
                    &operator_vec,
                    &approved_vec,
                    &block_vec,
                    &tx_hash_vec,
                    &ts_vec,
                ],
            )
            .await?;
        Ok(approvals.len())
    }

    #[allow(clippy::too_many_arguments)]
    async fn pg_get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<NftTransferData>, Option<TransferCursor>)> {
        let client = self.pg_client().await?;
        let mut query = String::new();
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM erc721.nft_wallet_activity wa
                 JOIN erc721.nft_transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ",
            );
            query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(wallet_addr)));
            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND wa.token IN ({list})"));
            }
        } else {
            query.push_str(
                "SELECT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM erc721.nft_transfers t
                 WHERE 1=1",
            );
            if let Some(from_addr) = from {
                query.push_str(" AND t.from_addr = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(from_addr)));
            }
            if let Some(to_addr) = to {
                query.push_str(" AND t.to_addr = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(to_addr)));
            }
            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND t.token IN ({list})"));
            }
        }

        if !token_ids.is_empty() {
            let list = token_ids
                .iter()
                .map(|tid| Self::pg_next_param(&mut params, u256_to_blob(*tid)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND t.token_id IN ({list})"));
        }
        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ");
            query.push_str(&Self::pg_next_param(&mut params, block_min.to_string()));
        }
        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ");
            query.push_str(&Self::pg_next_param(&mut params, block_max.to_string()));
        }
        if let Some(c) = cursor {
            let p1 = Self::pg_next_param(&mut params, c.block_number.to_string());
            let p2 = Self::pg_next_param(&mut params, c.block_number.to_string());
            let p3 = Self::pg_next_param(&mut params, c.id);
            query.push_str(&format!(
                " AND (t.block_number < {p1} OR (t.block_number = {p2} AND t.id < {p3}))"
            ));
        }
        query.push_str(" ORDER BY t.block_number DESC, t.id DESC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, limit as i64));

        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let transfers: Vec<NftTransferData> = rows
            .into_iter()
            .map(|row| NftTransferData {
                id: Some(row.get::<usize, i64>(0)),
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                token_id: blob_to_u256(&row.get::<usize, Vec<u8>>(2)),
                from: blob_to_felt(&row.get::<usize, Vec<u8>>(3)),
                to: blob_to_felt(&row.get::<usize, Vec<u8>>(4)),
                block_number: row.get::<usize, String>(5).parse::<u64>().unwrap_or(0),
                tx_hash: blob_to_felt(&row.get::<usize, Vec<u8>>(6)),
                timestamp: row.get::<usize, String>(7).parse::<i64>().ok(),
            })
            .collect();
        let next_cursor = if transfers.len() == limit as usize {
            transfers.last().map(|t| TransferCursor {
                block_number: t.block_number,
                id: t.id.unwrap_or_default(),
            })
        } else {
            None
        };
        Ok((transfers, next_cursor))
    }

    async fn pg_get_owner(&self, token: Felt, token_id: U256) -> Result<Option<Felt>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT owner FROM erc721.nft_ownership WHERE token = $1 AND token_id = $2",
                &[&felt_to_blob(token), &u256_to_blob(token_id)],
            )
            .await?;
        Ok(row.map(|r| blob_to_felt(&r.get::<usize, Vec<u8>>(0))))
    }

    async fn pg_get_ownership_by_owner(
        &self,
        owner: Felt,
        tokens: &[Felt],
        cursor: Option<OwnershipCursor>,
        limit: u32,
    ) -> Result<(Vec<NftOwnershipData>, Option<OwnershipCursor>)> {
        let client = self.pg_client().await?;
        let mut query = String::from(
            "SELECT id, token, token_id, owner, block_number FROM erc721.nft_ownership WHERE owner = ",
        );
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();
        query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(owner)));
        if !tokens.is_empty() {
            let list = tokens
                .iter()
                .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND token IN ({list})"));
        }
        if let Some(c) = cursor {
            let p1 = Self::pg_next_param(&mut params, c.block_number.to_string());
            let p2 = Self::pg_next_param(&mut params, c.block_number.to_string());
            let p3 = Self::pg_next_param(&mut params, c.id);
            query.push_str(&format!(
                " AND (block_number < {p1} OR (block_number = {p2} AND id < {p3}))"
            ));
        }
        query.push_str(" ORDER BY block_number DESC, id DESC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, limit as i64));
        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let ownership: Vec<NftOwnershipData> = rows
            .into_iter()
            .map(|row| NftOwnershipData {
                id: Some(row.get::<usize, i64>(0)),
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                token_id: blob_to_u256(&row.get::<usize, Vec<u8>>(2)),
                owner: blob_to_felt(&row.get::<usize, Vec<u8>>(3)),
                block_number: row.get::<usize, String>(4).parse::<u64>().unwrap_or(0),
            })
            .collect();
        let next_cursor = if ownership.len() == limit as usize {
            ownership.last().map(|o| OwnershipCursor {
                block_number: o.block_number,
                id: o.id.unwrap_or_default(),
            })
        } else {
            None
        };
        Ok((ownership, next_cursor))
    }

    async fn pg_query_token_ids_by_facets(
        &self,
        token: Felt,
        resolved_filters: &[ResolvedFacetFilter],
        cursor_token_id: Option<U256>,
        page_fetch: i64,
        include_facets: bool,
        facet_limit: i64,
    ) -> Result<TokenAttributeQueryResult> {
        let client = self.pg_client().await?;
        let (candidate_query, candidate_params_for_page) =
            Self::pg_build_candidate_query(token, resolved_filters);
        let (_, candidate_params_for_count) =
            Self::pg_build_candidate_query(token, resolved_filters);

        let mut query = format!(
            "WITH candidate_tokens AS ({candidate_query}) SELECT token_id FROM candidate_tokens"
        );
        let mut params = candidate_params_for_page;
        if let Some(cursor) = cursor_token_id {
            query.push_str(" WHERE token_id > ");
            query.push_str(&Self::pg_next_param(&mut params, u256_to_blob(cursor)));
        }
        query.push_str(" ORDER BY token_id ASC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, page_fetch));

        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let mut token_ids: Vec<U256> = rows
            .into_iter()
            .map(|row| blob_to_u256(&row.get::<usize, Vec<u8>>(0)))
            .collect();

        let page_limit = (page_fetch - 1) as usize;
        let next_cursor_token_id = if token_ids.len() > page_limit {
            let next = token_ids[page_limit];
            token_ids.truncate(page_limit);
            Some(next)
        } else {
            None
        };

        let count_query = format!(
            "WITH candidate_tokens AS ({candidate_query}) SELECT COUNT(*) FROM candidate_tokens"
        );
        let count_refs: Vec<&(dyn PgToSql + Sync)> = candidate_params_for_count
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let total_row = client.query_one(&count_query, &count_refs).await?;
        let total_hits = total_row.get::<usize, i64>(0) as u64;

        let facets = if include_facets {
            let (_, candidate_params_for_facets) =
                Self::pg_build_candidate_query(token, resolved_filters);
            let mut facet_query = format!(
                "WITH candidate_tokens AS ({candidate_query})
                 SELECT fk.key_display, fv.value_display, COUNT(*) AS cnt
                 FROM erc721.facet_token_map m
                 JOIN candidate_tokens c ON c.token_id = m.token_id
                 JOIN erc721.facet_keys fk ON fk.id = m.facet_key_id
                 JOIN erc721.facet_values fv ON fv.id = m.facet_value_id
                 WHERE m.token = "
            );
            let mut facet_params = candidate_params_for_facets;
            facet_query.push_str(&Self::pg_next_param(&mut facet_params, felt_to_blob(token)));
            facet_query.push_str(
                " GROUP BY fk.key_display, fv.value_display
                  ORDER BY cnt DESC, fk.key_display ASC, fv.value_display ASC
                  LIMIT ",
            );
            facet_query.push_str(&Self::pg_next_param(&mut facet_params, facet_limit));
            let facet_refs: Vec<&(dyn PgToSql + Sync)> = facet_params
                .iter()
                .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
                .collect();
            let rows = client.query(&facet_query, &facet_refs).await?;
            rows.into_iter()
                .map(|row| AttributeFacetCount {
                    key: row.get(0),
                    value: row.get(1),
                    count: row.get::<usize, i64>(2) as u64,
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(TokenAttributeQueryResult {
            token_ids,
            next_cursor_token_id,
            total_hits,
            facets,
        })
    }

    async fn pg_get_transfer_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM erc721.nft_transfers", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_token_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(DISTINCT token) FROM erc721.nft_transfers",
                &[],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_nft_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM erc721.nft_ownership", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_latest_block(&self) -> Result<Option<u64>> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT MAX(block_number) FROM erc721.nft_transfers", &[])
            .await?;
        let v: Option<String> = row.get(0);
        Ok(v.and_then(|x| x.parse::<u64>().ok()))
    }

    async fn pg_has_token_metadata(&self, token: Felt) -> Result<bool> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc721.token_metadata
                 WHERE token = $1
                   AND name IS NOT NULL
                   AND BTRIM(name) <> ''
                   AND symbol IS NOT NULL
                   AND BTRIM(symbol) <> ''
                   AND total_supply IS NOT NULL",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) > 0)
    }

    async fn pg_has_token_metadata_batch(&self, tokens: &[Felt]) -> Result<HashSet<Felt>> {
        let client = self.pg_client().await?;
        let token_blobs: Vec<Vec<u8>> = tokens.iter().map(|token| felt_to_blob(*token)).collect();
        let rows = client
            .query(
                "SELECT token FROM erc721.token_metadata
                 WHERE token = ANY($1::bytea[])
                   AND name IS NOT NULL
                   AND BTRIM(name) <> ''
                   AND symbol IS NOT NULL
                   AND BTRIM(symbol) <> ''
                   AND total_supply IS NOT NULL",
                &[&token_blobs],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| blob_to_felt(&row.get::<usize, Vec<u8>>(0)))
            .collect())
    }

    async fn pg_upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        let client = self.pg_client().await?;
        client.execute(
            "INSERT INTO erc721.token_metadata (token, name, symbol, total_supply)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (token) DO UPDATE SET
                 name = COALESCE(EXCLUDED.name, erc721.token_metadata.name),
                 symbol = COALESCE(EXCLUDED.symbol, erc721.token_metadata.symbol),
                 total_supply = COALESCE(EXCLUDED.total_supply, erc721.token_metadata.total_supply)",
            &[&felt_to_blob(token), &name, &symbol, &total_supply.map(u256_to_blob)],
        ).await?;
        Ok(())
    }

    async fn pg_get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<U256>)>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT name, symbol, total_supply FROM erc721.token_metadata WHERE token = $1",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.map(|r| {
            let supply: Option<Vec<u8>> = r.get(2);
            (r.get(0), r.get(1), supply.map(|b| blob_to_u256(&b)))
        }))
    }

    async fn pg_get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(Felt, Option<String>, Option<String>, Option<U256>)>,
        Option<Felt>,
    )> {
        let client = self.pg_client().await?;
        let fetch_limit = limit.clamp(1, 1000) as i64 + 1;
        let rows = if let Some(cursor_token) = cursor {
            client
                .query(
                    "SELECT token, name, symbol, total_supply
                 FROM erc721.token_metadata
                 WHERE token > $1
                 ORDER BY token ASC
                 LIMIT $2",
                    &[&felt_to_blob(cursor_token), &fetch_limit],
                )
                .await?
        } else {
            client
                .query(
                    "SELECT token, name, symbol, total_supply
                 FROM erc721.token_metadata
                 ORDER BY token ASC
                 LIMIT $1",
                    &[&fetch_limit],
                )
                .await?
        };
        let mut out: Vec<(Felt, Option<String>, Option<String>, Option<U256>)> = rows
            .into_iter()
            .map(|row| {
                let supply: Option<Vec<u8>> = row.get(3);
                (
                    blob_to_felt(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                    supply.map(|b| blob_to_u256(&b)),
                )
            })
            .collect();
        let capped = limit.clamp(1, 1000) as usize;
        let next_cursor = if out.len() > capped {
            let next = out[capped].0;
            out.truncate(capped);
            Some(next)
        } else {
            None
        };
        Ok((out, next_cursor))
    }

    async fn pg_has_token_uri(&self, token: Felt, token_id: U256) -> Result<bool> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc721.token_uris
                 WHERE token = $1
                   AND token_id = $2",
                &[&felt_to_blob(token), &u256_to_blob(token_id)],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) > 0)
    }

    async fn pg_has_token_uri_batch(
        &self,
        tokens: &[(Felt, U256)],
    ) -> Result<HashSet<(Felt, U256)>> {
        let client = self.pg_client().await?;
        let token_blobs: Vec<Vec<u8>> = tokens
            .iter()
            .map(|(token, _)| felt_to_blob(*token))
            .collect();
        let token_id_blobs: Vec<Vec<u8>> = tokens
            .iter()
            .map(|(_, token_id)| u256_to_blob(*token_id))
            .collect();
        let rows = client
            .query(
                "SELECT DISTINCT u.token, u.token_id
                 FROM erc721.token_uris u
                 JOIN unnest($1::bytea[], $2::bytea[]) AS i(token, token_id)
                   ON u.token = i.token AND u.token_id = i.token_id",
                &[&token_blobs, &token_id_blobs],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    blob_to_felt(&row.get::<usize, Vec<u8>>(0)),
                    blob_to_u256(&row.get::<usize, Vec<u8>>(1)),
                )
            })
            .collect())
    }

    async fn pg_get_token_uris_by_contract(
        &self,
        token: Felt,
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        let client = self.pg_client().await?;
        let rows = client
            .query(
                "SELECT token_id, uri, metadata_json
             FROM erc721.token_uris
             WHERE token = $1
             ORDER BY token_id ASC",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    blob_to_u256(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                )
            })
            .collect())
    }

    async fn pg_get_token_uris_batch(
        &self,
        token: Felt,
        token_ids: &[U256],
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        let client = self.pg_client().await?;
        let token_id_blobs: Vec<Vec<u8>> = token_ids
            .iter()
            .map(|token_id| u256_to_blob(*token_id))
            .collect();
        let rows = client
            .query(
                "SELECT token_id, uri, metadata_json
                 FROM erc721.token_uris
                 WHERE token = $1 AND token_id = ANY($2::bytea[])",
                &[&felt_to_blob(token), &token_id_blobs],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    blob_to_u256(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                )
            })
            .collect())
    }
}

fn db_pool_url(db_path: &str) -> String {
    if db_path == ":memory:" || db_path == "sqlite::memory:" {
        return "sqlite::memory:".to_owned();
    }

    if db_path.starts_with("postgres://")
        || db_path.starts_with("postgresql://")
        || db_path.starts_with("sqlite:")
    {
        return db_path.to_owned();
    }

    format!("sqlite://{db_path}?mode=rwc")
}

#[async_trait::async_trait]
impl TokenUriStore for Erc721Storage {
    async fn store_token_uris_batch(&self, results: &[TokenUriResult]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let expected_attributes = results
            .iter()
            .map(|result| extract_metadata_attributes(result.metadata_json.as_deref()))
            .collect::<Vec<_>>();
        let token_blobs = results
            .iter()
            .map(|result| result.contract.to_be_bytes_vec())
            .collect::<Vec<_>>();
        let token_id_blobs = results
            .iter()
            .map(|result| result.token_id.to_big_endian().to_vec())
            .collect::<Vec<_>>();

        if self.is_postgres() {
            let mut client = self.pg_client().await?;
            let mut changed_indexes = Vec::new();
            for (idx, result) in results.iter().enumerate() {
                if !pg_token_uri_state_matches(&client, result, &expected_attributes[idx]).await? {
                    changed_indexes.push(idx);
                }
            }

            if changed_indexes.is_empty() {
                let facet_inputs = results
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| FacetSyncInput {
                        token_blob: token_blobs[idx].clone(),
                        token_id_blob: token_id_blobs[idx].clone(),
                        attributes: &expected_attributes[idx],
                    })
                    .collect::<Vec<_>>();
                let tx = client.transaction().await?;
                pg_sync_facets_for_tokens(&tx, &facet_inputs).await?;
                tx.commit().await?;
                return Ok(());
            }

            let tx = client.transaction().await?;

            for idx in changed_indexes {
                let result = &results[idx];
                let attrs = &expected_attributes[idx];
                let token_blob = &token_blobs[idx];
                let token_id_blob = &token_id_blobs[idx];

                tx.execute(
                    "INSERT INTO erc721.token_uris (token, token_id, uri, metadata_json, updated_at)
                     VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT::TEXT)
                     ON CONFLICT(token, token_id) DO UPDATE SET
                        uri = EXCLUDED.uri,
                        metadata_json = EXCLUDED.metadata_json,
                        updated_at = EXCLUDED.updated_at",
                    &[
                        &token_blob,
                        &token_id_blob,
                        &result.uri.as_deref(),
                        &result.metadata_json.as_deref(),
                    ],
                )
                .await?;

                tx.execute(
                    "DELETE FROM erc721.token_attributes WHERE token = $1 AND token_id = $2",
                    &[&token_blob, &token_id_blob],
                )
                .await?;

                for (key, value) in attrs {
                    tx.execute(
                        "INSERT INTO erc721.token_attributes (token, token_id, key, value)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (token, token_id, key) DO UPDATE SET value = EXCLUDED.value",
                        &[&token_blob, &token_id_blob, key, value],
                    )
                    .await?;
                }
            }

            let facet_inputs = results
                .iter()
                .enumerate()
                .map(|(idx, _)| FacetSyncInput {
                    token_blob: token_blobs[idx].clone(),
                    token_id_blob: token_id_blobs[idx].clone(),
                    attributes: &expected_attributes[idx],
                })
                .collect::<Vec<_>>();
            pg_sync_facets_for_tokens(&tx, &facet_inputs).await?;
            tx.commit().await?;
            return Ok(());
        }

        let mut conn = self.sqlite_conn()?;
        let mut changed_indexes = Vec::new();
        for (idx, result) in results.iter().enumerate() {
            if !sqlite_token_uri_state_matches(&conn, result, &expected_attributes[idx])? {
                changed_indexes.push(idx);
            }
        }

        if changed_indexes.is_empty() {
            let facet_inputs = results
                .iter()
                .enumerate()
                .map(|(idx, _)| FacetSyncInput {
                    token_blob: token_blobs[idx].clone(),
                    token_id_blob: token_id_blobs[idx].clone(),
                    attributes: &expected_attributes[idx],
                })
                .collect::<Vec<_>>();
            let tx = conn.transaction()?;
            sqlite_sync_facets_for_tokens(&tx, &facet_inputs)?;
            tx.commit()?;
            return Ok(());
        }

        let tx = conn.transaction()?;
        {
            let mut upsert_stmt = tx.prepare_cached(
                "INSERT INTO token_uris (token, token_id, uri, metadata_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, strftime('%s', 'now'))
                 ON CONFLICT(token, token_id) DO UPDATE SET
                    uri = excluded.uri,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at",
            )?;
            let mut delete_attrs_stmt = tx.prepare_cached(
                "DELETE FROM token_attributes WHERE token = ?1 AND token_id = ?2",
            )?;
            let mut insert_attr_stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO token_attributes (token, token_id, key, value)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;

            for idx in changed_indexes {
                let result = &results[idx];
                let attrs = &expected_attributes[idx];
                let token_blob = &token_blobs[idx];
                let token_id_blob = &token_id_blobs[idx];

                upsert_stmt.execute(params![
                    token_blob,
                    token_id_blob,
                    result.uri.as_deref(),
                    result.metadata_json.as_deref()
                ])?;
                delete_attrs_stmt.execute(params![token_blob, token_id_blob])?;

                for (key, value) in attrs {
                    insert_attr_stmt.execute(params![token_blob, token_id_blob, key, value])?;
                }
            }
        }

        let facet_inputs = results
            .iter()
            .enumerate()
            .map(|(idx, _)| FacetSyncInput {
                token_blob: token_blobs[idx].clone(),
                token_id_blob: token_id_blobs[idx].clone(),
                attributes: &expected_attributes[idx],
            })
            .collect::<Vec<_>>();
        sqlite_sync_facets_for_tokens(&tx, &facet_inputs)?;
        tx.commit()?;
        Ok(())
    }
}

fn extract_metadata_attributes(metadata_json: Option<&str>) -> Vec<(String, String)> {
    let Some(metadata_json) = metadata_json else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata_json) else {
        return Vec::new();
    };
    let Some(attrs) = value.get("attributes").and_then(|attrs| attrs.as_array()) else {
        return Vec::new();
    };

    let mut normalized = BTreeMap::new();
    for attr in attrs {
        let key = attr
            .get("trait_type")
            .or_else(|| attr.get("key"))
            .and_then(|value| value.as_str())
            .and_then(sanitize_metadata_text);
        let value = attr
            .get("value")
            .and_then(|value| {
                value.as_str().map(ToOwned::to_owned).or_else(|| {
                    if value.is_null() {
                        None
                    } else {
                        Some(value.to_string())
                    }
                })
            })
            .and_then(|value| sanitize_metadata_text(&value));

        if let (Some(key), Some(value)) = (key, value) {
            normalized.insert(key, value);
        }
    }

    normalized.into_iter().collect()
}

type TokenFacetKey = (Vec<u8>, Vec<u8>);
type FacetAssignmentMap = BTreeMap<i64, i64>;

struct FacetSyncInput<'a> {
    token_blob: Vec<u8>,
    token_id_blob: Vec<u8>,
    attributes: &'a [(String, String)],
}

#[derive(Default)]
struct FacetSyncPlan {
    deletes: Vec<(Vec<u8>, Vec<u8>, i64)>,
    inserts: Vec<(Vec<u8>, Vec<u8>, i64, i64)>,
    count_deltas: HashMap<i64, i64>,
}

fn build_facet_sync_plan(
    entries: &[FacetSyncInput<'_>],
    existing: &HashMap<TokenFacetKey, FacetAssignmentMap>,
    desired: &HashMap<TokenFacetKey, FacetAssignmentMap>,
) -> FacetSyncPlan {
    let mut plan = FacetSyncPlan::default();

    for entry in entries {
        let token_key = (entry.token_blob.clone(), entry.token_id_blob.clone());
        let existing_map = existing.get(&token_key).cloned().unwrap_or_default();
        let desired_map = desired.get(&token_key).cloned().unwrap_or_default();

        if existing_map == desired_map {
            continue;
        }

        for (facet_key_id, existing_value_id) in &existing_map {
            if desired_map.get(facet_key_id) != Some(existing_value_id) {
                plan.deletes.push((
                    entry.token_blob.clone(),
                    entry.token_id_blob.clone(),
                    *facet_key_id,
                ));
                *plan.count_deltas.entry(*existing_value_id).or_insert(0) -= 1;
            }
        }

        for (facet_key_id, desired_value_id) in &desired_map {
            if existing_map.get(facet_key_id) != Some(desired_value_id) {
                plan.inserts.push((
                    entry.token_blob.clone(),
                    entry.token_id_blob.clone(),
                    *facet_key_id,
                    *desired_value_id,
                ));
                *plan.count_deltas.entry(*desired_value_id).or_insert(0) += 1;
            }
        }
    }

    plan.count_deltas.retain(|_, delta| *delta != 0);
    plan
}

fn sqlite_sync_facets_for_tokens(
    tx: &rusqlite::Transaction<'_>,
    entries: &[FacetSyncInput<'_>],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let existing = sqlite_load_existing_facet_assignments(tx, entries)?;
    let desired = sqlite_resolve_desired_facet_assignments(tx, entries)?;
    let plan = build_facet_sync_plan(entries, &existing, &desired);
    sqlite_apply_facet_sync_plan(tx, &plan)
}

fn sqlite_load_existing_facet_assignments(
    tx: &rusqlite::Transaction<'_>,
    entries: &[FacetSyncInput<'_>],
) -> Result<HashMap<TokenFacetKey, FacetAssignmentMap>> {
    let mut assignments: HashMap<TokenFacetKey, FacetAssignmentMap> = HashMap::new();

    for chunk in entries.chunks(SQLITE_TOKEN_PAIR_BATCH_SIZE) {
        let conditions = vec!["(token = ? AND token_id = ?)"; chunk.len()].join(" OR ");
        let query = format!(
            "SELECT token, token_id, facet_key_id, facet_value_id
             FROM facet_token_map
             WHERE {conditions}"
        );

        let mut params: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 2);
        for entry in chunk {
            params.push(&entry.token_blob);
            params.push(&entry.token_id_blob);
        }

        let mut stmt = tx.prepare_cached(&query)?;
        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok((
                row.get::<usize, Vec<u8>>(0)?,
                row.get::<usize, Vec<u8>>(1)?,
                row.get::<usize, i64>(2)?,
                row.get::<usize, i64>(3)?,
            ))
        })?;

        for row in rows {
            let (token_blob, token_id_blob, facet_key_id, facet_value_id) = row?;
            assignments
                .entry((token_blob, token_id_blob))
                .or_default()
                .insert(facet_key_id, facet_value_id);
        }
    }

    Ok(assignments)
}

fn sqlite_resolve_desired_facet_assignments(
    tx: &rusqlite::Transaction<'_>,
    entries: &[FacetSyncInput<'_>],
) -> Result<HashMap<TokenFacetKey, FacetAssignmentMap>> {
    let mut desired: HashMap<TokenFacetKey, FacetAssignmentMap> = HashMap::new();
    let mut key_cache: HashMap<(Vec<u8>, String), i64> = HashMap::new();
    let mut value_cache: HashMap<(Vec<u8>, i64, String), i64> = HashMap::new();

    let mut insert_key_stmt = tx.prepare_cached(
        "INSERT INTO facet_keys (token, key_norm, key_display)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(token, key_norm) DO UPDATE SET key_display = excluded.key_display",
    )?;
    let mut select_key_stmt =
        tx.prepare_cached("SELECT id FROM facet_keys WHERE token = ?1 AND key_norm = ?2")?;
    let mut insert_value_stmt = tx.prepare_cached(
        "INSERT INTO facet_values (token, facet_key_id, value_norm, value_display)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(token, facet_key_id, value_norm) DO UPDATE SET value_display = excluded.value_display",
    )?;
    let mut select_value_stmt = tx.prepare_cached(
        "SELECT id FROM facet_values WHERE token = ?1 AND facet_key_id = ?2 AND value_norm = ?3",
    )?;

    for entry in entries {
        let mut assignments = BTreeMap::new();

        for (key, value) in entry.attributes {
            let key_cache_key = (entry.token_blob.clone(), key.clone());
            let facet_key_id = if let Some(facet_key_id) = key_cache.get(&key_cache_key) {
                *facet_key_id
            } else {
                insert_key_stmt.execute(params![&entry.token_blob, key, key])?;
                let facet_key_id = select_key_stmt
                    .query_row(params![&entry.token_blob, key], |row| {
                        row.get::<usize, i64>(0)
                    })?;
                key_cache.insert(key_cache_key, facet_key_id);
                facet_key_id
            };

            let value_cache_key = (entry.token_blob.clone(), facet_key_id, value.clone());
            let facet_value_id = if let Some(facet_value_id) = value_cache.get(&value_cache_key) {
                *facet_value_id
            } else {
                insert_value_stmt.execute(params![
                    &entry.token_blob,
                    facet_key_id,
                    value,
                    value
                ])?;
                let facet_value_id = select_value_stmt
                    .query_row(params![&entry.token_blob, facet_key_id, value], |row| {
                        row.get::<usize, i64>(0)
                    })?;
                value_cache.insert(value_cache_key, facet_value_id);
                facet_value_id
            };

            assignments.insert(facet_key_id, facet_value_id);
        }

        desired.insert(
            (entry.token_blob.clone(), entry.token_id_blob.clone()),
            assignments,
        );
    }

    Ok(desired)
}

fn sqlite_apply_facet_sync_plan(
    tx: &rusqlite::Transaction<'_>,
    plan: &FacetSyncPlan,
) -> Result<()> {
    if plan.deletes.is_empty() && plan.inserts.is_empty() && plan.count_deltas.is_empty() {
        return Ok(());
    }

    let mut delete_stmt = tx.prepare_cached(
        "DELETE FROM facet_token_map WHERE token = ?1 AND token_id = ?2 AND facet_key_id = ?3",
    )?;
    let mut insert_stmt = tx.prepare_cached(
        "INSERT OR REPLACE INTO facet_token_map (token, token_id, facet_key_id, facet_value_id)
         VALUES (?1, ?2, ?3, ?4)",
    )?;
    let mut update_count_stmt = tx.prepare_cached(
        "UPDATE facet_values
         SET token_count = CAST(MAX(CAST(token_count AS INTEGER) + ?2, 0) AS TEXT)
         WHERE id = ?1",
    )?;

    for (token_blob, token_id_blob, facet_key_id) in &plan.deletes {
        delete_stmt.execute(params![token_blob, token_id_blob, facet_key_id])?;
    }

    for (token_blob, token_id_blob, facet_key_id, facet_value_id) in &plan.inserts {
        insert_stmt.execute(params![
            token_blob,
            token_id_blob,
            facet_key_id,
            facet_value_id
        ])?;
    }

    for (facet_value_id, delta) in &plan.count_deltas {
        update_count_stmt.execute(params![facet_value_id, delta])?;
    }

    Ok(())
}

async fn pg_sync_facets_for_tokens(
    tx: &tokio_postgres::Transaction<'_>,
    entries: &[FacetSyncInput<'_>],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let existing = pg_load_existing_facet_assignments(tx, entries).await?;
    let desired = pg_resolve_desired_facet_assignments(tx, entries).await?;
    let plan = build_facet_sync_plan(entries, &existing, &desired);
    pg_apply_facet_sync_plan(tx, &plan).await
}

async fn pg_load_existing_facet_assignments(
    tx: &tokio_postgres::Transaction<'_>,
    entries: &[FacetSyncInput<'_>],
) -> Result<HashMap<TokenFacetKey, FacetAssignmentMap>> {
    let mut assignments: HashMap<TokenFacetKey, FacetAssignmentMap> = HashMap::new();

    for chunk in entries.chunks(SQLITE_TOKEN_PAIR_BATCH_SIZE) {
        let mut query = String::from(
            "SELECT token, token_id, facet_key_id, facet_value_id
             FROM erc721.facet_token_map
             WHERE ",
        );
        let mut params: Vec<&(dyn PgToSql + Sync)> = Vec::with_capacity(chunk.len() * 2);

        for (idx, entry) in chunk.iter().enumerate() {
            if idx > 0 {
                query.push_str(" OR ");
            }
            let token_param = params.len() + 1;
            let token_id_param = params.len() + 2;
            query.push_str(&format!(
                "(token = ${token_param} AND token_id = ${token_id_param})"
            ));
            params.push(&entry.token_blob);
            params.push(&entry.token_id_blob);
        }

        let rows = tx.query(&query, &params).await?;
        for row in rows {
            let token_blob: Vec<u8> = row.get(0);
            let token_id_blob: Vec<u8> = row.get(1);
            let facet_key_id: i64 = row.get(2);
            let facet_value_id: i64 = row.get(3);
            assignments
                .entry((token_blob, token_id_blob))
                .or_default()
                .insert(facet_key_id, facet_value_id);
        }
    }

    Ok(assignments)
}

async fn pg_resolve_desired_facet_assignments(
    tx: &tokio_postgres::Transaction<'_>,
    entries: &[FacetSyncInput<'_>],
) -> Result<HashMap<TokenFacetKey, FacetAssignmentMap>> {
    let mut desired: HashMap<TokenFacetKey, FacetAssignmentMap> = HashMap::new();
    let mut key_cache: HashMap<(Vec<u8>, String), i64> = HashMap::new();
    let mut value_cache: HashMap<(Vec<u8>, i64, String), i64> = HashMap::new();

    for entry in entries {
        let mut assignments = BTreeMap::new();

        for (key, value) in entry.attributes {
            let key_cache_key = (entry.token_blob.clone(), key.clone());
            let facet_key_id = if let Some(facet_key_id) = key_cache.get(&key_cache_key) {
                *facet_key_id
            } else {
                tx.execute(
                    "INSERT INTO erc721.facet_keys (token, key_norm, key_display)
                     VALUES ($1, $2, $3)
                     ON CONFLICT(token, key_norm) DO UPDATE SET key_display = EXCLUDED.key_display",
                    &[&entry.token_blob, key, key],
                )
                .await?;
                let facet_key_id = tx
                    .query_one(
                        "SELECT id FROM erc721.facet_keys WHERE token = $1 AND key_norm = $2",
                        &[&entry.token_blob, key],
                    )
                    .await?
                    .get(0);
                key_cache.insert(key_cache_key, facet_key_id);
                facet_key_id
            };

            let value_cache_key = (entry.token_blob.clone(), facet_key_id, value.clone());
            let facet_value_id = if let Some(facet_value_id) = value_cache.get(&value_cache_key) {
                *facet_value_id
            } else {
                tx.execute(
                    "INSERT INTO erc721.facet_values (token, facet_key_id, value_norm, value_display)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT(token, facet_key_id, value_norm) DO UPDATE SET value_display = EXCLUDED.value_display",
                    &[&entry.token_blob, &facet_key_id, value, value],
                )
                .await?;
                let facet_value_id = tx
                    .query_one(
                        "SELECT id FROM erc721.facet_values WHERE token = $1 AND facet_key_id = $2 AND value_norm = $3",
                        &[&entry.token_blob, &facet_key_id, value],
                    )
                    .await?
                    .get(0);
                value_cache.insert(value_cache_key, facet_value_id);
                facet_value_id
            };

            assignments.insert(facet_key_id, facet_value_id);
        }

        desired.insert(
            (entry.token_blob.clone(), entry.token_id_blob.clone()),
            assignments,
        );
    }

    Ok(desired)
}

async fn pg_apply_facet_sync_plan(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &FacetSyncPlan,
) -> Result<()> {
    if plan.deletes.is_empty() && plan.inserts.is_empty() && plan.count_deltas.is_empty() {
        return Ok(());
    }

    for (token_blob, token_id_blob, facet_key_id) in &plan.deletes {
        tx.execute(
            "DELETE FROM erc721.facet_token_map WHERE token = $1 AND token_id = $2 AND facet_key_id = $3",
            &[token_blob, token_id_blob, facet_key_id],
        )
        .await?;
    }

    for (token_blob, token_id_blob, facet_key_id, facet_value_id) in &plan.inserts {
        tx.execute(
            "INSERT INTO erc721.facet_token_map (token, token_id, facet_key_id, facet_value_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(token, token_id, facet_key_id) DO UPDATE SET facet_value_id = EXCLUDED.facet_value_id",
            &[token_blob, token_id_blob, facet_key_id, facet_value_id],
        )
        .await?;
    }

    for (facet_value_id, delta) in &plan.count_deltas {
        tx.execute(
            "UPDATE erc721.facet_values
             SET token_count = CAST(GREATEST(CAST(token_count AS BIGINT) + $2, 0) AS TEXT)
             WHERE id = $1",
            &[facet_value_id, delta],
        )
        .await?;
    }

    Ok(())
}

fn sqlite_token_uri_state_matches(
    conn: &Connection,
    result: &TokenUriResult,
    expected_attributes: &[(String, String)],
) -> Result<bool> {
    let token_blob = result.contract.to_be_bytes_vec();
    let token_id_blob = result.token_id.to_big_endian().to_vec();
    let row = conn.query_row(
        "SELECT uri, metadata_json FROM token_uris WHERE token = ?1 AND token_id = ?2",
        params![&token_blob, &token_id_blob],
        |row| {
            Ok((
                row.get::<usize, Option<String>>(0)?,
                row.get::<usize, Option<String>>(1)?,
            ))
        },
    );
    let (existing_uri, existing_metadata_json) = match row {
        Ok(row) => row,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(false),
        Err(error) => return Err(error.into()),
    };

    if existing_uri.as_deref() != result.uri.as_deref()
        || existing_metadata_json.as_deref() != result.metadata_json.as_deref()
    {
        return Ok(false);
    }

    let mut stmt = conn.prepare_cached(
        "SELECT key, value FROM token_attributes
         WHERE token = ?1 AND token_id = ?2
         ORDER BY key ASC, value ASC",
    )?;
    let rows = stmt.query_map(params![&token_blob, &token_id_blob], |row| {
        Ok((row.get::<usize, String>(0)?, row.get::<usize, String>(1)?))
    })?;
    let existing_attributes = rows.collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(existing_attributes == expected_attributes)
}

async fn pg_token_uri_state_matches(
    client: &Client,
    result: &TokenUriResult,
    expected_attributes: &[(String, String)],
) -> Result<bool> {
    let token_blob = result.contract.to_be_bytes_vec();
    let token_id_blob = result.token_id.to_big_endian().to_vec();
    let row = client
        .query_opt(
            "SELECT uri, metadata_json FROM erc721.token_uris WHERE token = $1 AND token_id = $2",
            &[&token_blob, &token_id_blob],
        )
        .await?;
    let Some(row) = row else {
        return Ok(false);
    };

    let existing_uri: Option<String> = row.get(0);
    let existing_metadata_json: Option<String> = row.get(1);
    if existing_uri.as_deref() != result.uri.as_deref()
        || existing_metadata_json.as_deref() != result.metadata_json.as_deref()
    {
        return Ok(false);
    }

    let rows = client
        .query(
            "SELECT key, value FROM erc721.token_attributes
             WHERE token = $1 AND token_id = $2
             ORDER BY key ASC, value ASC",
            &[&token_blob, &token_id_blob],
        )
        .await?;
    let existing_attributes: Vec<(String, String)> = rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    Ok(existing_attributes == expected_attributes)
}

fn sanitize_metadata_text(input: &str) -> Option<String> {
    let sanitized = input.replace('\0', "").trim().to_owned();
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_contract() -> starknet_types_raw::Felt {
        starknet_types_raw::Felt::from_hex_unchecked(
            "0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4",
        )
    }

    fn primitive_token_id(value: u64) -> primitive_types::U256 {
        primitive_types::U256::from(value)
    }

    fn temp_db_path(test_name: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("torii-erc721-{test_name}-{nanos}.db"))
            .to_string_lossy()
            .to_string()
    }

    #[tokio::test]
    async fn has_token_metadata_requires_complete_erc721_row() {
        let db_path = temp_db_path("complete-metadata");
        let storage = Erc721Storage::new(&db_path).await.expect("create storage");
        let token = Felt::from_hex_unchecked(
            "0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4",
        );

        storage
            .upsert_token_metadata(token, None, None, Some(U256::from(301u64)))
            .await
            .expect("insert partial metadata");
        assert!(!storage
            .has_token_metadata(token)
            .await
            .expect("check partial metadata"));

        storage
            .upsert_token_metadata(token, Some("Beasts"), Some("BEAST"), None)
            .await
            .expect("complete metadata");
        assert!(storage
            .has_token_metadata(token)
            .await
            .expect("check complete metadata"));

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn store_token_uri_skips_unchanged_rewrite() {
        let db_path = temp_db_path("token-uri-noop");
        let storage = Erc721Storage::new(&db_path).await.expect("create storage");
        let result = TokenUriResult {
            contract: raw_contract(),
            token_id: primitive_token_id(1),
            uri: Some("ipfs://beasts/1".to_owned()),
            metadata_json: Some(
                r#"{"name":"Beast #1","attributes":[{"trait_type":"Class","value":"Wolf"}]}"#
                    .to_owned(),
            ),
        };

        storage
            .store_token_uri(&result)
            .await
            .expect("insert token uri");

        storage
            .with_sqlite_conn(|conn| {
                conn.execute(
                    "UPDATE token_uris SET updated_at = '123' WHERE token = ?1 AND token_id = ?2",
                    params![
                        result.contract.to_be_bytes_vec(),
                        result.token_id.to_big_endian().to_vec()
                    ],
                )?;
                Ok(())
            })
            .expect("set sentinel updated_at");

        storage
            .store_token_uri(&result)
            .await
            .expect("store unchanged token uri");

        let (updated_at, attr_count): (String, i64) = storage
            .with_sqlite_conn(|conn| {
                let updated_at: String = conn.query_row(
                    "SELECT updated_at FROM token_uris WHERE token = ?1 AND token_id = ?2",
                    params![
                        result.contract.to_be_bytes_vec(),
                        result.token_id.to_big_endian().to_vec()
                    ],
                    |row| row.get(0),
                )?;
                let attr_count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM token_attributes WHERE token = ?1 AND token_id = ?2",
                    params![
                        result.contract.to_be_bytes_vec(),
                        result.token_id.to_big_endian().to_vec()
                    ],
                    |row| row.get(0),
                )?;
                Ok((updated_at, attr_count))
            })
            .expect("read sqlite assertions");

        assert_eq!(updated_at, "123");
        assert_eq!(attr_count, 1);

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn store_token_uri_repairs_missing_attributes_for_same_metadata() {
        let db_path = temp_db_path("token-uri-backfill");
        let storage = Erc721Storage::new(&db_path).await.expect("create storage");
        let result = TokenUriResult {
            contract: raw_contract(),
            token_id: primitive_token_id(2),
            uri: Some("ipfs://beasts/2".to_owned()),
            metadata_json: Some(
                r#"{"name":"Beast #2","attributes":[{"trait_type":"Class","value":"Bear"}]}"#
                    .to_owned(),
            ),
        };

        storage
            .store_token_uri(&result)
            .await
            .expect("insert token uri");

        storage
            .with_sqlite_conn(|conn| {
                conn.execute(
                    "DELETE FROM token_attributes WHERE token = ?1 AND token_id = ?2",
                    params![
                        result.contract.to_be_bytes_vec(),
                        result.token_id.to_big_endian().to_vec()
                    ],
                )?;
                Ok(())
            })
            .expect("delete attributes");

        storage
            .store_token_uri(&result)
            .await
            .expect("repair missing attributes");

        let attr_count: i64 = storage
            .with_sqlite_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM token_attributes WHERE token = ?1 AND token_id = ?2",
                    params![
                        result.contract.to_be_bytes_vec(),
                        result.token_id.to_big_endian().to_vec()
                    ],
                    |row| row.get(0),
                )
            })
            .expect("count attributes");

        assert_eq!(attr_count, 1);

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn store_token_uri_repairs_missing_facets_for_same_metadata() {
        let db_path = temp_db_path("token-uri-facet-backfill");
        let storage = Erc721Storage::new(&db_path).await.expect("create storage");
        let result = TokenUriResult {
            contract: raw_contract(),
            token_id: primitive_token_id(3),
            uri: Some("ipfs://beasts/3".to_owned()),
            metadata_json: Some(
                r#"{"name":"Beast #3","attributes":[{"trait_type":"Class","value":"Wolf"},{"trait_type":"Color","value":"Red"}]}"#
                    .to_owned(),
            ),
        };

        storage
            .store_token_uri(&result)
            .await
            .expect("insert token uri");

        storage
            .with_sqlite_conn(|conn| {
                conn.execute(
                    "UPDATE facet_values SET token_count = '0' WHERE id IN (
                        SELECT facet_value_id FROM facet_token_map WHERE token = ?1 AND token_id = ?2
                    )",
                    params![result.contract.to_be_bytes_vec(), result.token_id.to_big_endian().to_vec()],
                )?;
                conn.execute(
                    "DELETE FROM facet_token_map WHERE token = ?1 AND token_id = ?2",
                    params![result.contract.to_be_bytes_vec(), result.token_id.to_big_endian().to_vec()],
                )?;
                Ok(())
            })
            .expect("delete facet mappings");

        storage
            .store_token_uri(&result)
            .await
            .expect("repair missing facets");

        let (facet_map_count, positive_counts): (i64, i64) = storage
            .with_sqlite_conn(|conn| {
                let facet_map_count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM facet_token_map WHERE token = ?1 AND token_id = ?2",
                    params![
                        result.contract.to_be_bytes_vec(),
                        result.token_id.to_big_endian().to_vec()
                    ],
                    |row| row.get(0),
                )?;
                let positive_counts: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM facet_values WHERE CAST(token_count AS INTEGER) > 0",
                    [],
                    |row| row.get(0),
                )?;
                Ok((facet_map_count, positive_counts))
            })
            .expect("read facet assertions");

        assert_eq!(facet_map_count, 2);
        assert_eq!(positive_counts, 2);

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn query_token_ids_by_attributes_uses_facet_index() {
        let db_path = temp_db_path("facet-query");
        let storage = Erc721Storage::new(&db_path).await.expect("create storage");
        let contract = Felt::from_hex_unchecked(
            "0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4",
        );

        let fixtures = [
            (
                1u64,
                r#"{"attributes":[{"trait_type":"Color","value":"Red"},{"trait_type":"Class","value":"Wolf"}]}"#,
            ),
            (
                2u64,
                r#"{"attributes":[{"trait_type":"Color","value":"Blue"},{"trait_type":"Class","value":"Wolf"}]}"#,
            ),
            (
                3u64,
                r#"{"attributes":[{"trait_type":"Color","value":"Red"},{"trait_type":"Class","value":"Bear"}]}"#,
            ),
        ];

        for (token_id, metadata_json) in fixtures {
            storage
                .store_token_uri(&TokenUriResult {
                    contract: contract.into(),
                    token_id: primitive_types::U256::from(token_id),
                    uri: Some(format!("ipfs://beasts/{token_id}")),
                    metadata_json: Some(metadata_json.to_owned()),
                })
                .await
                .expect("store token uri fixture");
        }

        let result = storage
            .query_token_ids_by_attributes(
                contract,
                &[
                    (
                        "Color".to_owned(),
                        vec!["Red".to_owned(), "Blue".to_owned()],
                    ),
                    ("Class".to_owned(), vec!["Wolf".to_owned()]),
                ],
                None,
                10,
                true,
                10,
            )
            .await
            .expect("query by attributes");

        assert_eq!(result.token_ids, vec![U256::from(1u64), U256::from(2u64)]);
        assert_eq!(result.total_hits, 2);
        assert!(result
            .facets
            .iter()
            .any(|facet| { facet.key == "Color" && facet.value == "Red" && facet.count == 1 }));
        assert!(result
            .facets
            .iter()
            .any(|facet| { facet.key == "Color" && facet.value == "Blue" && facet.count == 1 }));
        assert!(result
            .facets
            .iter()
            .any(|facet| { facet.key == "Class" && facet.value == "Wolf" && facet.count == 2 }));

        let _ = std::fs::remove_file(db_path);
    }
}
