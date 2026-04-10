//! SQLite storage for ERC20 transfers, approvals, and balances
//!
//! Uses binary (BLOB) storage for efficiency (~51% size reduction vs hex strings).
//! Uses U256 for amounts (proper 256-bit arithmetic for ERC20 token amounts).
//!
//! Balance tracking uses a "fetch-on-inconsistency" approach:
//! - Tracks balances computed from transfer events
//! - When a balance would go negative (indicating missed history like genesis allocations),
//!   fetches the actual balance from the chain and adjusts
//! - Records all adjustments in an audit table for debugging

use anyhow::Result;
use primitive_types::U256;
use rusqlite::{params, params_from_iter, Connection, ToSql};
use starknet_types_raw::Felt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_postgres::types::ToSql as PgToSql;
use tokio_postgres::{Client, NoTls};
use torii_common::{blob_to_u256, u256_to_blob};

use crate::balance_fetcher::BalanceFetchRequest;

/// Maximum value for U256 (2^256 - 1)
const SQLITE_MAX_BIND_VARS: usize = 900;
const SQLITE_TOKEN_WALLET_QUERY_CHUNK: usize = SQLITE_MAX_BIND_VARS - 1;
const SQLITE_ACTIVITY_INSERT_CHUNK: usize = SQLITE_MAX_BIND_VARS / 5;
const SQLITE_BALANCE_UPSERT_CHUNK: usize = SQLITE_MAX_BIND_VARS / 5;
const SQLITE_ADJUSTMENT_INSERT_CHUNK: usize = SQLITE_MAX_BIND_VARS / 6;
const DEFAULT_BALANCE_CACHE_CAPACITY: usize = 300_000;

#[derive(Debug)]
struct BalanceCacheState {
    enabled: bool,
    capacity: usize,
    generation: u64,
    values: HashMap<(Felt, Felt), U256>,
    generations: HashMap<(Felt, Felt), u64>,
    order: VecDeque<((Felt, Felt), u64)>,
}

impl BalanceCacheState {
    fn new(enabled: bool, capacity: usize) -> Self {
        Self {
            enabled,
            capacity,
            generation: 0,
            values: HashMap::new(),
            generations: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &(Felt, Felt)) -> Option<U256> {
        if !self.enabled {
            return None;
        }

        let value = self.values.get(key).copied()?;
        self.generation = self.generation.wrapping_add(1);
        self.generations.insert(*key, self.generation);
        self.order.push_back((*key, self.generation));
        Some(value)
    }

    fn put(&mut self, key: (Felt, Felt), value: U256) {
        if !self.enabled {
            return;
        }

        self.generation = self.generation.wrapping_add(1);
        self.values.insert(key, value);
        self.generations.insert(key, self.generation);
        self.order.push_back((key, self.generation));
        self.evict_if_needed();
    }

    fn put_many(&mut self, rows: &HashMap<(Felt, Felt), U256>) {
        if !self.enabled || rows.is_empty() {
            return;
        }

        for (key, value) in rows {
            self.put(*key, *value);
        }
    }

    fn evict_if_needed(&mut self) {
        if !self.enabled {
            return;
        }

        while self.values.len() > self.capacity {
            let Some((key, generation)) = self.order.pop_front() else {
                break;
            };
            let latest = self.generations.get(&key).copied();
            if latest == Some(generation) {
                self.values.remove(&key);
                self.generations.remove(&key);
            }
        }
    }

    fn size(&self) -> usize {
        self.values.len()
    }
}

struct ActivityInsertRow {
    account: Vec<u8>,
    token: Vec<u8>,
    ref_id: i64,
    role: &'static str,
    block_number: String,
}

struct BalanceUpsertRow {
    token: Vec<u8>,
    wallet: Vec<u8>,
    balance: Vec<u8>,
    last_block: String,
    last_tx_hash: Vec<u8>,
}

struct AdjustmentInsertRow {
    token: Vec<u8>,
    wallet: Vec<u8>,
    computed_balance: Vec<u8>,
    actual_balance: Vec<u8>,
    adjusted_at_block: String,
    tx_hash: Vec<u8>,
}

/// Safely adds two U256 values, capping at U256::MAX on overflow.
///
/// This is necessary because starknet-core's U256::Add uses checked_add().unwrap(),
/// which panics on overflow. For token balances, overflow should be extremely rare
/// (would require a balance > 2^256 - 1), but we handle it gracefully by capping
/// at the maximum value rather than crashing the indexer.
///
/// Possible causes of overflow:
/// - Malicious or buggy contract minting excessive tokens
/// - Data corruption in blockchain event data
/// - Accumulation of many transfers to the same address
fn safe_u256_add(a: U256, b: U256) -> U256 {
    // Check if addition would overflow
    // If a > U256::MAX - b, then a + b would overflow
    let max_minus_b = U256::MAX - b;
    if a > max_minus_b {
        tracing::warn!(
            "U256 addition overflow detected: {} + {} would exceed U256::MAX, capping at maximum",
            a,
            b
        );
        U256::MAX
    } else {
        a + b
    }
}

/// Direction filter for transfer queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransferDirection {
    /// All transfers (sent and received)
    #[default]
    All,
    /// Only sent transfers (wallet is sender)
    Sent,
    /// Only received transfers (wallet is receiver)
    Received,
}

/// Transfer data for batch insertion
pub struct TransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Approval data for batch insertion
pub struct ApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub owner: Felt,
    pub spender: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
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

/// Cursor for paginated approval queries
#[derive(Debug, Clone, Copy)]
pub struct ApprovalCursor {
    pub block_number: u64,
    pub id: i64,
}

/// Balance data for a token/wallet pair
#[derive(Debug, Clone)]
pub struct BalanceData {
    pub token: Felt,
    pub wallet: Felt,
    pub balance: U256,
    pub last_block: u64,
    pub last_tx_hash: Felt,
}

/// Balance adjustment record for audit trail
#[derive(Debug, Clone)]
pub struct BalanceAdjustment {
    pub token: Felt,
    pub wallet: Felt,
    /// What we computed from transfers
    pub computed_balance: U256,
    /// What the RPC returned as actual balance
    pub actual_balance: U256,
    /// Block at which adjustment was made
    pub adjusted_at_block: u64,
    pub tx_hash: Felt,
}

/// Result of a batch balance consistency check.
#[derive(Debug, Clone)]
pub struct BalanceCheckBatch {
    pub adjustment_requests: Vec<BalanceFetchRequest>,
    pub balance_snapshot: HashMap<(Felt, Felt), U256>,
}

impl Erc20Storage {
    fn build_balance_cache() -> Arc<Mutex<BalanceCacheState>> {
        let enabled = std::env::var("TORII_ERC20_BALANCE_CACHE_ENABLED")
            .ok()
            .is_none_or(|v| {
                let normalized = v.trim().to_ascii_lowercase();
                !matches!(normalized.as_str(), "0" | "false" | "no" | "off")
            });
        let capacity = std::env::var("TORII_ERC20_BALANCE_CACHE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_BALANCE_CACHE_CAPACITY);

        tracing::info!(
            target: "torii_erc20::storage",
            enabled,
            capacity,
            "ERC20 balance cache configured"
        );

        Arc::new(Mutex::new(BalanceCacheState::new(enabled, capacity)))
    }

    fn get_cached_balances(
        &self,
        pairs: &[(Felt, Felt)],
    ) -> (HashMap<(Felt, Felt), U256>, Vec<(Felt, Felt)>) {
        let mut cache = self.balance_cache.lock().unwrap();
        let mut hits = HashMap::new();
        let mut misses = Vec::new();

        for pair in pairs {
            if let Some(balance) = cache.get(pair) {
                hits.insert(*pair, balance);
            } else {
                misses.push(*pair);
            }
        }

        if !pairs.is_empty() {
            ::metrics::counter!("torii_erc20_balance_cache_hits_total")
                .increment(hits.len() as u64);
            ::metrics::counter!("torii_erc20_balance_cache_misses_total")
                .increment(misses.len() as u64);
            ::metrics::gauge!("torii_erc20_balance_cache_size").set(cache.size() as f64);
        }

        (hits, misses)
    }

    fn store_cached_balances(&self, rows: &HashMap<(Felt, Felt), U256>) {
        if rows.is_empty() {
            return;
        }

        let mut cache = self.balance_cache.lock().unwrap();
        cache.put_many(rows);
        ::metrics::gauge!("torii_erc20_balance_cache_size").set(cache.size() as f64);
    }

    /// Create or open the database
    pub async fn new(db_path: &str) -> Result<Self> {
        let balance_cache = Self::build_balance_cache();
        if db_path.starts_with("postgres://") || db_path.starts_with("postgresql://") {
            let pool_size = std::env::var("TORII_ERC20_PG_POOL_SIZE")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(8)
                .max(1);

            let mut pg_conns = Vec::with_capacity(pool_size);

            for _ in 0..pool_size {
                let (client, connection) = tokio_postgres::connect(db_path, NoTls).await?;
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        tracing::error!(target: "torii_erc20::storage", error = %e, "PostgreSQL connection task failed");
                    }
                });
                pg_conns.push(Arc::new(tokio::sync::Mutex::new(client)));
            }

            let schema_client = pg_conns
                .first()
                .expect("PostgreSQL connection pool must contain at least one client")
                .clone();
            let client = schema_client.lock().await;
            client
                .batch_execute(
                    r"
                CREATE SCHEMA IF NOT EXISTS erc20;

                CREATE TABLE IF NOT EXISTS erc20.transfers (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    from_addr BYTEA NOT NULL,
                    to_addr BYTEA NOT NULL,
                    amount BYTEA NOT NULL,
                    block_number TEXT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp TEXT DEFAULT (EXTRACT(EPOCH FROM NOW())::TEXT),
                    UNIQUE(token, tx_hash, from_addr, to_addr)
                );
                CREATE INDEX IF NOT EXISTS idx_transfers_token ON erc20.transfers(token);
                CREATE INDEX IF NOT EXISTS idx_transfers_from ON erc20.transfers(from_addr);
                CREATE INDEX IF NOT EXISTS idx_transfers_to ON erc20.transfers(to_addr);
                CREATE INDEX IF NOT EXISTS idx_transfers_block ON erc20.transfers(block_number);
                CREATE INDEX IF NOT EXISTS idx_transfers_token_block ON erc20.transfers(token, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_transfers_from_block ON erc20.transfers(from_addr, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_transfers_to_block ON erc20.transfers(to_addr, block_number DESC);

                CREATE TABLE IF NOT EXISTS erc20.approvals (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    spender BYTEA NOT NULL,
                    amount BYTEA NOT NULL,
                    block_number TEXT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp TEXT DEFAULT (EXTRACT(EPOCH FROM NOW())::TEXT),
                    UNIQUE(token, tx_hash, owner, spender)
                );
                CREATE INDEX IF NOT EXISTS idx_approvals_owner ON erc20.approvals(owner, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_approvals_spender ON erc20.approvals(spender, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_approvals_token ON erc20.approvals(token, block_number DESC);

                CREATE TABLE IF NOT EXISTS erc20.wallet_activity (
                    id BIGSERIAL PRIMARY KEY,
                    wallet_address BYTEA NOT NULL,
                    token BYTEA NOT NULL,
                    transfer_id BIGINT NOT NULL REFERENCES erc20.transfers(id),
                    direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                    block_number TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_block ON erc20.wallet_activity(wallet_address, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_token ON erc20.wallet_activity(wallet_address, token, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_wallet_activity_transfer ON erc20.wallet_activity(transfer_id);

                CREATE TABLE IF NOT EXISTS erc20.approval_activity (
                    id BIGSERIAL PRIMARY KEY,
                    account_address BYTEA NOT NULL,
                    token BYTEA NOT NULL,
                    approval_id BIGINT NOT NULL REFERENCES erc20.approvals(id),
                    role TEXT NOT NULL CHECK(role IN ('owner', 'spender', 'both')),
                    block_number TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_approval_activity_account_block ON erc20.approval_activity(account_address, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_approval_activity_account_token ON erc20.approval_activity(account_address, token, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_approval_activity_approval ON erc20.approval_activity(approval_id);

                CREATE TABLE IF NOT EXISTS erc20.balances (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    wallet BYTEA NOT NULL,
                    balance BYTEA NOT NULL,
                    last_block TEXT NOT NULL,
                    last_tx_hash BYTEA NOT NULL,
                    updated_at TEXT DEFAULT (EXTRACT(EPOCH FROM NOW())::TEXT),
                    UNIQUE(token, wallet)
                );
                CREATE INDEX IF NOT EXISTS idx_balances_token ON erc20.balances(token);
                CREATE INDEX IF NOT EXISTS idx_balances_wallet ON erc20.balances(wallet);

                CREATE TABLE IF NOT EXISTS erc20.balance_adjustments (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    wallet BYTEA NOT NULL,
                    computed_balance BYTEA NOT NULL,
                    actual_balance BYTEA NOT NULL,
                    adjusted_at_block TEXT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    created_at TEXT DEFAULT (EXTRACT(EPOCH FROM NOW())::TEXT)
                );
                CREATE INDEX IF NOT EXISTS idx_adjustments_wallet ON erc20.balance_adjustments(wallet);
                CREATE INDEX IF NOT EXISTS idx_adjustments_token ON erc20.balance_adjustments(token);

                CREATE TABLE IF NOT EXISTS erc20.token_metadata (
                    token BYTEA PRIMARY KEY,
                    name TEXT,
                    symbol TEXT,
                    decimals TEXT,
                    total_supply BYTEA
                );
                ",
                )
                .await?;

            tracing::info!(target: "torii_erc20::storage", pool_size, "PostgreSQL storage initialized");
            return Ok(Self {
                backend: StorageBackend::Postgres,
                conn: Arc::new(Mutex::new(Connection::open_in_memory()?)),
                balance_cache,
                pg_conns: Some(pg_conns),
                pg_rr: AtomicUsize::new(0),
            });
        }

        let conn = Connection::open(db_path)?;

        let cache_size_kb = std::env::var("TORII_ERC20_SQLITE_CACHE_SIZE_KB")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(64_000);
        let mmap_size = std::env::var("TORII_ERC20_SQLITE_MMAP_SIZE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v >= 0)
            .unwrap_or(268_435_456);
        let wal_autocheckpoint = std::env::var("TORII_ERC20_SQLITE_WAL_AUTOCHECKPOINT")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(10_000);
        let busy_timeout_ms = std::env::var("TORII_ERC20_SQLITE_BUSY_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(5_000);
        let synchronous = std::env::var("TORII_ERC20_SQLITE_SYNCHRONOUS")
            .ok()
            .map(|v| v.to_ascii_uppercase())
            .filter(|v| matches!(v.as_str(), "OFF" | "NORMAL" | "FULL" | "EXTRA"))
            .unwrap_or_else(|| "NORMAL".to_string());

        // Enable WAL mode + Performance PRAGMAs (critical for production scale)
        conn.execute_batch(&format!(
            "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous={synchronous};
                 PRAGMA foreign_keys=ON;
                 PRAGMA cache_size={};
                 PRAGMA temp_store=MEMORY;
                 PRAGMA mmap_size={mmap_size};
                 PRAGMA wal_autocheckpoint={wal_autocheckpoint};
                 PRAGMA page_size=4096;
                 PRAGMA busy_timeout={busy_timeout_ms};",
            -cache_size_kb
        ))?;

        tracing::info!(
            target: "torii_erc20::storage",
            cache_size_kb,
            mmap_size,
            wal_autocheckpoint,
            busy_timeout_ms,
            synchronous = %synchronous,
            "SQLite configured"
        );

        // Create transfers table with BLOB columns for efficient storage
        conn.execute(
            "CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                amount BLOB NOT NULL,
                block_number TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp TEXT DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, tx_hash, from_addr, to_addr)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_token ON transfers(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block_number)",
            [],
        )?;

        // Composite indexes for efficient queries
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_token_block ON transfers(token, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_from_block ON transfers(from_addr, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_to_block ON transfers(to_addr, block_number DESC)",
            [],
        )?;

        // Create approvals table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                owner BLOB NOT NULL,
                spender BLOB NOT NULL,
                amount BLOB NOT NULL,
                block_number TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp TEXT DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, tx_hash, owner, spender)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_owner ON approvals(owner, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_spender ON approvals(spender, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_token ON approvals(token, block_number DESC)",
            [],
        )?;

        // Metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        // Wallet activity table - denormalized for efficient wallet queries
        // Solves the OR query problem: "get all transfers where wallet is sender OR receiver"
        // Instead of: WHERE from_addr = ? OR to_addr = ? (can only use one index)
        // We use: JOIN wallet_activity WHERE wallet_address = ? (uses this table's index)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number TEXT NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES transfers(id)
            )",
            [],
        )?;

        // Compound index: wallet -> block (optimal for wallet activity queries)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_block
             ON wallet_activity(wallet_address, block_number DESC)",
            [],
        )?;

        // Compound index: wallet -> token -> block (token-specific activity)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_token
             ON wallet_activity(wallet_address, token, block_number DESC)",
            [],
        )?;

        // Index for reverse lookup (transfer -> wallets)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_transfer
             ON wallet_activity(transfer_id)",
            [],
        )?;

        // Approval activity table - similar pattern for approvals
        conn.execute(
            "CREATE TABLE IF NOT EXISTS approval_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_address BLOB NOT NULL,
                token BLOB NOT NULL,
                approval_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('owner', 'spender', 'both')),
                block_number TEXT NOT NULL,
                FOREIGN KEY (approval_id) REFERENCES approvals(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_account_block
             ON approval_activity(account_address, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_account_token
             ON approval_activity(account_address, token, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_approval
             ON approval_activity(approval_id)",
            [],
        )?;

        // Balance tracking tables
        // Tracks current balance per (token, wallet) pair
        conn.execute(
            "CREATE TABLE IF NOT EXISTS balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL,
                last_tx_hash BLOB NOT NULL,
                updated_at TEXT DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, wallet)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_token ON balances(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_wallet ON balances(wallet)",
            [],
        )?;

        // Balance adjustments table for audit trail
        // Records when we had to fetch balance from RPC due to inconsistency
        conn.execute(
            "CREATE TABLE IF NOT EXISTS balance_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                computed_balance BLOB NOT NULL,
                actual_balance BLOB NOT NULL,
                adjusted_at_block TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                created_at TEXT DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adjustments_wallet ON balance_adjustments(wallet)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adjustments_token ON balance_adjustments(token)",
            [],
        )?;

        // Token metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_metadata (
                token BLOB PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                decimals TEXT,
                total_supply BLOB
            )",
            [],
        )?;

        tracing::info!(target: "torii_erc20::storage", db_path = %db_path, "Database initialized");

        Ok(Self {
            backend: StorageBackend::Sqlite,
            conn: Arc::new(Mutex::new(conn)),
            balance_cache,
            pg_conns: None,
            pg_rr: AtomicUsize::new(0),
        })
    }

    fn sqlite_select_balances_for_pairs(
        conn: &Connection,
        pairs: &[(Felt, Felt)],
    ) -> Result<HashMap<(Felt, Felt), U256>> {
        let mut out = HashMap::new();
        if pairs.is_empty() {
            return Ok(out);
        }

        let mut wallets_by_token: HashMap<Felt, Vec<Felt>> = HashMap::new();
        for (token, wallet) in pairs {
            wallets_by_token.entry(*token).or_default().push(*wallet);
        }

        for (token, wallets) in wallets_by_token {
            let token_blob = felt_to_blob(token);
            for chunk in wallets.chunks(SQLITE_TOKEN_WALLET_QUERY_CHUNK) {
                let placeholders = std::iter::repeat_n("?", chunk.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "SELECT wallet, balance FROM balances WHERE token = ? AND wallet IN ({placeholders})"
                );

                let mut wallet_blobs = Vec::with_capacity(chunk.len());
                for wallet in chunk {
                    wallet_blobs.push(felt_to_blob(*wallet));
                }

                let mut params: Vec<&dyn ToSql> = Vec::with_capacity(1 + wallet_blobs.len());
                params.push(&token_blob);
                for wallet in &wallet_blobs {
                    params.push(wallet as &dyn ToSql);
                }

                let mut stmt = conn.prepare_cached(&sql)?;
                let rows = stmt.query_map(params_from_iter(params), |row| {
                    let wallet: Vec<u8> = row.get(0)?;
                    let balance: Vec<u8> = row.get(1)?;
                    Ok(((token, blob_to_felt(&wallet)), blob_to_u256(&balance)))
                })?;

                for row in rows {
                    let (key, balance) = row?;
                    out.insert(key, balance);
                }
            }
        }

        Ok(out)
    }

    fn sqlite_load_balances_for_pairs(
        &self,
        pairs: &[(Felt, Felt)],
    ) -> Result<HashMap<(Felt, Felt), U256>> {
        if pairs.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.conn.lock().unwrap();
        Self::sqlite_select_balances_for_pairs(&conn, pairs)
    }

    fn sqlite_insert_activity_rows(
        tx: &rusqlite::Transaction<'_>,
        table: &str,
        account_column: &str,
        id_column: &str,
        role_column: &str,
        rows: &[ActivityInsertRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        for chunk in rows.chunks(SQLITE_ACTIVITY_INSERT_CHUNK) {
            let placeholders = std::iter::repeat_n("(?, ?, ?, ?, ?)", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO {table} ({account_column}, token, {id_column}, {role_column}, block_number) \
                 VALUES {placeholders}"
            );

            let mut params: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 5);
            for row in chunk {
                params.push(&row.account);
                params.push(&row.token);
                params.push(&row.ref_id);
                params.push(&row.role);
                params.push(&row.block_number);
            }

            tx.execute(&sql, params_from_iter(params))?;
        }

        Ok(())
    }

    fn sqlite_upsert_balances_rows(
        tx: &rusqlite::Transaction<'_>,
        rows: &[BalanceUpsertRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        for chunk in rows.chunks(SQLITE_BALANCE_UPSERT_CHUNK) {
            let placeholders = std::iter::repeat_n("(?, ?, ?, ?, ?)", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO balances (token, wallet, balance, last_block, last_tx_hash) \
                 VALUES {placeholders} \
                 ON CONFLICT(token, wallet) DO UPDATE SET \
                    balance = excluded.balance, \
                    last_block = excluded.last_block, \
                    last_tx_hash = excluded.last_tx_hash, \
                    updated_at = strftime('%s', 'now') \
                 WHERE balances.balance != excluded.balance \
                    OR balances.last_block != excluded.last_block \
                    OR balances.last_tx_hash != excluded.last_tx_hash"
            );

            let mut params: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 5);
            for row in chunk {
                params.push(&row.token);
                params.push(&row.wallet);
                params.push(&row.balance);
                params.push(&row.last_block);
                params.push(&row.last_tx_hash);
            }

            tx.execute(&sql, params_from_iter(params))?;
        }

        Ok(())
    }

    fn sqlite_insert_adjustment_rows(
        tx: &rusqlite::Transaction<'_>,
        rows: &[AdjustmentInsertRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        for chunk in rows.chunks(SQLITE_ADJUSTMENT_INSERT_CHUNK) {
            let placeholders = std::iter::repeat_n("(?, ?, ?, ?, ?, ?)", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO balance_adjustments \
                 (token, wallet, computed_balance, actual_balance, adjusted_at_block, tx_hash) \
                 VALUES {placeholders}"
            );

            let mut params: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 6);
            for row in chunk {
                params.push(&row.token);
                params.push(&row.wallet);
                params.push(&row.computed_balance);
                params.push(&row.actual_balance);
                params.push(&row.adjusted_at_block);
                params.push(&row.tx_hash);
            }

            tx.execute(&sql, params_from_iter(params))?;
        }

        Ok(())
    }

    /// Insert multiple transfers in a single transaction
    ///
    /// This is significantly faster than inserting one by one because:
    /// - Single lock acquisition
    /// - Single transaction (all-or-nothing commit)
    /// - Prepared statement reuse
    pub async fn insert_transfers_batch(&self, transfers: &[TransferData]) -> Result<usize> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_insert_transfers_batch(transfers).await;
        }
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut transfer_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO transfers (token, from_addr, to_addr, amount, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;
            let mut wallet_activity_rows = Vec::with_capacity(transfers.len() * 2);

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let amount_blob = u256_to_blob(transfer.amount);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                let rows = transfer_stmt.execute(params![
                    &token_blob,
                    &from_blob,
                    &to_blob,
                    &amount_blob,
                    transfer.block_number.to_string(),
                    &tx_hash_blob,
                    transfer.timestamp.map(|t| t.to_string()),
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();
                    let block_number = transfer.block_number.to_string();

                    // Insert wallet activity records
                    // This enables O(log n) wallet queries instead of O(n) OR scans
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        wallet_activity_rows.push(ActivityInsertRow {
                            account: from_blob,
                            token: token_blob,
                            ref_id: transfer_id,
                            role: "both",
                            block_number,
                        });
                    } else {
                        // Sender record
                        if transfer.from != Felt::ZERO {
                            wallet_activity_rows.push(ActivityInsertRow {
                                account: from_blob,
                                token: token_blob.clone(),
                                ref_id: transfer_id,
                                role: "sent",
                                block_number: block_number.clone(),
                            });
                        }
                        // Receiver record
                        if transfer.to != Felt::ZERO {
                            wallet_activity_rows.push(ActivityInsertRow {
                                account: to_blob,
                                token: token_blob,
                                ref_id: transfer_id,
                                role: "received",
                                block_number,
                            });
                        }
                    }
                }
            }

            Self::sqlite_insert_activity_rows(
                &tx,
                "wallet_activity",
                "wallet_address",
                "transfer_id",
                "direction",
                &wallet_activity_rows,
            )?;
        }

        tx.commit()?;

        Ok(inserted)
    }

    /// Insert multiple approvals in a single transaction
    pub async fn insert_approvals_batch(&self, approvals: &[ApprovalData]) -> Result<usize> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_insert_approvals_batch(approvals).await;
        }
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut approval_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO approvals (token, owner, spender, amount, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;
            let mut approval_activity_rows = Vec::with_capacity(approvals.len() * 2);

            for approval in approvals {
                let token_blob = felt_to_blob(approval.token);
                let owner_blob = felt_to_blob(approval.owner);
                let spender_blob = felt_to_blob(approval.spender);
                let amount_blob = u256_to_blob(approval.amount);
                let tx_hash_blob = felt_to_blob(approval.tx_hash);

                let rows = approval_stmt.execute(params![
                    &token_blob,
                    &owner_blob,
                    &spender_blob,
                    &amount_blob,
                    approval.block_number.to_string(),
                    &tx_hash_blob,
                    approval.timestamp.map(|t| t.to_string()),
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let approval_id = tx.last_insert_rowid();
                    let block_number = approval.block_number.to_string();

                    // Insert approval activity records
                    if approval.owner != Felt::ZERO
                        && approval.spender != Felt::ZERO
                        && approval.owner == approval.spender
                    {
                        approval_activity_rows.push(ActivityInsertRow {
                            account: owner_blob,
                            token: token_blob,
                            ref_id: approval_id,
                            role: "both",
                            block_number,
                        });
                    } else {
                        // Owner record
                        if approval.owner != Felt::ZERO {
                            approval_activity_rows.push(ActivityInsertRow {
                                account: owner_blob,
                                token: token_blob.clone(),
                                ref_id: approval_id,
                                role: "owner",
                                block_number: block_number.clone(),
                            });
                        }
                        // Spender record
                        if approval.spender != Felt::ZERO {
                            approval_activity_rows.push(ActivityInsertRow {
                                account: spender_blob,
                                token: token_blob,
                                ref_id: approval_id,
                                role: "spender",
                                block_number,
                            });
                        }
                    }
                }
            }

            Self::sqlite_insert_activity_rows(
                &tx,
                "approval_activity",
                "account_address",
                "approval_id",
                "role",
                &approval_activity_rows,
            )?;
        }

        tx.commit()?;

        Ok(inserted)
    }

    /// Get filtered transfers with cursor-based pagination
    ///
    /// Supports:
    /// - `wallet`: Matches from OR to (uses wallet_activity table for efficient OR queries)
    /// - `from`: Exact from address match
    /// - `to`: Exact to address match
    /// - `tokens`: Token whitelist (empty = all tokens)
    /// - `direction`: Filter by sent/received/all (only applies with wallet)
    /// - `block_from`/`block_to`: Block range filter
    /// - `cursor`: Cursor from previous page
    /// - `limit`: Maximum results
    pub async fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        direction: TransferDirection,
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TransferData>, Option<TransferCursor>)> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_get_transfers_filtered(
                    wallet, from, to, tokens, direction, block_from, block_to, cursor, limit,
                )
                .await;
        }
        let conn = self.conn.lock().unwrap();

        // Build dynamic query based on filters
        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            // Use wallet_activity table for efficient OR queries
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM wallet_activity wa
                 JOIN transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));

            // Apply direction filter
            match direction {
                TransferDirection::All => {}
                TransferDirection::Sent => {
                    query.push_str(" AND wa.direction IN ('sent', 'both')");
                }
                TransferDirection::Received => {
                    query.push_str(" AND wa.direction IN ('received', 'both')");
                }
            }

            // Token filter on wallet_activity for efficiency
            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND wa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            // Standard query without wallet optimization
            query.push_str(
                "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM transfers t
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

        // Block range filters
        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ?");
            params_vec.push(Box::new(block_min.to_string()));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ?");
            params_vec.push(Box::new(block_max.to_string()));
        }

        // Cursor-based pagination
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
            let from_bytes: Vec<u8> = row.get(2)?;
            let to_bytes: Vec<u8> = row.get(3)?;
            let amount_bytes: Vec<u8> = row.get(4)?;
            let block_number_str: String = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp_str: Option<String> = row.get(7)?;

            Ok(TransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                amount: blob_to_u256(&amount_bytes),
                block_number: block_number_str.parse::<u64>().unwrap_or(0),
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp: timestamp_str.and_then(|s| s.parse::<i64>().ok()),
            })
        })?;

        let transfers: Vec<TransferData> = rows.collect::<Result<_, _>>()?;

        // Compute next cursor if there might be more pages
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

    /// Get filtered approvals with cursor-based pagination
    ///
    /// Supports:
    /// - `account`: Matches owner OR spender (uses approval_activity table)
    /// - `owner`: Exact owner address match
    /// - `spender`: Exact spender address match
    /// - `tokens`: Token whitelist (empty = all tokens)
    /// - `block_from`/`block_to`: Block range filter
    /// - `cursor`: Cursor from previous page
    /// - `limit`: Maximum results
    pub async fn get_approvals_filtered(
        &self,
        account: Option<Felt>,
        owner: Option<Felt>,
        spender: Option<Felt>,
        tokens: &[Felt],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<ApprovalCursor>,
        limit: u32,
    ) -> Result<(Vec<ApprovalData>, Option<ApprovalCursor>)> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_get_approvals_filtered(
                    account, owner, spender, tokens, block_from, block_to, cursor, limit,
                )
                .await;
        }
        let conn = self.conn.lock().unwrap();

        // Build dynamic query based on filters
        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(account_addr) = account {
            // Use approval_activity table for efficient OR queries
            query.push_str(
                "SELECT DISTINCT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM approval_activity aa
                 JOIN approvals a ON aa.approval_id = a.id
                 WHERE aa.account_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(account_addr)));

            // Token filter on approval_activity for efficiency
            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND aa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            // Standard query without account optimization
            query.push_str(
                "SELECT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM approvals a
                 WHERE 1=1",
            );

            if let Some(owner_addr) = owner {
                query.push_str(" AND a.owner = ?");
                params_vec.push(Box::new(felt_to_blob(owner_addr)));
            }

            if let Some(spender_addr) = spender {
                query.push_str(" AND a.spender = ?");
                params_vec.push(Box::new(felt_to_blob(spender_addr)));
            }

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND a.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        }

        // Block range filters
        if let Some(block_min) = block_from {
            query.push_str(" AND a.block_number >= ?");
            params_vec.push(Box::new(block_min.to_string()));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND a.block_number <= ?");
            params_vec.push(Box::new(block_max.to_string()));
        }

        // Cursor-based pagination
        if let Some(c) = cursor {
            query.push_str(" AND (a.block_number < ? OR (a.block_number = ? AND a.id < ?))");
            params_vec.push(Box::new(c.block_number.to_string()));
            params_vec.push(Box::new(c.block_number.to_string()));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY a.block_number DESC, a.id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare_cached(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let owner_bytes: Vec<u8> = row.get(2)?;
            let spender_bytes: Vec<u8> = row.get(3)?;
            let amount_bytes: Vec<u8> = row.get(4)?;
            let block_number_str: String = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp_str: Option<String> = row.get(7)?;

            Ok(ApprovalData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                owner: blob_to_felt(&owner_bytes),
                spender: blob_to_felt(&spender_bytes),
                amount: blob_to_u256(&amount_bytes),
                block_number: block_number_str.parse::<u64>().unwrap_or(0),
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp: timestamp_str.and_then(|s| s.parse::<i64>().ok()),
            })
        })?;

        let approvals: Vec<ApprovalData> = rows.collect::<Result<_, _>>()?;

        // Compute next cursor if there might be more pages
        let next_cursor = if approvals.len() == limit as usize {
            approvals.last().map(|a| ApprovalCursor {
                block_number: a.block_number,
                id: a.id.unwrap(),
            })
        } else {
            None
        };

        Ok((approvals, next_cursor))
    }

    /// Get transfer count
    pub async fn get_transfer_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_transfer_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get approval count
    pub async fn get_approval_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_approval_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM approvals", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get indexed token count
    pub async fn get_token_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(DISTINCT token) FROM transfers", [], |row| {
                row.get(0)
            })?;
        Ok(count as u64)
    }

    /// Get latest block number indexed
    pub async fn get_latest_block(&self) -> Result<Option<u64>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_latest_block().await;
        }
        let conn = self.conn.lock().unwrap();
        let block: Option<String> = conn
            .query_row("SELECT MAX(block_number) FROM transfers", [], |row| {
                row.get(0)
            })
            .ok()
            .flatten();
        Ok(block.and_then(|b| b.parse::<u64>().ok()))
    }

    // ===== Balance Tracking Methods =====

    /// Get current balance for a wallet/token pair
    pub async fn get_balance(&self, token: Felt, wallet: Felt) -> Result<Option<U256>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_balance(token, wallet).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let wallet_blob = felt_to_blob(wallet);

        let result: Option<Vec<u8>> = conn
            .query_row(
                "SELECT balance FROM balances WHERE token = ? AND wallet = ?",
                params![&token_blob, &wallet_blob],
                |row| row.get(0),
            )
            .ok();

        Ok(result.map(|bytes| blob_to_u256(&bytes)))
    }

    /// Get balance with last block info for a wallet/token pair
    pub async fn get_balance_with_block(
        &self,
        token: Felt,
        wallet: Felt,
    ) -> Result<Option<(U256, u64)>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_balance_with_block(token, wallet).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let wallet_blob = felt_to_blob(wallet);

        let result: Option<(Vec<u8>, String)> = conn
            .query_row(
                "SELECT balance, last_block FROM balances WHERE token = ? AND wallet = ?",
                params![&token_blob, &wallet_blob],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        Ok(result.map(|(bytes, block_str)| {
            (blob_to_u256(&bytes), block_str.parse::<u64>().unwrap_or(0))
        }))
    }

    /// Get balances with optional token/wallet filters and cursor pagination.
    ///
    /// Pagination is cursor-based on the `balances.id` primary key in ascending order.
    /// Returns `(rows, next_cursor)`.
    pub async fn get_balances_filtered(
        &self,
        token: Option<Felt>,
        wallet: Option<Felt>,
        cursor: Option<i64>,
        limit: u32,
    ) -> Result<(Vec<BalanceData>, Option<i64>)> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_get_balances_filtered(token, wallet, cursor, limit)
                .await;
        }
        let conn = self.conn.lock().unwrap();

        let mut query = String::from(
            "SELECT id, token, wallet, balance, last_block, last_tx_hash
             FROM balances
             WHERE 1=1",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(token_addr) = token {
            query.push_str(" AND token = ?");
            params_vec.push(Box::new(felt_to_blob(token_addr)));
        }

        if let Some(wallet_addr) = wallet {
            query.push_str(" AND wallet = ?");
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));
        }

        if let Some(c) = cursor {
            query.push_str(" AND id > ?");
            params_vec.push(Box::new(c));
        }

        query.push_str(" ORDER BY id ASC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare_cached(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let wallet_bytes: Vec<u8> = row.get(2)?;
            let balance_bytes: Vec<u8> = row.get(3)?;
            let last_block_str: String = row.get(4)?;
            let last_tx_hash_bytes: Vec<u8> = row.get(5)?;

            Ok((
                id,
                BalanceData {
                    token: blob_to_felt(&token_bytes),
                    wallet: blob_to_felt(&wallet_bytes),
                    balance: blob_to_u256(&balance_bytes),
                    last_block: last_block_str.parse::<u64>().unwrap_or(0),
                    last_tx_hash: blob_to_felt(&last_tx_hash_bytes),
                },
            ))
        })?;

        let mut out: Vec<BalanceData> = Vec::new();
        let mut last_id: Option<i64> = None;

        for row in rows {
            let (id, data) = row?;
            last_id = Some(id);
            out.push(data);
        }

        let next_cursor = if out.len() == limit as usize {
            last_id
        } else {
            None
        };

        Ok((out, next_cursor))
    }

    /// Get balances for multiple wallet/token pairs in a single query
    pub async fn get_balances_batch(
        &self,
        pairs: &[(Felt, Felt)],
    ) -> Result<HashMap<(Felt, Felt), U256>> {
        if pairs.is_empty() {
            return Ok(HashMap::new());
        }

        let unique_pairs = pairs
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        self.load_balances_for_pairs(&unique_pairs).await
    }

    async fn load_balances_for_pairs(
        &self,
        pairs: &[(Felt, Felt)],
    ) -> Result<HashMap<(Felt, Felt), U256>> {
        let lookup_start = std::time::Instant::now();
        let (mut result, missing) = self.get_cached_balances(pairs);
        ::metrics::counter!("torii_erc20_balance_lookup_pairs_total", "source" => "cache")
            .increment(pairs.len() as u64);

        if !missing.is_empty() {
            let db_load_start = std::time::Instant::now();
            let from_db = if self.backend == StorageBackend::Postgres {
                self.pg_get_balances_batch(&missing).await?
            } else {
                self.sqlite_load_balances_for_pairs(&missing)?
            };
            ::metrics::histogram!("torii_erc20_balance_db_load_seconds")
                .record(db_load_start.elapsed().as_secs_f64());

            self.store_cached_balances(&from_db);
            result.extend(from_db);
        }

        ::metrics::histogram!("torii_erc20_balance_lookup_seconds")
            .record(lookup_start.elapsed().as_secs_f64());
        Ok(result)
    }

    fn collect_affected_pairs(transfers: &[TransferData]) -> Vec<(Felt, Felt)> {
        transfers
            .iter()
            .flat_map(|transfer| {
                let mut pairs = Vec::with_capacity(2);
                if transfer.from != Felt::ZERO {
                    pairs.push((transfer.token, transfer.from));
                }
                if transfer.to != Felt::ZERO {
                    pairs.push((transfer.token, transfer.to));
                }
                pairs
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    }

    fn collect_sender_pairs(transfers: &[TransferData]) -> Vec<(Felt, Felt)> {
        transfers
            .iter()
            .filter(|transfer| transfer.from != Felt::ZERO)
            .map(|transfer| (transfer.token, transfer.from))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    }

    fn build_adjustment_requests(
        transfers: &[TransferData],
        current_balances: &HashMap<(Felt, Felt), U256>,
    ) -> Vec<BalanceFetchRequest> {
        let mut pending_debits: HashMap<(Felt, Felt), U256> = HashMap::new();
        let mut adjustment_requests = Vec::new();
        let mut requested_keys: HashSet<(Felt, Felt, u64)> = HashSet::new();

        for transfer in transfers {
            if transfer.from == Felt::ZERO {
                continue;
            }

            let key = (transfer.token, transfer.from);
            let stored_balance = current_balances
                .get(&key)
                .copied()
                .unwrap_or(U256::from(0u64));
            let total_pending = pending_debits
                .get(&key)
                .copied()
                .unwrap_or(U256::from(0u64));
            let total_needed = total_pending + transfer.amount;

            if stored_balance >= total_needed {
                pending_debits.insert(key, total_needed);
            } else {
                let block_before = transfer.block_number.saturating_sub(1);
                let req_key = (transfer.token, transfer.from, block_before);
                if requested_keys.insert(req_key) {
                    adjustment_requests.push(BalanceFetchRequest {
                        token: transfer.token,
                        wallet: transfer.from,
                        block_number: block_before,
                    });
                }
            }
        }

        adjustment_requests
    }

    pub async fn check_balances_batch_with_snapshot(
        &self,
        transfers: &[TransferData],
    ) -> Result<BalanceCheckBatch> {
        if transfers.is_empty() {
            return Ok(BalanceCheckBatch {
                adjustment_requests: Vec::new(),
                balance_snapshot: HashMap::new(),
            });
        }

        // For consistency checks we only need sender balances.
        // Receiver balances are loaded lazily in apply() if missing.
        let sender_pairs = Self::collect_sender_pairs(transfers);
        let balance_snapshot = self.load_balances_for_pairs(&sender_pairs).await?;
        let adjustment_requests = Self::build_adjustment_requests(transfers, &balance_snapshot);

        if !adjustment_requests.is_empty() {
            tracing::info!(
                target: "torii_erc20::storage",
                count = adjustment_requests.len(),
                "Detected balance inconsistencies, will fetch from RPC"
            );
        }

        Ok(BalanceCheckBatch {
            adjustment_requests,
            balance_snapshot,
        })
    }

    /// Check which transfers need balance adjustments
    ///
    /// For each transfer, checks if the sender's current balance would go negative.
    /// Returns BalanceFetchRequests for wallets that need adjustment.
    ///
    /// The returned requests have block_number set to transfer.block_number - 1
    /// (the block RIGHT BEFORE the transfer occurred).
    pub async fn check_balances_batch(
        &self,
        transfers: &[TransferData],
    ) -> Result<Vec<BalanceFetchRequest>> {
        Ok(self
            .check_balances_batch_with_snapshot(transfers)
            .await?
            .adjustment_requests)
    }

    /// Apply transfers with adjustments and update balances
    ///
    /// This is the main balance tracking method. It:
    /// 1. Uses adjustments for wallets where we fetched actual balances
    /// 2. Applies transfer debits/credits to update balances
    /// 3. Records adjustments in the audit table
    ///
    /// # Arguments
    /// * `transfers` - The transfers to apply
    /// * `adjustments` - Map of (token, wallet) -> actual_balance fetched from RPC
    pub async fn apply_transfers_with_adjustments(
        &self,
        transfers: &[TransferData],
        adjustments: &HashMap<(Felt, Felt), U256>,
    ) -> Result<()> {
        self.apply_transfers_with_adjustments_with_snapshot(transfers, adjustments, None)
            .await
    }

    pub async fn apply_transfers_with_adjustments_with_snapshot(
        &self,
        transfers: &[TransferData],
        adjustments: &HashMap<(Felt, Felt), U256>,
        balance_snapshot: Option<HashMap<(Felt, Felt), U256>>,
    ) -> Result<()> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_apply_transfers_with_adjustments(transfers, adjustments, balance_snapshot)
                .await;
        }
        if transfers.is_empty() {
            return Ok(());
        }

        let affected_pairs = Self::collect_affected_pairs(transfers);
        let mut balance_cache = balance_snapshot.unwrap_or_default();
        if balance_cache.is_empty() {
            balance_cache = self.load_balances_for_pairs(&affected_pairs).await?;
        } else {
            let missing_pairs = affected_pairs
                .iter()
                .copied()
                .filter(|pair| !balance_cache.contains_key(pair))
                .collect::<Vec<_>>();
            if !missing_pairs.is_empty() {
                let missing_balances = self.load_balances_for_pairs(&missing_pairs).await?;
                balance_cache.extend(missing_balances);
            }
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let adjustment_context: HashMap<(Felt, Felt), (u64, Felt)> = transfers
            .iter()
            .filter(|transfer| transfer.from != Felt::ZERO)
            .map(|transfer| {
                (
                    (transfer.token, transfer.from),
                    (transfer.block_number, transfer.tx_hash),
                )
            })
            .collect();

        // Apply adjustments - these are the "corrected" starting balances
        let mut adjustments_to_record: Vec<BalanceAdjustment> = Vec::new();

        for ((token, wallet), actual_balance) in adjustments {
            let key = (*token, *wallet);
            let computed = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));

            // Only record if there's actually a difference
            if computed != *actual_balance {
                if let Some((adjusted_at_block, tx_hash)) = adjustment_context.get(&key) {
                    adjustments_to_record.push(BalanceAdjustment {
                        token: *token,
                        wallet: *wallet,
                        computed_balance: computed,
                        actual_balance: *actual_balance,
                        adjusted_at_block: *adjusted_at_block,
                        tx_hash: *tx_hash,
                    });
                }
            }

            // Update cache with actual balance
            balance_cache.insert(key, *actual_balance);
        }

        // Apply transfers to balances
        let mut last_block_per_wallet: HashMap<(Felt, Felt), (u64, Felt)> = HashMap::new();

        for transfer in transfers {
            // Debit sender (if not mint)
            if transfer.from != Felt::ZERO {
                let key = (transfer.token, transfer.from);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                // Saturating subtract: return 0 if would underflow
                let new_balance = if current >= transfer.amount {
                    current - transfer.amount
                } else {
                    U256::from(0u64)
                };
                balance_cache.insert(key, new_balance);
                last_block_per_wallet.insert(key, (transfer.block_number, transfer.tx_hash));
            }

            // Credit receiver (if not burn)
            if transfer.to != Felt::ZERO {
                let key = (transfer.token, transfer.to);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                // Use safe addition to handle potential overflow gracefully
                let new_balance = safe_u256_add(current, transfer.amount);
                balance_cache.insert(key, new_balance);
                last_block_per_wallet.insert(key, (transfer.block_number, transfer.tx_hash));
            }
        }

        let mut balance_rows = Vec::with_capacity(last_block_per_wallet.len());
        for ((token, wallet), (last_block, last_tx_hash)) in &last_block_per_wallet {
            let balance = balance_cache
                .get(&(*token, *wallet))
                .copied()
                .unwrap_or(U256::from(0u64));
            balance_rows.push(BalanceUpsertRow {
                token: felt_to_blob(*token),
                wallet: felt_to_blob(*wallet),
                balance: u256_to_blob(balance),
                last_block: last_block.to_string(),
                last_tx_hash: felt_to_blob(*last_tx_hash),
            });
        }
        Self::sqlite_upsert_balances_rows(&tx, &balance_rows)?;

        // Record adjustments for audit
        if !adjustments_to_record.is_empty() {
            let adjustment_rows = adjustments_to_record
                .iter()
                .map(|adj| AdjustmentInsertRow {
                    token: felt_to_blob(adj.token),
                    wallet: felt_to_blob(adj.wallet),
                    computed_balance: u256_to_blob(adj.computed_balance),
                    actual_balance: u256_to_blob(adj.actual_balance),
                    adjusted_at_block: adj.adjusted_at_block.to_string(),
                    tx_hash: felt_to_blob(adj.tx_hash),
                })
                .collect::<Vec<_>>();
            Self::sqlite_insert_adjustment_rows(&tx, &adjustment_rows)?;

            tracing::info!(
                target: "torii_erc20::storage",
                count = adjustments_to_record.len(),
                "Applied balance adjustments (genesis/airdrop detection)"
            );
        }

        tx.commit()?;

        let updated_balances = last_block_per_wallet
            .keys()
            .filter_map(|key| {
                balance_cache
                    .get(key)
                    .copied()
                    .map(|balance| (*key, balance))
            })
            .collect::<HashMap<_, _>>();
        self.store_cached_balances(&updated_balances);

        Ok(())
    }

    /// Get adjustment count (for statistics)
    pub async fn get_adjustment_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM balance_adjustments", [], |row| {
            row.get(0)
        })?;
        Ok(count as u64)
    }

    /// Get unique wallet count with balances
    pub async fn get_wallet_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(DISTINCT wallet) FROM balances", [], |row| {
                row.get(0)
            })?;
        Ok(count as u64)
    }

    // ===== Token Metadata Methods =====

    /// Check if metadata exists for a token
    pub async fn has_token_metadata(&self, token: Felt) -> Result<bool> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_has_token_metadata(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_metadata WHERE token = ?",
            params![&token_blob],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Returns the subset of `tokens` that already have metadata.
    pub async fn has_token_metadata_batch(&self, tokens: &[Felt]) -> Result<HashSet<Felt>> {
        if tokens.is_empty() {
            return Ok(HashSet::new());
        }

        if self.backend == StorageBackend::Postgres {
            return self.pg_has_token_metadata_batch(tokens).await;
        }

        let conn = self.conn.lock().unwrap();
        let mut existing = HashSet::with_capacity(tokens.len());
        let mut stmt =
            conn.prepare_cached("SELECT 1 FROM token_metadata WHERE token = ? LIMIT 1")?;
        for token in tokens {
            let mut rows = stmt.query(params![felt_to_blob(*token)])?;
            if rows.next()?.is_some() {
                existing.insert(*token);
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
        decimals: Option<u8>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        let clean_name = name.map(|s| s.replace('\0', ""));
        let clean_symbol = symbol.map(|s| s.replace('\0', ""));
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_upsert_token_metadata(
                    token,
                    clean_name.as_deref(),
                    clean_symbol.as_deref(),
                    decimals,
                    total_supply,
                )
                .await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let supply_blob = total_supply.map(u256_to_blob);
        conn.execute(
            "INSERT INTO token_metadata (token, name, symbol, decimals, total_supply)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(token) DO UPDATE SET
                 name = COALESCE(excluded.name, token_metadata.name),
                 symbol = COALESCE(excluded.symbol, token_metadata.symbol),
                 decimals = COALESCE(excluded.decimals, token_metadata.decimals),
                 total_supply = COALESCE(excluded.total_supply, token_metadata.total_supply)",
            params![
                &token_blob,
                clean_name.as_deref(),
                clean_symbol.as_deref(),
                decimals.map(|d| d.to_string()),
                supply_blob.as_deref()
            ],
        )?;
        Ok(())
    }

    /// Get token metadata
    pub async fn get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<u8>, Option<U256>)>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_metadata(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let result = conn
            .query_row(
                "SELECT name, symbol, decimals, total_supply FROM token_metadata WHERE token = ?",
                params![&token_blob],
                |row| {
                    let name: Option<String> = row.get(0)?;
                    let symbol: Option<String> = row.get(1)?;
                    let decimals: Option<String> = row.get(2)?;
                    let total_supply: Option<Vec<u8>> = row.get(3)?;
                    Ok((
                        name,
                        symbol,
                        decimals.and_then(|d| d.parse::<u8>().ok()),
                        total_supply.map(|b| blob_to_u256(&b)),
                    ))
                },
            )
            .ok();
        Ok(result)
    }

    /// Get all token metadata
    pub async fn get_all_token_metadata(
        &self,
    ) -> Result<
        Vec<(
            Felt,
            Option<String>,
            Option<String>,
            Option<u8>,
            Option<U256>,
        )>,
    > {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT token, name, symbol, decimals, total_supply FROM token_metadata",
        )?;
        let rows = stmt.query_map([], |row| {
            let token_bytes: Vec<u8> = row.get(0)?;
            let name: Option<String> = row.get(1)?;
            let symbol: Option<String> = row.get(2)?;
            let decimals: Option<String> = row.get(3)?;
            let total_supply: Option<Vec<u8>> = row.get(4)?;
            Ok((
                blob_to_felt(&token_bytes),
                name,
                symbol,
                decimals.and_then(|d| d.parse::<u8>().ok()),
                total_supply.map(|b| blob_to_u256(&b)),
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
        Vec<(
            Felt,
            Option<String>,
            Option<String>,
            Option<u8>,
            Option<U256>,
        )>,
        Option<Felt>,
    )> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_metadata_paginated(cursor, limit).await;
        }
        let conn = self.conn.lock().unwrap();
        let fetch_limit = limit.clamp(1, 1000) as usize + 1;

        let mut out = if let Some(cursor_token) = cursor {
            let cursor_blob = felt_to_blob(cursor_token);
            let mut stmt = conn.prepare_cached(
                "SELECT token, name, symbol, decimals, total_supply
                 FROM token_metadata
                 WHERE token > ?1
                 ORDER BY token ASC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![&cursor_blob, fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let decimals: Option<String> = row.get(3)?;
                let total_supply: Option<Vec<u8>> = row.get(4)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    decimals.and_then(|d| d.parse::<u8>().ok()),
                    total_supply.map(|b| blob_to_u256(&b)),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare_cached(
                "SELECT token, name, symbol, decimals, total_supply
                 FROM token_metadata
                 ORDER BY token ASC
                 LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let decimals: Option<String> = row.get(3)?;
                let total_supply: Option<Vec<u8>> = row.get(4)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    decimals.and_then(|d| d.parse::<u8>().ok()),
                    total_supply.map(|b| blob_to_u256(&b)),
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

    async fn pg_client(&self) -> Result<tokio::sync::MutexGuard<'_, Client>> {
        let conns = self
            .pg_conns
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL connections not initialized"))?;
        let idx = self.pg_rr.fetch_add(1, Ordering::Relaxed) % conns.len();
        Ok(conns[idx].lock().await)
    }

    fn pg_next_param(
        params: &mut Vec<Box<dyn PgToSql + Sync + Send>>,
        value: impl PgToSql + Sync + Send + 'static,
    ) -> String {
        params.push(Box::new(value));
        format!("${}", params.len())
    }

    async fn pg_insert_transfers_batch(&self, transfers: &[TransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let zero_blob = felt_to_blob(Felt::ZERO);
        let mut token_vec = Vec::with_capacity(transfers.len());
        let mut from_vec = Vec::with_capacity(transfers.len());
        let mut to_vec = Vec::with_capacity(transfers.len());
        let mut amount_vec = Vec::with_capacity(transfers.len());
        let mut block_vec: Vec<String> = Vec::with_capacity(transfers.len());
        let mut tx_hash_vec = Vec::with_capacity(transfers.len());
        let mut ts_vec: Vec<String> = Vec::with_capacity(transfers.len());

        for transfer in transfers {
            token_vec.push(felt_to_blob(transfer.token));
            from_vec.push(felt_to_blob(transfer.from));
            to_vec.push(felt_to_blob(transfer.to));
            amount_vec.push(u256_to_blob(transfer.amount));
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
                    INSERT INTO erc20.transfers (token, from_addr, to_addr, amount, block_number, tx_hash, timestamp)
                    SELECT i.token, i.from_addr, i.to_addr, i.amount, i.block_number, i.tx_hash, i.timestamp
                    FROM unnest(
                        $1::bytea[],
                        $2::bytea[],
                        $3::bytea[],
                        $4::bytea[],
                        $5::text[],
                        $6::bytea[],
                        $7::text[]
                    ) AS i(token, from_addr, to_addr, amount, block_number, tx_hash, timestamp)
                    ON CONFLICT (token, tx_hash, from_addr, to_addr) DO NOTHING
                    RETURNING id, token, from_addr, to_addr, block_number
                ),
                _activity AS (
                    INSERT INTO erc20.wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                    SELECT t.from_addr, t.token, t.id, 'both', t.block_number
                    FROM inserted t
                    WHERE t.from_addr <> $8::bytea AND t.to_addr <> $8::bytea AND t.from_addr = t.to_addr
                    UNION ALL
                    SELECT t.from_addr, t.token, t.id, 'sent', t.block_number
                    FROM inserted t
                    WHERE t.from_addr <> $8::bytea AND t.from_addr <> t.to_addr
                    UNION ALL
                    SELECT t.to_addr, t.token, t.id, 'received', t.block_number
                    FROM inserted t
                    WHERE t.to_addr <> $8::bytea AND t.from_addr <> t.to_addr
                )
                SELECT COUNT(*)::bigint FROM inserted",
                &[
                    &token_vec,
                    &from_vec,
                    &to_vec,
                    &amount_vec,
                    &block_vec,
                    &tx_hash_vec,
                    &ts_vec,
                    &zero_blob,
                ],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as usize)
    }

    async fn pg_insert_approvals_batch(&self, approvals: &[ApprovalData]) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }

        let zero_blob = felt_to_blob(Felt::ZERO);
        let mut token_vec = Vec::with_capacity(approvals.len());
        let mut owner_vec = Vec::with_capacity(approvals.len());
        let mut spender_vec = Vec::with_capacity(approvals.len());
        let mut amount_vec = Vec::with_capacity(approvals.len());
        let mut block_vec: Vec<String> = Vec::with_capacity(approvals.len());
        let mut tx_hash_vec = Vec::with_capacity(approvals.len());
        let mut ts_vec: Vec<String> = Vec::with_capacity(approvals.len());

        for approval in approvals {
            token_vec.push(felt_to_blob(approval.token));
            owner_vec.push(felt_to_blob(approval.owner));
            spender_vec.push(felt_to_blob(approval.spender));
            amount_vec.push(u256_to_blob(approval.amount));
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
        let row = client
            .query_one(
                "WITH inserted AS (
                    INSERT INTO erc20.approvals (token, owner, spender, amount, block_number, tx_hash, timestamp)
                    SELECT i.token, i.owner, i.spender, i.amount, i.block_number, i.tx_hash, i.timestamp
                    FROM unnest(
                        $1::bytea[],
                        $2::bytea[],
                        $3::bytea[],
                        $4::bytea[],
                        $5::text[],
                        $6::bytea[],
                        $7::text[]
                    ) AS i(token, owner, spender, amount, block_number, tx_hash, timestamp)
                    ON CONFLICT (token, tx_hash, owner, spender) DO NOTHING
                    RETURNING id, token, owner, spender, block_number
                ),
                _activity AS (
                    INSERT INTO erc20.approval_activity (account_address, token, approval_id, role, block_number)
                    SELECT a.owner, a.token, a.id, 'both', a.block_number
                    FROM inserted a
                    WHERE a.owner <> $8::bytea AND a.spender <> $8::bytea AND a.owner = a.spender
                    UNION ALL
                    SELECT a.owner, a.token, a.id, 'owner', a.block_number
                    FROM inserted a
                    WHERE a.owner <> $8::bytea AND a.owner <> a.spender
                    UNION ALL
                    SELECT a.spender, a.token, a.id, 'spender', a.block_number
                    FROM inserted a
                    WHERE a.spender <> $8::bytea AND a.owner <> a.spender
                )
                SELECT COUNT(*)::bigint FROM inserted",
                &[
                    &token_vec,
                    &owner_vec,
                    &spender_vec,
                    &amount_vec,
                    &block_vec,
                    &tx_hash_vec,
                    &ts_vec,
                    &zero_blob,
                ],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as usize)
    }

    async fn pg_get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        direction: TransferDirection,
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TransferData>, Option<TransferCursor>)> {
        let client = self.pg_client().await?;
        let mut query = String::new();
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            let p = Self::pg_next_param(&mut params, felt_to_blob(wallet_addr));
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM erc20.wallet_activity wa
                 JOIN erc20.transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ",
            );
            query.push_str(&p);

            match direction {
                TransferDirection::All => {}
                TransferDirection::Sent => query.push_str(" AND wa.direction IN ('sent', 'both')"),
                TransferDirection::Received => {
                    query.push_str(" AND wa.direction IN ('received', 'both')");
                }
            }

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
                "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM erc20.transfers t
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

        let pl = Self::pg_next_param(&mut params, limit as i64);
        query.push_str(&format!(
            " ORDER BY t.block_number DESC, t.id DESC LIMIT {pl}"
        ));
        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;

        let transfers: Vec<TransferData> = rows
            .into_iter()
            .map(|row| TransferData {
                id: Some(row.get::<usize, i64>(0)),
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                from: blob_to_felt(&row.get::<usize, Vec<u8>>(2)),
                to: blob_to_felt(&row.get::<usize, Vec<u8>>(3)),
                amount: blob_to_u256(&row.get::<usize, Vec<u8>>(4)),
                block_number: row.get::<usize, String>(5).parse::<u64>().unwrap_or(0),
                tx_hash: blob_to_felt(&row.get::<usize, Vec<u8>>(6)),
                timestamp: row
                    .get::<usize, Option<String>>(7)
                    .and_then(|s| s.parse::<i64>().ok()),
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

    async fn pg_get_approvals_filtered(
        &self,
        account: Option<Felt>,
        owner: Option<Felt>,
        spender: Option<Felt>,
        tokens: &[Felt],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<ApprovalCursor>,
        limit: u32,
    ) -> Result<(Vec<ApprovalData>, Option<ApprovalCursor>)> {
        let client = self.pg_client().await?;
        let mut query = String::new();
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();

        if let Some(account_addr) = account {
            query.push_str(
                "SELECT DISTINCT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM erc20.approval_activity aa
                 JOIN erc20.approvals a ON aa.approval_id = a.id
                 WHERE aa.account_address = ",
            );
            query.push_str(&Self::pg_next_param(
                &mut params,
                felt_to_blob(account_addr),
            ));

            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND aa.token IN ({list})"));
            }
        } else {
            query.push_str(
                "SELECT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM erc20.approvals a
                 WHERE 1=1",
            );
            if let Some(owner_addr) = owner {
                query.push_str(" AND a.owner = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(owner_addr)));
            }
            if let Some(spender_addr) = spender {
                query.push_str(" AND a.spender = ");
                query.push_str(&Self::pg_next_param(
                    &mut params,
                    felt_to_blob(spender_addr),
                ));
            }
            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND a.token IN ({list})"));
            }
        }

        if let Some(block_min) = block_from {
            query.push_str(" AND a.block_number >= ");
            query.push_str(&Self::pg_next_param(&mut params, block_min.to_string()));
        }
        if let Some(block_max) = block_to {
            query.push_str(" AND a.block_number <= ");
            query.push_str(&Self::pg_next_param(&mut params, block_max.to_string()));
        }
        if let Some(c) = cursor {
            let p1 = Self::pg_next_param(&mut params, c.block_number.to_string());
            let p2 = Self::pg_next_param(&mut params, c.block_number.to_string());
            let p3 = Self::pg_next_param(&mut params, c.id);
            query.push_str(&format!(
                " AND (a.block_number < {p1} OR (a.block_number = {p2} AND a.id < {p3}))"
            ));
        }
        let pl = Self::pg_next_param(&mut params, limit as i64);
        query.push_str(&format!(
            " ORDER BY a.block_number DESC, a.id DESC LIMIT {pl}"
        ));

        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let approvals: Vec<ApprovalData> = rows
            .into_iter()
            .map(|row| ApprovalData {
                id: Some(row.get::<usize, i64>(0)),
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                owner: blob_to_felt(&row.get::<usize, Vec<u8>>(2)),
                spender: blob_to_felt(&row.get::<usize, Vec<u8>>(3)),
                amount: blob_to_u256(&row.get::<usize, Vec<u8>>(4)),
                block_number: row.get::<usize, String>(5).parse::<u64>().unwrap_or(0),
                tx_hash: blob_to_felt(&row.get::<usize, Vec<u8>>(6)),
                timestamp: row
                    .get::<usize, Option<String>>(7)
                    .and_then(|s| s.parse::<i64>().ok()),
            })
            .collect();

        let next_cursor = if approvals.len() == limit as usize {
            approvals.last().map(|a| ApprovalCursor {
                block_number: a.block_number,
                id: a.id.unwrap_or_default(),
            })
        } else {
            None
        };
        Ok((approvals, next_cursor))
    }

    async fn pg_get_transfer_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM erc20.transfers", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_approval_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM erc20.approvals", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_token_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(DISTINCT token) FROM erc20.transfers", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_latest_block(&self) -> Result<Option<u64>> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT MAX(block_number) FROM erc20.transfers", &[])
            .await?;
        let v: Option<String> = row.get(0);
        Ok(v.and_then(|x| x.parse::<u64>().ok()))
    }

    async fn pg_get_balance(&self, token: Felt, wallet: Felt) -> Result<Option<U256>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT balance FROM erc20.balances WHERE token = $1 AND wallet = $2",
                &[&felt_to_blob(token), &felt_to_blob(wallet)],
            )
            .await?;
        Ok(row.map(|r| blob_to_u256(&r.get::<usize, Vec<u8>>(0))))
    }

    async fn pg_get_balance_with_block(
        &self,
        token: Felt,
        wallet: Felt,
    ) -> Result<Option<(U256, u64)>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT balance, last_block FROM erc20.balances WHERE token = $1 AND wallet = $2",
                &[&felt_to_blob(token), &felt_to_blob(wallet)],
            )
            .await?;
        Ok(row.map(|r| {
            (
                blob_to_u256(&r.get::<usize, Vec<u8>>(0)),
                r.get::<usize, String>(1).parse::<u64>().unwrap_or(0),
            )
        }))
    }

    async fn pg_get_balances_filtered(
        &self,
        token: Option<Felt>,
        wallet: Option<Felt>,
        cursor: Option<i64>,
        limit: u32,
    ) -> Result<(Vec<BalanceData>, Option<i64>)> {
        let client = self.pg_client().await?;
        let mut query = String::from(
            "SELECT id, token, wallet, balance, last_block, last_tx_hash FROM erc20.balances WHERE 1=1",
        );
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();
        if let Some(token_addr) = token {
            query.push_str(" AND token = ");
            query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(token_addr)));
        }
        if let Some(wallet_addr) = wallet {
            query.push_str(" AND wallet = ");
            query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(wallet_addr)));
        }
        if let Some(c) = cursor {
            query.push_str(" AND id > ");
            query.push_str(&Self::pg_next_param(&mut params, c));
        }
        query.push_str(" ORDER BY id ASC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, limit as i64));

        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let mut out = Vec::new();
        let mut last_id = None;
        for row in rows {
            let id: i64 = row.get(0);
            last_id = Some(id);
            out.push(BalanceData {
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                wallet: blob_to_felt(&row.get::<usize, Vec<u8>>(2)),
                balance: blob_to_u256(&row.get::<usize, Vec<u8>>(3)),
                last_block: row.get::<usize, String>(4).parse::<u64>().unwrap_or(0),
                last_tx_hash: blob_to_felt(&row.get::<usize, Vec<u8>>(5)),
            });
        }
        let next_cursor = if out.len() == limit as usize {
            last_id
        } else {
            None
        };
        Ok((out, next_cursor))
    }

    async fn pg_get_balances_batch(
        &self,
        pairs: &[(Felt, Felt)],
    ) -> Result<HashMap<(Felt, Felt), U256>> {
        if pairs.is_empty() {
            return Ok(HashMap::new());
        }

        let unique_pairs = pairs
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let client = self.pg_client().await?;
        let mut result = HashMap::new();
        for chunk in unique_pairs.chunks(2000) {
            let mut token_vec = Vec::with_capacity(chunk.len());
            let mut wallet_vec = Vec::with_capacity(chunk.len());
            for (token, wallet) in chunk {
                token_vec.push(felt_to_blob(*token));
                wallet_vec.push(felt_to_blob(*wallet));
            }

            let rows = client
                .query(
                    "SELECT b.token, b.wallet, b.balance
                     FROM erc20.balances b
                     JOIN unnest($1::bytea[], $2::bytea[]) AS req(token, wallet)
                       ON b.token = req.token AND b.wallet = req.wallet",
                    &[&token_vec, &wallet_vec],
                )
                .await?;

            for row in rows {
                result.insert(
                    (
                        blob_to_felt(&row.get::<usize, Vec<u8>>(0)),
                        blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                    ),
                    blob_to_u256(&row.get::<usize, Vec<u8>>(2)),
                );
            }
        }
        Ok(result)
    }

    async fn pg_apply_transfers_with_adjustments(
        &self,
        transfers: &[TransferData],
        adjustments: &HashMap<(Felt, Felt), U256>,
        balance_snapshot: Option<HashMap<(Felt, Felt), U256>>,
    ) -> Result<()> {
        if transfers.is_empty() {
            return Ok(());
        }

        let affected_pairs = Self::collect_affected_pairs(transfers);
        let mut balance_cache = balance_snapshot.unwrap_or_default();
        if balance_cache.is_empty() {
            balance_cache = self.load_balances_for_pairs(&affected_pairs).await?;
        } else {
            let missing_pairs = affected_pairs
                .iter()
                .copied()
                .filter(|pair| !balance_cache.contains_key(pair))
                .collect::<Vec<_>>();
            if !missing_pairs.is_empty() {
                let missing_balances = self.load_balances_for_pairs(&missing_pairs).await?;
                balance_cache.extend(missing_balances);
            }
        }
        let adjustment_context: HashMap<(Felt, Felt), (u64, Felt)> = transfers
            .iter()
            .filter(|transfer| transfer.from != Felt::ZERO)
            .map(|transfer| {
                (
                    (transfer.token, transfer.from),
                    (transfer.block_number, transfer.tx_hash),
                )
            })
            .collect();

        let mut client = self.pg_client().await?;
        let tx = client.transaction().await?;

        let mut adjustments_to_record: Vec<BalanceAdjustment> = Vec::new();
        for ((token, wallet), actual_balance) in adjustments {
            let key = (*token, *wallet);
            let computed = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
            if computed != *actual_balance {
                if let Some((adjusted_at_block, tx_hash)) = adjustment_context.get(&key) {
                    adjustments_to_record.push(BalanceAdjustment {
                        token: *token,
                        wallet: *wallet,
                        computed_balance: computed,
                        actual_balance: *actual_balance,
                        adjusted_at_block: *adjusted_at_block,
                        tx_hash: *tx_hash,
                    });
                }
            }
            balance_cache.insert(key, *actual_balance);
        }

        let mut last_block_per_wallet: HashMap<(Felt, Felt), (u64, Felt)> = HashMap::new();
        for transfer in transfers {
            if transfer.from != Felt::ZERO {
                let key = (transfer.token, transfer.from);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                let new_balance = if current >= transfer.amount {
                    current - transfer.amount
                } else {
                    U256::from(0u64)
                };
                balance_cache.insert(key, new_balance);
                last_block_per_wallet.insert(key, (transfer.block_number, transfer.tx_hash));
            }
            if transfer.to != Felt::ZERO {
                let key = (transfer.token, transfer.to);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                // Use safe addition to handle potential overflow gracefully
                balance_cache.insert(key, safe_u256_add(current, transfer.amount));
                last_block_per_wallet.insert(key, (transfer.block_number, transfer.tx_hash));
            }
        }

        let mut balance_rows = Vec::with_capacity(last_block_per_wallet.len());
        for ((token, wallet), (last_block, last_tx_hash)) in &last_block_per_wallet {
            let balance = balance_cache
                .get(&(*token, *wallet))
                .copied()
                .unwrap_or(U256::from(0u64));
            balance_rows.push((
                felt_to_blob(*token),
                felt_to_blob(*wallet),
                u256_to_blob(balance),
                last_block.to_string(),
                felt_to_blob(*last_tx_hash),
            ));
        }

        for chunk in balance_rows.chunks(2000) {
            let mut tokens = Vec::with_capacity(chunk.len());
            let mut wallets = Vec::with_capacity(chunk.len());
            let mut balances = Vec::with_capacity(chunk.len());
            let mut last_blocks: Vec<String> = Vec::with_capacity(chunk.len());
            let mut tx_hashes = Vec::with_capacity(chunk.len());

            for (token, wallet, balance, last_block, tx_hash) in chunk {
                tokens.push(token.clone());
                wallets.push(wallet.clone());
                balances.push(balance.clone());
                last_blocks.push(last_block.clone());
                tx_hashes.push(tx_hash.clone());
            }

            tx.execute(
                "INSERT INTO erc20.balances (token, wallet, balance, last_block, last_tx_hash, updated_at)
                 SELECT token, wallet, balance, last_block, last_tx_hash, EXTRACT(EPOCH FROM NOW())::TEXT
                 FROM unnest($1::bytea[], $2::bytea[], $3::bytea[], $4::text[], $5::bytea[])
                      AS b(token, wallet, balance, last_block, last_tx_hash)
                 ON CONFLICT (token, wallet) DO UPDATE SET
                     balance = EXCLUDED.balance,
                     last_block = EXCLUDED.last_block,
                     last_tx_hash = EXCLUDED.last_tx_hash,
                     updated_at = EXTRACT(EPOCH FROM NOW())::TEXT
                 WHERE erc20.balances.balance IS DISTINCT FROM EXCLUDED.balance
                    OR erc20.balances.last_block IS DISTINCT FROM EXCLUDED.last_block
                    OR erc20.balances.last_tx_hash IS DISTINCT FROM EXCLUDED.last_tx_hash",
                &[&tokens, &wallets, &balances, &last_blocks, &tx_hashes],
            )
            .await?;
        }

        for chunk in adjustments_to_record.chunks(2000) {
            let mut tokens = Vec::with_capacity(chunk.len());
            let mut wallets = Vec::with_capacity(chunk.len());
            let mut computed = Vec::with_capacity(chunk.len());
            let mut actual = Vec::with_capacity(chunk.len());
            let mut blocks: Vec<String> = Vec::with_capacity(chunk.len());
            let mut tx_hashes = Vec::with_capacity(chunk.len());

            for adj in chunk {
                tokens.push(felt_to_blob(adj.token));
                wallets.push(felt_to_blob(adj.wallet));
                computed.push(u256_to_blob(adj.computed_balance));
                actual.push(u256_to_blob(adj.actual_balance));
                blocks.push(adj.adjusted_at_block.to_string());
                tx_hashes.push(felt_to_blob(adj.tx_hash));
            }

            tx.execute(
                "INSERT INTO erc20.balance_adjustments
                 (token, wallet, computed_balance, actual_balance, adjusted_at_block, tx_hash)
                 SELECT token, wallet, computed_balance, actual_balance, adjusted_at_block, tx_hash
                 FROM unnest($1::bytea[], $2::bytea[], $3::bytea[], $4::bytea[], $5::text[], $6::bytea[])
                      AS a(token, wallet, computed_balance, actual_balance, adjusted_at_block, tx_hash)",
                &[&tokens, &wallets, &computed, &actual, &blocks, &tx_hashes],
            )
            .await?;
        }

        tx.commit().await?;

        let updated_balances = last_block_per_wallet
            .keys()
            .filter_map(|key| {
                balance_cache
                    .get(key)
                    .copied()
                    .map(|balance| (*key, balance))
            })
            .collect::<HashMap<_, _>>();
        self.store_cached_balances(&updated_balances);

        Ok(())
    }

    async fn pg_has_token_metadata(&self, token: Felt) -> Result<bool> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc20.token_metadata WHERE token = $1",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) > 0)
    }

    async fn pg_has_token_metadata_batch(&self, tokens: &[Felt]) -> Result<HashSet<Felt>> {
        if tokens.is_empty() {
            return Ok(HashSet::new());
        }

        let client = self.pg_client().await?;
        let blob_to_token: HashMap<Vec<u8>, Felt> =
            tokens.iter().map(|t| (felt_to_blob(*t), *t)).collect();
        let blob_params: Vec<Vec<u8>> = blob_to_token.keys().cloned().collect();

        let rows = client
            .query(
                "SELECT token FROM erc20.token_metadata WHERE token = ANY($1::bytea[])",
                &[&blob_params],
            )
            .await?;

        let mut existing = HashSet::new();
        for row in rows {
            let token_blob: Vec<u8> = row.get(0);
            if let Some(token) = blob_to_token.get(&token_blob) {
                existing.insert(*token);
            }
        }

        Ok(existing)
    }

    async fn pg_upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        decimals: Option<u8>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        let clean_name = name.map(|s| s.replace('\0', ""));
        let clean_symbol = symbol.map(|s| s.replace('\0', ""));
        let supply_blob: Option<Vec<u8>> = total_supply.map(u256_to_blob);
        let decimals_str = decimals.map(|d| d.to_string());
        let client = self.pg_client().await?;
        client
            .execute(
                "INSERT INTO erc20.token_metadata (token, name, symbol, decimals, total_supply)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (token) DO UPDATE SET
                 name = COALESCE(EXCLUDED.name, erc20.token_metadata.name),
                 symbol = COALESCE(EXCLUDED.symbol, erc20.token_metadata.symbol),
                 decimals = COALESCE(EXCLUDED.decimals, erc20.token_metadata.decimals),
                 total_supply = COALESCE(EXCLUDED.total_supply, erc20.token_metadata.total_supply)",
                &[
                    &felt_to_blob(token),
                    &clean_name.as_deref(),
                    &clean_symbol.as_deref(),
                    &decimals_str.as_deref(),
                    &supply_blob,
                ],
            )
            .await?;
        Ok(())
    }

    async fn pg_get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<u8>, Option<U256>)>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT name, symbol, decimals, total_supply FROM erc20.token_metadata WHERE token = $1",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.map(|r| {
            let decimals: Option<String> = r.get(2);
            let total_supply: Option<Vec<u8>> = r.get(3);
            (
                r.get(0),
                r.get(1),
                decimals.and_then(|d| d.parse::<u8>().ok()),
                total_supply.map(|b| blob_to_u256(&b)),
            )
        }))
    }

    async fn pg_get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(
            Felt,
            Option<String>,
            Option<String>,
            Option<u8>,
            Option<U256>,
        )>,
        Option<Felt>,
    )> {
        let client = self.pg_client().await?;
        let fetch_limit = limit.clamp(1, 1000) as i64 + 1;
        let rows = if let Some(cursor_token) = cursor {
            client
                .query(
                    "SELECT token, name, symbol, decimals, total_supply
                 FROM erc20.token_metadata
                 WHERE token > $1
                 ORDER BY token ASC
                 LIMIT $2",
                    &[&felt_to_blob(cursor_token), &fetch_limit],
                )
                .await?
        } else {
            client
                .query(
                    "SELECT token, name, symbol, decimals, total_supply
                 FROM erc20.token_metadata
                 ORDER BY token ASC
                 LIMIT $1",
                    &[&fetch_limit],
                )
                .await?
        };

        let mut out: Vec<(
            Felt,
            Option<String>,
            Option<String>,
            Option<u8>,
            Option<U256>,
        )> = rows
            .into_iter()
            .map(|row| {
                let decimals: Option<String> = row.get(3);
                let total_supply: Option<Vec<u8>> = row.get(4);
                (
                    blob_to_felt(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                    decimals.and_then(|d| d.parse::<u8>().ok()),
                    total_supply.map(|b| blob_to_u256(&b)),
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
}
