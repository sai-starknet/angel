use crate::proto::types::clause::ClauseType;
use crate::proto::types::member_value::ValueType;
use crate::proto::types::{
    self, ComparisonOperator, ContractType, LogicalOperator, PaginationDirection, PatternMatching,
};
use crate::proto::world::world_server::World;
use crate::proto::world::{
    RetrieveContractsRequest, RetrieveContractsResponse, RetrieveControllersRequest,
    RetrieveControllersResponse, RetrieveEntitiesRequest, RetrieveEntitiesResponse,
    RetrieveEventsRequest, RetrieveEventsResponse, RetrieveTokenBalancesRequest,
    RetrieveTokenBalancesResponse, RetrieveTokenContractsRequest, RetrieveTokenContractsResponse,
    RetrieveTokenTransfersRequest, RetrieveTokenTransfersResponse, RetrieveTokensRequest,
    RetrieveTokensResponse, RetrieveTransactionsRequest, RetrieveTransactionsResponse,
    SubscribeContractsRequest, SubscribeContractsResponse, SubscribeEntitiesRequest,
    SubscribeEntityResponse, SubscribeEventsRequest, SubscribeEventsResponse,
    SubscribeTokenBalancesRequest, SubscribeTokenBalancesResponse, SubscribeTokenTransfersRequest,
    SubscribeTokenTransfersResponse, SubscribeTokensRequest, SubscribeTokensResponse,
    SubscribeTransactionsRequest, SubscribeTransactionsResponse, UpdateEntitiesSubscriptionRequest,
    UpdateTokenBalancesSubscriptionRequest, UpdateTokenSubscriptionRequest,
    UpdateTokenTransfersSubscriptionRequest, WorldsRequest, WorldsResponse,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use introspect_types::serialize::ToCairoDeSeFrom;
use introspect_types::serialize_def::CairoTypeSerialization;
use introspect_types::{
    Attributes, CairoDeserializer, ColumnDef, ColumnInfo, PrimaryTypeDef, ResultDef, TupleDef,
    TypeDef,
};
use num_bigint::{BigInt, BigUint, Sign};
use primitive_types::{U256, U512};
use prost::Message;
use serde::ser::SerializeMap;
use serde::Serializer;
use serde_json::{Map, Serializer as JsonSerializer, Value};
use sqlx::any::AnyPoolOptions;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Any, AnyConnection, Column, ConnectOptions, Pool, QueryBuilder, Row};
use starknet_types_raw::Felt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use torii_dojo::store::sqlite::SqliteStore;
use torii_dojo::store::DojoStoreTrait;
use torii_dojo::DojoTable;
use torii_introspect::events::{CreateTable, Record, UpdateTable};
use torii_introspect::schema::TableSchema;
use torii_runtime_common::database::DEFAULT_SQLITE_MAX_CONNECTIONS;
use torii_sql::DbBackend;

const SUBSCRIPTION_SEEN_CACHE_CAPACITY: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKind {
    Entity,
    EventMessage,
}

impl TableKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::EventMessage => "event_message",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "entity" => Some(Self::Entity),
            "event_message" => Some(Self::EventMessage),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct EcsService {
    state: Arc<EcsState>,
}

struct CachedViewsSql {
    controllers: String,
    token_balances: String,
}

struct EcsState {
    pool: Pool<Any>,
    backend: DbBackend,
    database_url: String,
    has_erc20: bool,
    has_erc721: bool,
    has_erc1155: bool,
    erc20_url: Option<String>,
    erc721_url: Option<String>,
    erc1155_url: Option<String>,
    managed_tables: Mutex<Option<Arc<HashMap<String, ManagedTable>>>>,
    cached_views_sql: RwLock<Option<CachedViewsSql>>,
    entity_subscriptions: RwLock<EntitySubscriptionRegistry>,
    event_message_subscriptions: RwLock<EntitySubscriptionRegistry>,
    event_subscriptions: Mutex<HashMap<u64, EventSubscription>>,
    contract_subscriptions: Mutex<HashMap<u64, ContractSubscription>>,
    token_subscriptions: Mutex<HashMap<u64, TokenSubscription>>,
    token_balance_subscriptions: Mutex<HashMap<u64, TokenBalanceSubscription>>,
    token_transfer_subscriptions: Mutex<HashMap<u64, TokenTransferSubscription>>,
    transaction_subscriptions: Mutex<HashMap<u64, TransactionSubscription>>,
}

struct EntitySubscription {
    clause: Option<types::Clause>,
    world_addresses: HashSet<Vec<u8>>,
    sender: mpsc::Sender<Result<SubscribeEntityResponse, Status>>,
}

#[derive(Default)]
struct EntitySubscriptionRegistry {
    subscriptions: HashMap<u64, EntitySubscriptionEntry>,
    groups: HashMap<EntitySubscriptionKey, SharedEntitySubscription>,
}

#[derive(Clone)]
struct EntitySubscriptionEntry {
    key: EntitySubscriptionKey,
    sender: mpsc::Sender<Result<SubscribeEntityResponse, Status>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EntitySubscriptionKey {
    clause_bytes: Vec<u8>,
    world_addresses: Vec<Vec<u8>>,
}

struct SharedEntitySubscription {
    clause: Option<types::Clause>,
    world_addresses: HashSet<Vec<u8>>,
    senders: HashMap<u64, mpsc::Sender<Result<SubscribeEntityResponse, Status>>>,
}

#[derive(Clone)]
struct EntitySubscriptionGroupSnapshot {
    clause: Option<types::Clause>,
    world_addresses: HashSet<Vec<u8>>,
    senders: Vec<(u64, mpsc::Sender<Result<SubscribeEntityResponse, Status>>)>,
}

pub struct ManagedSubscriptionStream<T> {
    stream: ReceiverStream<Result<T, Status>>,
    cleanup: Option<SubscriptionCleanup>,
}

struct SubscriptionCleanup {
    state: Arc<EcsState>,
    kind: SubscriptionKind,
    subscription_id: u64,
}

#[derive(Clone, Copy)]
enum SubscriptionKind {
    Entity(TableKind),
    Event,
    Contract,
    Token,
    TokenBalance,
    TokenTransfer,
    Transaction,
}

#[derive(Clone, Default)]
struct SeenCache {
    items: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl SeenCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            items: HashSet::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn from_items<I>(items: I, capacity: usize) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let mut cache = Self::with_capacity(capacity);
        cache.extend(items);
        cache
    }

    fn contains(&self, item: &str) -> bool {
        self.items.contains(item)
    }

    fn extend<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = String>,
    {
        for item in items {
            if self.items.insert(item.clone()) {
                self.order.push_back(item);
            }
            while self.items.len() > self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.items.remove(&evicted);
                } else {
                    break;
                }
            }
        }
    }

    fn snapshot(&self) -> Self {
        self.clone()
    }
}

struct EventSubscription {
    keys: Vec<types::KeysClause>,
    sender: mpsc::Sender<Result<SubscribeEventsResponse, Status>>,
}

struct ContractSubscription {
    query: types::ContractQuery,
    sender: mpsc::Sender<Result<SubscribeContractsResponse, Status>>,
}

struct TokenSubscription {
    contract_addresses: Vec<Vec<u8>>,
    token_ids: Vec<Vec<u8>>,
    seen: SeenCache,
    sender: mpsc::Sender<Result<SubscribeTokensResponse, Status>>,
}

struct TokenBalanceSubscription {
    contract_addresses: Vec<Vec<u8>>,
    account_addresses: Vec<Vec<u8>>,
    token_ids: Vec<Vec<u8>>,
    latest: HashMap<String, Vec<u8>>,
    sender: mpsc::Sender<Result<SubscribeTokenBalancesResponse, Status>>,
}

struct TokenTransferSubscription {
    contract_addresses: Vec<Vec<u8>>,
    account_addresses: Vec<Vec<u8>>,
    token_ids: Vec<Vec<u8>>,
    seen: SeenCache,
    sender: mpsc::Sender<Result<SubscribeTokenTransfersResponse, Status>>,
}

struct TransactionSubscription {
    filter: types::TransactionFilter,
    seen: SeenCache,
    sender: mpsc::Sender<Result<SubscribeTransactionsResponse, Status>>,
}

impl EntitySubscriptionKey {
    fn new(clause: Option<&types::Clause>, world_addresses: &HashSet<Vec<u8>>) -> Self {
        let mut world_addresses = world_addresses.iter().cloned().collect::<Vec<_>>();
        world_addresses.sort();

        Self {
            clause_bytes: clause.map_or_else(Vec::new, Message::encode_to_vec),
            world_addresses,
        }
    }
}

impl EntitySubscriptionRegistry {
    fn insert(&mut self, subscription_id: u64, subscription: EntitySubscription) {
        let key =
            EntitySubscriptionKey::new(subscription.clause.as_ref(), &subscription.world_addresses);
        let sender = subscription.sender.clone();

        self.groups
            .entry(key.clone())
            .or_insert_with(|| SharedEntitySubscription {
                clause: subscription.clause,
                world_addresses: subscription.world_addresses,
                senders: HashMap::new(),
            })
            .senders
            .insert(subscription_id, sender.clone());

        self.subscriptions
            .insert(subscription_id, EntitySubscriptionEntry { key, sender });
    }

    fn update(
        &mut self,
        subscription_id: u64,
        clause: Option<types::Clause>,
        world_addresses: HashSet<Vec<u8>>,
    ) -> bool {
        let Some(entry) = self.subscriptions.get_mut(&subscription_id) else {
            return false;
        };

        let next_key = EntitySubscriptionKey::new(clause.as_ref(), &world_addresses);
        if entry.key == next_key {
            return false;
        }

        let sender = entry.sender.clone();
        let previous_key = entry.key.clone();
        let remove_previous_group = if let Some(group) = self.groups.get_mut(&previous_key) {
            group.senders.remove(&subscription_id);
            group.senders.is_empty()
        } else {
            false
        };
        if remove_previous_group {
            self.groups.remove(&previous_key);
        }

        self.groups
            .entry(next_key.clone())
            .or_insert_with(|| SharedEntitySubscription {
                clause,
                world_addresses,
                senders: HashMap::new(),
            })
            .senders
            .insert(subscription_id, sender);

        entry.key = next_key;
        true
    }

    fn remove(&mut self, subscription_id: u64) -> bool {
        let Some(entry) = self.subscriptions.remove(&subscription_id) else {
            return false;
        };

        let remove_group = if let Some(group) = self.groups.get_mut(&entry.key) {
            group.senders.remove(&subscription_id);
            group.senders.is_empty()
        } else {
            false
        };
        if remove_group {
            self.groups.remove(&entry.key);
        }

        true
    }

    fn group_count(&self) -> usize {
        self.groups.len()
    }

    fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    fn snapshot_groups(&self) -> Vec<EntitySubscriptionGroupSnapshot> {
        self.groups
            .values()
            .map(|group| EntitySubscriptionGroupSnapshot {
                clause: group.clause.clone(),
                world_addresses: group.world_addresses.clone(),
                senders: group
                    .senders
                    .iter()
                    .map(|(&subscription_id, sender)| (subscription_id, sender.clone()))
                    .collect(),
            })
            .collect()
    }
}

impl EcsState {
    fn entity_subscription_registry(&self, kind: TableKind) -> &RwLock<EntitySubscriptionRegistry> {
        match kind {
            TableKind::Entity => &self.entity_subscriptions,
            TableKind::EventMessage => &self.event_message_subscriptions,
        }
    }

    async fn insert_entity_subscription(
        &self,
        kind: TableKind,
        subscription_id: u64,
        subscription: EntitySubscription,
    ) {
        let (subscription_count, group_count) = {
            let mut subscriptions = self.entity_subscription_registry(kind).write().await;
            subscriptions.insert(subscription_id, subscription);
            (
                subscriptions.subscription_count(),
                subscriptions.group_count(),
            )
        };
        record_active_entity_subscriptions(kind, subscription_count);
        record_active_entity_subscription_groups(kind, group_count);
    }

    async fn update_entity_subscription(
        &self,
        kind: TableKind,
        subscription_id: u64,
        clause: Option<types::Clause>,
        world_addresses: HashSet<Vec<u8>>,
    ) {
        let group_count = {
            let mut subscriptions = self.entity_subscription_registry(kind).write().await;
            if !subscriptions.update(subscription_id, clause, world_addresses) {
                return;
            }
            subscriptions.group_count()
        };
        record_active_entity_subscription_groups(kind, group_count);
    }

    async fn remove_entity_subscription(&self, kind: TableKind, subscription_id: u64) -> bool {
        let (subscription_count, group_count) = {
            let mut subscriptions = self.entity_subscription_registry(kind).write().await;
            if !subscriptions.remove(subscription_id) {
                return false;
            }
            (
                subscriptions.subscription_count(),
                subscriptions.group_count(),
            )
        };
        record_active_entity_subscriptions(kind, subscription_count);
        record_active_entity_subscription_groups(kind, group_count);
        true
    }

    async fn remove_subscription(&self, kind: SubscriptionKind, subscription_id: u64) -> bool {
        match kind {
            SubscriptionKind::Entity(kind) => {
                self.remove_entity_subscription(kind, subscription_id).await
            }
            SubscriptionKind::Event => self
                .event_subscriptions
                .lock()
                .await
                .remove(&subscription_id)
                .is_some(),
            SubscriptionKind::Contract => self
                .contract_subscriptions
                .lock()
                .await
                .remove(&subscription_id)
                .is_some(),
            SubscriptionKind::Token => self
                .token_subscriptions
                .lock()
                .await
                .remove(&subscription_id)
                .is_some(),
            SubscriptionKind::TokenBalance => self
                .token_balance_subscriptions
                .lock()
                .await
                .remove(&subscription_id)
                .is_some(),
            SubscriptionKind::TokenTransfer => self
                .token_transfer_subscriptions
                .lock()
                .await
                .remove(&subscription_id)
                .is_some(),
            SubscriptionKind::Transaction => self
                .transaction_subscriptions
                .lock()
                .await
                .remove(&subscription_id)
                .is_some(),
        }
    }
}

impl<T> ManagedSubscriptionStream<T> {
    fn new(
        state: Arc<EcsState>,
        kind: SubscriptionKind,
        subscription_id: u64,
        receiver: mpsc::Receiver<Result<T, Status>>,
    ) -> Self {
        Self {
            stream: ReceiverStream::new(receiver),
            cleanup: Some(SubscriptionCleanup {
                state,
                kind,
                subscription_id,
            }),
        }
    }
}

impl<T> Stream for ManagedSubscriptionStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_next(cx)
    }
}

impl<T> Drop for ManagedSubscriptionStream<T> {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.schedule();
        }
    }
}

impl SubscriptionCleanup {
    fn schedule(self) {
        let state = self.state;
        let kind = self.kind;
        let subscription_id = self.subscription_id;

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = state.remove_subscription(kind, subscription_id).await;
            });
            return;
        }

        match kind {
            SubscriptionKind::Entity(kind) => {
                let mut subscriptions = match state.entity_subscription_registry(kind).try_write() {
                    Ok(subscriptions) => subscriptions,
                    Err(_) => return,
                };
                if subscriptions.remove(subscription_id) {
                    record_active_entity_subscriptions(kind, subscriptions.subscription_count());
                    record_active_entity_subscription_groups(kind, subscriptions.group_count());
                }
            }
            SubscriptionKind::Event => {
                if let Ok(mut subscriptions) = state.event_subscriptions.try_lock() {
                    subscriptions.remove(&subscription_id);
                }
            }
            SubscriptionKind::Contract => {
                if let Ok(mut subscriptions) = state.contract_subscriptions.try_lock() {
                    subscriptions.remove(&subscription_id);
                }
            }
            SubscriptionKind::Token => {
                if let Ok(mut subscriptions) = state.token_subscriptions.try_lock() {
                    subscriptions.remove(&subscription_id);
                }
            }
            SubscriptionKind::TokenBalance => {
                if let Ok(mut subscriptions) = state.token_balance_subscriptions.try_lock() {
                    subscriptions.remove(&subscription_id);
                }
            }
            SubscriptionKind::TokenTransfer => {
                if let Ok(mut subscriptions) = state.token_transfer_subscriptions.try_lock() {
                    subscriptions.remove(&subscription_id);
                }
            }
            SubscriptionKind::Transaction => {
                if let Ok(mut subscriptions) = state.transaction_subscriptions.try_lock() {
                    subscriptions.remove(&subscription_id);
                }
            }
        }
    }
}

#[derive(Clone)]
struct ManagedTable {
    world_address: Felt,
    table: DojoTable,
    kind: TableKind,
}

#[derive(Default)]
struct EntityAggregate {
    world_address: Vec<u8>,
    hashed_keys: Vec<u8>,
    models: Vec<types::Struct>,
    created_at: u64,
    updated_at: u64,
    executed_at: u64,
}

impl EntityAggregate {
    fn into_proto(self) -> types::Entity {
        types::Entity {
            hashed_keys: self.hashed_keys,
            models: self.models,
            created_at: self.created_at,
            updated_at: self.updated_at,
            executed_at: self.executed_at,
            world_address: self.world_address,
        }
    }
}

impl EcsService {
    pub async fn new(
        database_url: &str,
        max_connections: Option<u32>,
        erc20_url: Option<&str>,
        erc721_url: Option<&str>,
        erc1155_url: Option<&str>,
    ) -> Result<Self> {
        sqlx::any::install_default_drivers();

        let backend = get_db_backend(database_url);
        let database_url = match backend {
            DbBackend::Postgres => database_url.to_string(),
            DbBackend::Sqlite => sqlite_url(database_url)?,
        };
        let has_erc20 = erc20_url.is_some();
        let has_erc721 = erc721_url.is_some();
        let has_erc1155 = erc1155_url.is_some();
        let erc20_url = erc20_url.map(std::string::ToString::to_string);
        let erc721_url = erc721_url.map(std::string::ToString::to_string);
        let erc1155_url = erc1155_url.map(std::string::ToString::to_string);

        let pool_options = AnyPoolOptions::new().max_connections(max_connections.unwrap_or(
            if backend == DbBackend::Sqlite {
                DEFAULT_SQLITE_MAX_CONNECTIONS
            } else {
                5
            },
        ));
        let pool = match backend {
            DbBackend::Sqlite => {
                pool_options
                    .after_connect({
                        let erc20_url = erc20_url.clone();
                        let erc721_url = erc721_url.clone();
                        let erc1155_url = erc1155_url.clone();
                        move |conn, _meta| {
                            let erc20_url = erc20_url.clone();
                            let erc721_url = erc721_url.clone();
                            let erc1155_url = erc1155_url.clone();
                            Box::pin(async move {
                                attach_sqlite_databases(
                                    conn,
                                    erc20_url.as_deref(),
                                    erc721_url.as_deref(),
                                    erc1155_url.as_deref(),
                                )
                                .await
                            })
                        }
                    })
                    .connect(&database_url)
                    .await?
            }
            DbBackend::Postgres => pool_options.connect(&database_url).await?,
        };

        let service = Self {
            state: Arc::new(EcsState {
                pool,
                backend,
                database_url,
                has_erc20,
                has_erc721,
                has_erc1155,
                erc20_url,
                erc721_url,
                erc1155_url,
                managed_tables: Mutex::new(None),
                cached_views_sql: RwLock::new(None),
                entity_subscriptions: RwLock::new(EntitySubscriptionRegistry::default()),
                event_message_subscriptions: RwLock::new(EntitySubscriptionRegistry::default()),
                event_subscriptions: Mutex::new(HashMap::new()),
                contract_subscriptions: Mutex::new(HashMap::new()),
                token_subscriptions: Mutex::new(HashMap::new()),
                token_balance_subscriptions: Mutex::new(HashMap::new()),
                token_transfer_subscriptions: Mutex::new(HashMap::new()),
                transaction_subscriptions: Mutex::new(HashMap::new()),
            }),
        };
        service.initialize().await?;
        Ok(service)
    }

    async fn initialize(&self) -> Result<()> {
        if self.state.backend == DbBackend::Sqlite {
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&self.state.pool)
                .await
                .ok();
        }

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_contracts (
                contract_address TEXT PRIMARY KEY,
                contract_type INTEGER NOT NULL,
                head BIGINT,
                tps BIGINT,
                last_block_timestamp BIGINT,
                last_pending_block_tx TEXT,
                updated_at BIGINT NOT NULL,
                created_at BIGINT NOT NULL
            )",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_table_kinds (
                table_id TEXT PRIMARY KEY,
                world_address TEXT NOT NULL,
                kind TEXT NOT NULL,
                updated_at BIGINT NOT NULL
            )",
        )
        .execute(&self.state.pool)
        .await?;

        let entity_meta_sql = match self.state.backend {
            DbBackend::Sqlite => {
                "CREATE TABLE IF NOT EXISTS torii_ecs_entity_meta (
                    kind TEXT NOT NULL,
                    world_address TEXT NOT NULL,
                    table_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    executed_at BIGINT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(kind, world_address, table_id, entity_id)
                )"
            }
            DbBackend::Postgres => {
                "CREATE TABLE IF NOT EXISTS torii_ecs_entity_meta (
                    kind TEXT NOT NULL,
                    world_address TEXT NOT NULL,
                    table_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    executed_at BIGINT NOT NULL,
                    deleted BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY(kind, world_address, table_id, entity_id)
                )"
            }
        };
        sqlx::query(entity_meta_sql)
            .execute(&self.state.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_events (
                event_id TEXT PRIMARY KEY,
                world_address TEXT NOT NULL,
                block_number BIGINT NOT NULL,
                executed_at BIGINT NOT NULL,
                transaction_hash TEXT NOT NULL,
                keys_json TEXT NOT NULL,
                data_json TEXT NOT NULL
            )",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS torii_ecs_entity_models (
                kind TEXT NOT NULL,
                world_address TEXT NOT NULL,
                table_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                row_json TEXT NOT NULL,
                updated_at BIGINT NOT NULL,
                PRIMARY KEY(kind, world_address, table_id, entity_id)
            )",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_lookup_idx
             ON torii_ecs_entity_meta(kind, world_address, entity_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_table_lookup_idx
             ON torii_ecs_entity_meta(kind, table_id, world_address, entity_id, deleted)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_keyset_idx
             ON torii_ecs_entity_meta(kind, deleted, entity_id, world_address)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_models_lookup_idx
             ON torii_ecs_entity_models(kind, world_address, entity_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_models_table_lookup_idx
             ON torii_ecs_entity_models(kind, table_id, world_address, entity_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query("DROP INDEX IF EXISTS torii_ecs_entity_models_key_lookup_idx")
            .execute(&self.state.pool)
            .await
            .ok();

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_events_cursor_idx
             ON torii_ecs_events(event_id)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_page_idx
             ON torii_ecs_entity_meta(kind, deleted, entity_id, world_address, table_id, created_at, updated_at, executed_at)",
        )
        .execute(&self.state.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS torii_ecs_entity_meta_kind_table_page_idx
             ON torii_ecs_entity_meta(kind, deleted, table_id, entity_id, world_address)",
        )
        .execute(&self.state.pool)
        .await?;

        let emh_sql = self.event_messages_historical_view_sql().await?;
        match self.state.backend {
            DbBackend::Sqlite => {
                // sqlite-dynamic-ok: persistent view DDL for event_messages_historical.
                sqlx::query(&format!(
                    "CREATE VIEW IF NOT EXISTS event_messages_historical AS {emh_sql}"
                ))
                .execute(&self.state.pool)
                .await
                .ok();
            }
            DbBackend::Postgres => {
                // sqlite-dynamic-ok: persistent view DDL for event_messages_historical.
                sqlx::query(&format!(
                    "CREATE OR REPLACE VIEW event_messages_historical AS {emh_sql}"
                ))
                .execute(&self.state.pool)
                .await
                .ok();
            }
        }

        Ok(())
    }

    pub async fn attach_erc_databases(&self) -> Result<()> {
        if self.state.backend != DbBackend::Sqlite {
            return Ok(());
        }
        let mut conn = self.acquire_initialized_connection().await?;
        for (schema, url) in [
            ("erc20", &self.state.erc20_url),
            ("erc721", &self.state.erc721_url),
            ("erc1155", &self.state.erc1155_url),
        ] {
            if let Some(url) = url {
                let options = SqliteConnectOptions::from_str(url)?;
                #[allow(clippy::match_bool)]
                #[allow(clippy::single_match_else)]
                match options.is_in_memory() {
                    true => tracing::info!(schema, "Attaching in-memory ERC database"),
                    false => {
                        let path = options.get_filename();
                        let file_exists = path.exists();
                        tracing::info!(
                            schema,
                            path = %path.display(),
                            file_exists,
                            "Attaching ERC database"
                        );
                    }
                }
                attach_sqlite_database(&mut conn, schema, &options).await?;
                match sqlx::query(sqlite_master_preview_sql(schema))
                    .fetch_all(&mut *conn)
                    .await
                {
                    Ok(rows) => {
                        let tables: Vec<String> =
                            rows.iter().filter_map(|r| r.try_get("name").ok()).collect();
                        tracing::info!(schema, ?tables, "Attached ERC database — found tables");
                    }
                    Err(e) => {
                        tracing::error!(
                            schema,
                            error = %e,
                            "Attached ERC database but failed to list tables"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn acquire_initialized_connection(&self) -> Result<PoolConnection<Any>> {
        Ok(self.state.pool.acquire().await?)
    }

    async fn acquire_query_connection(
        &self,
        operation: &'static str,
    ) -> Result<PoolConnection<Any>> {
        let start = Instant::now();
        let conn = self.state.pool.acquire().await?;
        ::metrics::histogram!(
            "torii_ecs_db_pool_acquire_seconds",
            "operation" => operation,
            "backend" => self.state.backend.as_str()
        )
        .record(start.elapsed().as_secs_f64());
        Ok(conn)
    }

    pub async fn record_contract_progress(
        &self,
        contract_address: Felt,
        contract_type: ContractType,
        head: u64,
        executed_at: u64,
        last_pending_block_tx: Option<Felt>,
    ) -> Result<()> {
        let now = Utc::now().timestamp() as u64;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_contracts (
                    contract_address, contract_type, head, tps, last_block_timestamp,
                    last_pending_block_tx, updated_at, created_at
                ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?6)
                ON CONFLICT(contract_address) DO UPDATE SET
                    contract_type = excluded.contract_type,
                    head = excluded.head,
                    last_block_timestamp = excluded.last_block_timestamp,
                    last_pending_block_tx = excluded.last_pending_block_tx,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_contracts (
                    contract_address, contract_type, head, tps, last_block_timestamp,
                    last_pending_block_tx, updated_at, created_at
                ) VALUES ($1, $2, $3, NULL, $4, $5, $6, $6)
                ON CONFLICT(contract_address) DO UPDATE SET
                    contract_type = EXCLUDED.contract_type,
                    head = EXCLUDED.head,
                    last_block_timestamp = EXCLUDED.last_block_timestamp,
                    last_pending_block_tx = EXCLUDED.last_pending_block_tx,
                    updated_at = EXCLUDED.updated_at"
            }
        };
        sqlx::query(sql)
            .bind(felt_hex(contract_address))
            .bind(contract_type as i32)
            .bind(head as i64)
            .bind(executed_at as i64)
            .bind(last_pending_block_tx.map(felt_hex))
            .bind(now as i64)
            .execute(&self.state.pool)
            .await?;

        if self.state.contract_subscriptions.lock().await.is_empty() {
            return Ok(());
        }

        let contract = self
            .load_contracts(&types::ContractQuery {
                contract_addresses: vec![contract_address.into()],
                contract_types: vec![],
            })
            .await?
            .into_iter()
            .next();

        if let Some(contract) = contract {
            self.publish_contract_update(contract).await;
        }
        Ok(())
    }

    pub async fn record_table_kind(
        &self,
        world_address: Felt,
        table_id: Felt,
        kind: TableKind,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_table_kinds (table_id, world_address, kind, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(table_id) DO UPDATE SET
                    world_address = excluded.world_address,
                    kind = excluded.kind,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_table_kinds (table_id, world_address, kind, updated_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(table_id) DO UPDATE SET
                    world_address = EXCLUDED.world_address,
                    kind = EXCLUDED.kind,
                    updated_at = EXCLUDED.updated_at"
            }
        };
        sqlx::query(sql)
            .bind(felt_hex(table_id))
            .bind(felt_hex(world_address))
            .bind(kind.as_str())
            .bind(now)
            .execute(&self.state.pool)
            .await?;
        self.update_managed_table_kind(table_id, world_address, kind)
            .await;
        Ok(())
    }

    pub async fn cache_created_table(&self, world_address: Felt, table: &CreateTable) {
        let schema = TableSchema {
            id: table.id,
            name: table.name.clone(),
            attributes: table.attributes.clone(),
            primary: table.primary.clone(),
            columns: table.columns.clone(),
        };
        self.put_managed_table(world_address, DojoTable::from(schema))
            .await;
    }

    pub async fn cache_updated_table(&self, world_address: Felt, table: &UpdateTable) {
        let schema = TableSchema {
            id: table.id,
            name: table.name.clone(),
            attributes: table.attributes.clone(),
            primary: table.primary.clone(),
            columns: table.columns.clone(),
        };
        self.put_managed_table(world_address, DojoTable::from(schema))
            .await;
    }

    pub async fn upsert_entity_meta(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        entity_id: Felt,
        executed_at: u64,
        deleted: bool,
    ) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_entity_meta (
                    kind, world_address, table_id, entity_id, created_at, updated_at, executed_at, deleted
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?5, ?6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    executed_at = excluded.executed_at,
                    deleted = excluded.deleted"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_entity_meta (
                    kind, world_address, table_id, entity_id, created_at, updated_at, executed_at, deleted
                 ) VALUES ($1, $2, $3, $4, $5, $5, $5, $6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    executed_at = EXCLUDED.executed_at,
                    deleted = EXCLUDED.deleted"
            }
        };
        let mut query = sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .bind(felt_hex(entity_id))
            .bind(executed_at as i64);
        query = match self.state.backend {
            DbBackend::Sqlite => query.bind(i64::from(deleted)),
            DbBackend::Postgres => query.bind(deleted),
        };
        query.execute(&self.state.pool).await?;
        Ok(())
    }

    pub async fn upsert_entity_model(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        columns: &[Felt],
        record: &Record,
        executed_at: u64,
    ) -> Result<()> {
        let Some(table) = self.load_managed_table(table_id).await? else {
            return Ok(());
        };
        let row = record_to_json_map(&table.table, columns, record)?;
        let row_json = serde_json::to_string(&row)?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_entity_models (
                    kind, world_address, table_id, entity_id, row_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    row_json = excluded.row_json,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_entity_models (
                    kind, world_address, table_id, entity_id, row_json, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                    row_json = EXCLUDED.row_json,
                    updated_at = EXCLUDED.updated_at"
            }
        };
        sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .bind(felt_hex(record.id))
            .bind(row_json)
            .bind(executed_at as i64)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_entity_model(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        entity_id: Felt,
    ) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "DELETE FROM torii_ecs_entity_models
                 WHERE kind = ?1 AND world_address = ?2 AND table_id = ?3 AND entity_id = ?4"
            }
            DbBackend::Postgres => {
                "DELETE FROM torii_ecs_entity_models
                 WHERE kind = $1 AND world_address = $2 AND table_id = $3 AND entity_id = $4"
            }
        };
        sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .bind(felt_hex(entity_id))
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    pub async fn table_kind(&self, table_id: Felt) -> Result<TableKind> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => "SELECT kind FROM torii_ecs_table_kinds WHERE table_id = ?1",
            DbBackend::Postgres => "SELECT kind FROM torii_ecs_table_kinds WHERE table_id = $1",
        };
        let row = sqlx::query(sql)
            .bind(felt_hex(table_id))
            .fetch_optional(&self.state.pool)
            .await?;
        Ok(row
            .and_then(|row| row.try_get::<String, _>("kind").ok())
            .and_then(|kind| TableKind::from_str(&kind))
            .unwrap_or(TableKind::Entity))
    }

    pub async fn store_event(
        &self,
        world_address: Felt,
        transaction_hash: Felt,
        block_number: u64,
        executed_at: u64,
        keys: &[Felt],
        data: &[Felt],
        ordinal: usize,
    ) -> Result<()> {
        let event_id = format!(
            "{}:{}:{}",
            felt_hex(world_address),
            felt_hex(transaction_hash),
            ordinal
        );
        let keys_json =
            serde_json::to_string(&keys.iter().map(|felt| felt_hex(*felt)).collect::<Vec<_>>())?;
        let data_json =
            serde_json::to_string(&data.iter().map(|felt| felt_hex(*felt)).collect::<Vec<_>>())?;

        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_ecs_events (
                    event_id, world_address, block_number, executed_at, transaction_hash, keys_json, data_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(event_id) DO NOTHING"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_ecs_events (
                    event_id, world_address, block_number, executed_at, transaction_hash, keys_json, data_json
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT(event_id) DO NOTHING"
            }
        };
        sqlx::query(sql)
            .bind(event_id)
            .bind(felt_hex(world_address))
            .bind(block_number as i64)
            .bind(executed_at as i64)
            .bind(felt_hex(transaction_hash))
            .bind(keys_json)
            .bind(data_json)
            .execute(&self.state.pool)
            .await?;

        self.publish_event_update(types::Event {
            keys: keys.iter().map(Into::into).collect(),
            data: data.iter().map(Into::into).collect(),
            transaction_hash: transaction_hash.into(),
        })
        .await;

        Ok(())
    }

    pub async fn publish_entity_update(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<()> {
        let start = Instant::now();
        let subscriptions = self.state.entity_subscription_registry(kind);
        let group_snapshots = {
            let subscriptions = subscriptions.read().await;
            if subscriptions.is_empty() {
                return Ok(());
            }
            subscriptions.snapshot_groups()
        };

        let entity = self
            .load_entity_by_id(kind, world_address, entity_id)
            .await?
            .map(EntityAggregate::into_proto);
        let Some(entity) = entity else {
            return Ok(());
        };

        let checked_groups = group_snapshots.len();
        let checked_subscribers = group_snapshots
            .iter()
            .map(|group| group.senders.len())
            .sum::<usize>();
        let targets = group_snapshots
            .into_iter()
            .filter_map(|group| {
                if !group.world_addresses.is_empty()
                    && !group.world_addresses.contains(&entity.world_address)
                {
                    return None;
                }
                if let Some(clause) = &group.clause {
                    if !entity_matches_clause(&entity, clause) {
                        return None;
                    }
                }
                Some(group.senders)
            })
            .collect::<Vec<_>>();
        let matched_groups = targets.len();
        let matched_subscribers = targets.iter().map(Vec::len).sum::<usize>();

        let mut closed = Vec::new();
        for group_targets in targets {
            for (subscription_id, sender) in group_targets {
                if let Err(err) = sender.try_send(Ok(SubscribeEntityResponse {
                    entity: Some(entity.clone()),
                    subscription_id,
                })) {
                    if matches!(err, TrySendError::Closed(_)) {
                        closed.push(subscription_id);
                    }
                }
            }
        }

        if !closed.is_empty() {
            for subscription_id in closed {
                let _ = self
                    .state
                    .remove_entity_subscription(kind, subscription_id)
                    .await;
            }
        }
        ::metrics::histogram!("torii_ecs_publish_entity_update_seconds")
            .record(start.elapsed().as_secs_f64());
        ::metrics::counter!("torii_ecs_publish_entity_subscription_groups_checked_total")
            .increment(checked_groups as u64);
        ::metrics::counter!("torii_ecs_publish_entity_subscription_groups_matched_total")
            .increment(matched_groups as u64);
        ::metrics::counter!("torii_ecs_publish_entity_subscribers_checked_total")
            .increment(checked_subscribers as u64);
        ::metrics::counter!("torii_ecs_publish_entity_subscribers_matched_total")
            .increment(matched_subscribers as u64);
        Ok(())
    }

    async fn publish_event_update(&self, event: types::Event) {
        let start = Instant::now();
        let (checked, targets) = {
            let subscriptions = self.state.event_subscriptions.lock().await;
            if subscriptions.is_empty() {
                return;
            }
            let checked = subscriptions.len();
            let targets = subscriptions
                .iter()
                .filter_map(|(&subscription_id, subscription)| {
                    if !match_keys(&event.keys, &subscription.keys) {
                        return None;
                    }
                    Some((subscription_id, subscription.sender.clone()))
                })
                .collect::<Vec<_>>();
            (checked, targets)
        };
        let matched = targets.len();

        let mut closed = Vec::new();
        for (subscription_id, sender) in targets {
            if let Err(err) = sender.try_send(Ok(SubscribeEventsResponse {
                event: Some(event.clone()),
            })) {
                if matches!(err, TrySendError::Closed(_)) {
                    closed.push(subscription_id);
                }
            }
        }

        if !closed.is_empty() {
            let mut subscriptions = self.state.event_subscriptions.lock().await;
            for subscription_id in closed {
                subscriptions.remove(&subscription_id);
            }
        }
        ::metrics::histogram!("torii_ecs_publish_event_update_seconds")
            .record(start.elapsed().as_secs_f64());
        ::metrics::counter!("torii_ecs_publish_event_subscribers_checked_total")
            .increment(checked as u64);
        ::metrics::counter!("torii_ecs_publish_event_subscribers_matched_total")
            .increment(matched as u64);
    }

    async fn publish_contract_update(&self, contract: types::Contract) {
        let start = Instant::now();
        let (checked, targets) = {
            let subscriptions = self.state.contract_subscriptions.lock().await;
            if subscriptions.is_empty() {
                return;
            }
            let checked = subscriptions.len();
            let targets = subscriptions
                .iter()
                .filter_map(|(&subscription_id, subscription)| {
                    if !contract_matches_query(&contract, &subscription.query) {
                        return None;
                    }
                    Some((subscription_id, subscription.sender.clone()))
                })
                .collect::<Vec<_>>();
            (checked, targets)
        };
        let matched = targets.len();

        let mut closed = Vec::new();
        for (subscription_id, sender) in targets {
            if let Err(err) = sender.try_send(Ok(SubscribeContractsResponse {
                contract: Some(contract.clone()),
            })) {
                if matches!(err, TrySendError::Closed(_)) {
                    closed.push(subscription_id);
                }
            }
        }

        if !closed.is_empty() {
            let mut subscriptions = self.state.contract_subscriptions.lock().await;
            for subscription_id in closed {
                subscriptions.remove(&subscription_id);
            }
        }
        ::metrics::histogram!("torii_ecs_publish_contract_update_seconds")
            .record(start.elapsed().as_secs_f64());
        ::metrics::counter!("torii_ecs_publish_contract_subscribers_checked_total")
            .increment(checked as u64);
        ::metrics::counter!("torii_ecs_publish_contract_subscribers_matched_total")
            .increment(matched as u64);
    }

    async fn load_contracts(&self, query: &types::ContractQuery) -> Result<Vec<types::Contract>> {
        let mut builder = QueryBuilder::<Any>::new(
            "SELECT contract_address, contract_type, head, tps, last_block_timestamp, \
                    last_pending_block_tx, updated_at, created_at \
             FROM torii_ecs_contracts",
        );
        if !query.contract_addresses.is_empty() || !query.contract_types.is_empty() {
            builder.push(" WHERE ");
            if !query.contract_addresses.is_empty() {
                builder.push("contract_address IN (");
                {
                    let mut separated = builder.separated(", ");
                    for address in &query.contract_addresses {
                        separated.push_bind(felt_hex(Felt::from_be_bytes_slice(address)?));
                    }
                }
                builder.push(")");
                if !query.contract_types.is_empty() {
                    builder.push(" AND ");
                }
            }
            if !query.contract_types.is_empty() {
                builder.push("contract_type IN (");
                {
                    let mut separated = builder.separated(", ");
                    for contract_type in &query.contract_types {
                        separated.push_bind(*contract_type);
                    }
                }
                builder.push(")");
            }
        }

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        let mut contracts = Vec::new();
        for row in rows {
            let contract_address = row.try_get::<String, _>("contract_address")?;
            let contract_type = row.try_get::<i32, _>("contract_type")?;
            let contract = types::Contract {
                contract_address: felt_from_hex(&contract_address)?.into(),
                contract_type,
                head: row
                    .try_get::<Option<i64>, _>("head")?
                    .map(|value| value as u64),
                tps: row
                    .try_get::<Option<i64>, _>("tps")?
                    .map(|value| value as u64),
                last_block_timestamp: row
                    .try_get::<Option<i64>, _>("last_block_timestamp")?
                    .map(|value| value as u64),
                last_pending_block_tx: row
                    .try_get::<Option<String>, _>("last_pending_block_tx")?
                    .map(|value| felt_from_hex(&value))
                    .transpose()?
                    .map(Into::into),
                updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                created_at: row.try_get::<i64, _>("created_at")? as u64,
            };
            contracts.push(contract);
        }
        Ok(contracts)
    }

    async fn load_controllers(
        &self,
        query: &types::ControllerQuery,
    ) -> Result<(Vec<types::Controller>, String)> {
        let pagination = query.pagination.clone().unwrap_or(types::Pagination {
            cursor: String::new(),
            limit: 100,
            direction: PaginationDirection::Forward as i32,
            order_by: Vec::new(),
        });
        let limit = Self::pagination_limit_with_max(Some(&pagination), 100, None);
        let target_limit = limit.saturating_add(1);
        let direction_is_backward = pagination.direction == PaginationDirection::Backward as i32;
        let order_sql = if direction_is_backward { "DESC" } else { "ASC" };

        let mut conn = self.acquire_initialized_connection().await?;
        self.ensure_sql_compat_views_on_connection(&mut conn)
            .await?;

        let mut builder =
            QueryBuilder::<Any>::new("SELECT address, username, deployed_at FROM controllers");
        let mut has_where = if query.contract_addresses.is_empty() {
            false
        } else {
            builder.push(" WHERE address IN (");
            {
                let mut separated = builder.separated(", ");
                for address in &query.contract_addresses {
                    separated.push_bind(felt_hex(Felt::from_be_bytes_slice(address)?));
                }
            }
            builder.push(")");
            true
        };

        if !query.usernames.is_empty() {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
                has_where = true;
            }
            builder.push("username IN (");
            {
                let mut separated = builder.separated(", ");
                for username in &query.usernames {
                    separated.push_bind(username.clone());
                }
            }
            builder.push(")");
        }

        if !pagination.cursor.is_empty() {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
            }
            builder.push("LOWER(SUBSTR(address, 3)) ");
            builder.push(if direction_is_backward { "< " } else { "> " });
            builder.push_bind(
                pagination
                    .cursor
                    .trim_start_matches("0x")
                    .trim_start_matches("0X")
                    .to_ascii_lowercase(),
            );
        }

        builder.push(" ORDER BY address ");
        builder.push(order_sql);
        builder.push(" LIMIT ");
        builder.push_bind(target_limit as i64);

        let rows = builder.build().fetch_all(&mut *conn).await?;
        let mut controllers = rows
            .into_iter()
            .map(|row| {
                let address = row.try_get::<String, _>("address")?;
                let deployed_at_timestamp = row
                    .try_get::<Option<String>, _>("deployed_at")?
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
                    .map(|value| value.timestamp() as u64)
                    .unwrap_or_default();
                Ok(types::Controller {
                    address: felt_from_hex(&address)?.into(),
                    username: row
                        .try_get::<Option<String>, _>("username")?
                        .unwrap_or_default(),
                    deployed_at_timestamp,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let has_more = controllers.len() > limit;
        if has_more {
            controllers.truncate(limit);
        }
        if direction_is_backward {
            controllers.reverse();
        }

        let next_cursor = if has_more {
            controllers
                .last()
                .map(|controller| hex::encode(&controller.address))
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok((controllers, next_cursor))
    }

    fn scoped_table(&self, schema: &str, table: &str) -> String {
        format!("{schema}.{table}")
    }

    async fn sql_object_exists_on_connection(
        &self,
        conn: &mut PoolConnection<Any>,
        schema: Option<&str>,
        name: &str,
    ) -> Result<bool> {
        match self.state.backend {
            DbBackend::Sqlite => {
                let query = match schema {
                    Some(schema) => format!(
                        "SELECT 1 FROM {schema}.sqlite_master \
                         WHERE type IN ('table', 'view') AND name = ? LIMIT 1"
                    ),
                    None => "SELECT 1 FROM sqlite_master \
                             WHERE type IN ('table', 'view') AND name = ? LIMIT 1"
                        .to_string(),
                };

                let exists = sqlx::query_scalar::<Any, i64>(&query)
                    .bind(name)
                    .fetch_optional(&mut **conn)
                    .await;
                match exists {
                    Ok(row) => Ok(row.is_some()),
                    Err(_) if schema.is_some() => Ok(false),
                    Err(error) => Err(error.into()),
                }
            }
            DbBackend::Postgres => {
                let schema = schema.unwrap_or("public");
                let exists = sqlx::query_scalar::<Any, i64>(
                    "SELECT 1
                     FROM (
                        SELECT table_name AS object_name
                        FROM information_schema.tables
                        WHERE table_schema = $1
                        UNION ALL
                        SELECT table_name AS object_name
                        FROM information_schema.views
                        WHERE table_schema = $1
                     ) objects
                     WHERE object_name = $2
                     LIMIT 1",
                )
                .bind(schema)
                .bind(name)
                .fetch_optional(&mut **conn)
                .await?;
                Ok(exists.is_some())
            }
        }
    }

    async fn replace_sql_compat_view_on_connection(
        &self,
        conn: &mut PoolConnection<Any>,
        view_name: &str,
        select_sql: &str,
    ) -> Result<()> {
        match self.state.backend {
            DbBackend::Sqlite => {
                // sqlite-dynamic-ok: view DDL requires the target view identifier in SQL text.
                sqlx::query(&format!("DROP VIEW IF EXISTS {view_name}"))
                    .execute(&mut **conn)
                    .await?;
                // sqlite-dynamic-ok: view DDL requires the target view identifier and SELECT body in SQL text.
                sqlx::query(&format!("CREATE TEMP VIEW {view_name} AS {select_sql}"))
                    .execute(&mut **conn)
                    .await?;
            }
            DbBackend::Postgres => {
                // sqlite-dynamic-ok: compatibility view DDL is an intentional dynamic exception.
                sqlx::query(&format!(
                    "CREATE OR REPLACE VIEW {view_name} AS {select_sql}"
                ))
                .execute(&mut **conn)
                .await?;
            }
        }
        Ok(())
    }

    async fn compat_view_exists_on_connection(
        &self,
        conn: &mut PoolConnection<Any>,
        name: &str,
    ) -> Result<bool> {
        if self
            .sql_object_exists_on_connection(conn, None, name)
            .await?
        {
            return Ok(true);
        }
        if self.state.backend == DbBackend::Sqlite {
            return self
                .sql_object_exists_on_connection(conn, Some("temp"), name)
                .await;
        }
        Ok(false)
    }

    async fn ensure_sql_compat_views_on_connection(
        &self,
        conn: &mut PoolConnection<Any>,
    ) -> Result<()> {
        {
            let cached = self.state.cached_views_sql.read().await;
            if let Some(views) = cached.as_ref() {
                if !self
                    .compat_view_exists_on_connection(conn, "controllers")
                    .await?
                {
                    self.replace_sql_compat_view_on_connection(
                        conn,
                        "controllers",
                        &views.controllers,
                    )
                    .await?;
                }
                if !self
                    .compat_view_exists_on_connection(conn, "token_balances")
                    .await?
                {
                    self.replace_sql_compat_view_on_connection(
                        conn,
                        "token_balances",
                        &views.token_balances,
                    )
                    .await?;
                }
                return Ok(());
            }
        }

        let controllers_sql = self.controllers_view_sql(conn).await?;
        let token_balances_sql = self.token_balances_view_sql(conn).await?;

        if !self
            .compat_view_exists_on_connection(conn, "controllers")
            .await?
        {
            self.replace_sql_compat_view_on_connection(conn, "controllers", &controllers_sql)
                .await?;
        }
        if !self
            .compat_view_exists_on_connection(conn, "token_balances")
            .await?
        {
            self.replace_sql_compat_view_on_connection(conn, "token_balances", &token_balances_sql)
                .await?;
        }

        let mut cached = self.state.cached_views_sql.write().await;
        *cached = Some(CachedViewsSql {
            controllers: controllers_sql,
            token_balances: token_balances_sql,
        });
        Ok(())
    }

    async fn controllers_view_sql(&self, conn: &mut PoolConnection<Any>) -> Result<String> {
        let mut address_sources = Vec::new();
        for (table, column) in [
            ("NUMS-LeaderboardScore", "player"),
            ("NUMS-Claimed", "player_id"),
            ("NUMS-BundleIssuance", "recipient"),
        ] {
            if self
                .sql_object_exists_on_connection(conn, None, table)
                .await?
            {
                address_sources.push(format!(
                    "SELECT {column} AS address FROM \"{table}\" \
                     WHERE {column} IS NOT NULL AND {column} != ''"
                ));
            }
        }

        let source_sql = if address_sources.is_empty() {
            "SELECT NULL AS address WHERE 1 = 0".to_string()
        } else {
            address_sources.join(" UNION ")
        };

        Ok(format!(
            "SELECT address, address AS username, address AS id, NULL AS deployed_at \
             FROM ({source_sql}) controller_addresses"
        ))
    }

    async fn event_messages_historical_view_sql(&self) -> Result<String> {
        let executed_at = match self.state.backend {
            DbBackend::Sqlite => "datetime(meta.executed_at, 'unixepoch')",
            DbBackend::Postgres => "TO_TIMESTAMP(meta.executed_at)::TEXT",
        };
        let created_at = match self.state.backend {
            DbBackend::Sqlite => "datetime(meta.created_at, 'unixepoch')",
            DbBackend::Postgres => "TO_TIMESTAMP(meta.created_at)::TEXT",
        };
        let updated_at = match self.state.backend {
            DbBackend::Sqlite => "datetime(meta.updated_at, 'unixepoch')",
            DbBackend::Postgres => "TO_TIMESTAMP(meta.updated_at)::TEXT",
        };
        let deleted_predicate = match self.state.backend {
            DbBackend::Sqlite => "meta.deleted = 0",
            DbBackend::Postgres => "meta.deleted = FALSE",
        };
        let recipient = match self.state.backend {
            DbBackend::Sqlite => "COALESCE(json_extract(models.row_json, '$.recipient'), '')",
            DbBackend::Postgres => "COALESCE(models.row_json::jsonb ->> 'recipient', '')",
        };

        Ok(format!(
            "SELECT \
                models.world_address || ':' || models.entity_id AS id, \
                models.world_address || ':' || models.entity_id AS event_id, \
                models.world_address || ':' || models.table_id AS model_id, \
                models.entity_id, \
                {recipient} || '/' AS keys, \
                models.row_json AS data, \
                models.world_address, \
                {created_at} AS created_at, \
                {updated_at} AS updated_at, \
                {executed_at} AS executed_at \
             FROM torii_ecs_entity_models models \
             JOIN torii_ecs_entity_meta meta
               ON meta.kind = models.kind
              AND meta.world_address = models.world_address
              AND meta.table_id = models.table_id
              AND meta.entity_id = models.entity_id
             WHERE models.kind = 'event_message'
               AND {deleted_predicate}"
        ))
    }

    async fn token_balances_view_sql(&self, conn: &mut PoolConnection<Any>) -> Result<String> {
        let mut parts = Vec::new();

        if self
            .sql_object_exists_on_connection(conn, Some("erc20"), "balances")
            .await?
        {
            let token = sql_hex_blob_expr(self.state.backend, "token", 64);
            let wallet = sql_hex_blob_expr(self.state.backend, "wallet", 64);
            let balance = sql_hex_blob_expr(self.state.backend, "balance", 64);
            parts.push(format!(
                "SELECT \
                    {wallet} || '/' || {token} AS id, \
                    {wallet} AS account_address, \
                    {token} AS contract_address, \
                    {token} AS token_id, \
                    {balance} AS balance \
                 FROM {}",
                self.scoped_table("erc20", "balances")
            ));
        }

        if self
            .sql_object_exists_on_connection(conn, Some("erc721"), "nft_ownership")
            .await?
        {
            let token = sql_hex_blob_expr(self.state.backend, "token", 64);
            let owner = sql_hex_blob_expr(self.state.backend, "owner", 64);
            let token_id = sql_hex_blob_expr(self.state.backend, "token_id", 64);
            let one = sql_hex_literal_expr(self.state.backend, "1", 64);
            parts.push(format!(
                "SELECT \
                    {owner} || '/' || {token} || ':' || {token_id} AS id, \
                    {owner} AS account_address, \
                    {token} AS contract_address, \
                    {token_id} AS token_id, \
                    {one} AS balance \
                 FROM {}",
                self.scoped_table("erc721", "nft_ownership")
            ));
        }

        if self
            .sql_object_exists_on_connection(conn, Some("erc1155"), "erc1155_balances")
            .await?
        {
            let contract = sql_hex_blob_expr(self.state.backend, "contract", 64);
            let wallet = sql_hex_blob_expr(self.state.backend, "wallet", 64);
            let token_id = sql_hex_blob_expr(self.state.backend, "token_id", 64);
            let balance = sql_hex_blob_expr(self.state.backend, "balance", 64);
            parts.push(format!(
                "SELECT \
                    {wallet} || '/' || {contract} || ':' || {token_id} AS id, \
                    {wallet} AS account_address, \
                    {contract} AS contract_address, \
                    {token_id} AS token_id, \
                    {balance} AS balance \
                 FROM {}",
                self.scoped_table("erc1155", "erc1155_balances")
            ));
        }

        if parts.is_empty() {
            Ok("SELECT \
                    CAST(NULL AS TEXT) AS id, \
                    CAST(NULL AS TEXT) AS account_address, \
                    CAST(NULL AS TEXT) AS contract_address, \
                    CAST(NULL AS TEXT) AS token_id, \
                    CAST(NULL AS TEXT) AS balance \
                 WHERE 1 = 0"
                .to_string())
        } else {
            Ok(parts.join(" UNION ALL "))
        }
    }

    fn pagination_limit(pagination: Option<&types::Pagination>, default: usize) -> usize {
        Self::pagination_limit_with_max(pagination, default, Some(1000))
    }

    fn pagination_limit_with_max(
        pagination: Option<&types::Pagination>,
        default: usize,
        max: Option<usize>,
    ) -> usize {
        pagination
            .map(|pagination| pagination.limit)
            .filter(|limit| *limit > 0)
            .map_or(default, |limit| limit as usize)
            .min(max.unwrap_or(usize::MAX))
    }

    async fn load_token_contracts(
        &self,
        query: &types::TokenContractQuery,
    ) -> Result<(Vec<types::TokenContract>, String)> {
        let limit = Self::pagination_limit(query.pagination.as_ref(), 100);
        let mut items = Vec::new();
        let mut conn = self.acquire_initialized_connection().await?;
        let include_erc20 = self.state.has_erc20
            && (query.contract_types.is_empty()
                || query.contract_types.contains(&(ContractType::Erc20 as i32)));
        let include_erc721 = self.state.has_erc721
            && (query.contract_types.is_empty()
                || query
                    .contract_types
                    .contains(&(ContractType::Erc721 as i32)));
        let include_erc1155 = self.state.has_erc1155
            && (query.contract_types.is_empty()
                || query
                    .contract_types
                    .contains(&(ContractType::Erc1155 as i32)));

        if include_erc20 {
            let table = self.scoped_table("erc20", "token_metadata");
            let mut builder = QueryBuilder::<Any>::new(format!(
                "SELECT token, name, symbol, decimals, total_supply FROM {table}"
            ));
            push_blob_in_filter(&mut builder, "token", &query.contract_addresses);
            match builder.build().fetch_all(&mut *conn).await {
                Ok(rows) => {
                    for row in rows {
                        let contract_address =
                            Felt::from_be_bytes_slice(&row.try_get::<Vec<u8>, _>("token")?)?
                                .to_be_bytes_vec(); // Why not just get vec?
                        items.push(types::TokenContract {
                            contract_address,
                            contract_type: ContractType::Erc20 as i32,
                            name: row
                                .try_get::<Option<String>, _>("name")?
                                .unwrap_or_default(),
                            symbol: row
                                .try_get::<Option<String>, _>("symbol")?
                                .unwrap_or_default(),
                            decimals: row
                                .try_get::<Option<String>, _>("decimals")?
                                .and_then(|s| s.parse::<u32>().ok())
                                .unwrap_or_default(),
                            metadata: Vec::new(),
                            total_supply: canonical_optional_u256_bytes_from_db(
                                row.try_get::<Option<Vec<u8>>, _>("total_supply")?,
                            )?,
                            traits: String::new(),
                            token_metadata: Vec::new(),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to query erc20.token_metadata — returning empty ERC20 contracts"
                    );
                }
            }
        }

        if include_erc721 {
            let table = self.scoped_table("erc721", "token_metadata");
            let uris = self.scoped_table("erc721", "token_uris");
            let ownership = self.scoped_table("erc721", "nft_ownership");
            let mut builder = QueryBuilder::<Any>::new(format!(
                "SELECT tm.token, tm.name, tm.symbol, tm.total_supply, \
                        (SELECT COUNT(DISTINCT o.token_id) FROM {ownership} o WHERE o.token = tm.token) AS ownership_count, \
                        COALESCE((SELECT metadata_json FROM {uris} tu WHERE tu.token = tm.token ORDER BY tu.token_id LIMIT 1), '') AS token_metadata \
                 FROM {table} tm"
            ));
            push_blob_in_filter(&mut builder, "tm.token", &query.contract_addresses);
            for row in builder.build().fetch_all(&mut *conn).await? {
                let contract_address = row.try_get::<Vec<u8>, _>("token")?; // TODO: can it just be a vec?
                let total_supply = canonical_optional_u256_bytes_from_db(
                    row.try_get::<Option<Vec<u8>>, _>("total_supply")?,
                )?
                .or_else(|| {
                    row.try_get::<Option<i64>, _>("ownership_count")
                        .ok()
                        .flatten()
                        .map(|c| u256_bytes_from_u64(c as u64))
                });
                items.push(types::TokenContract {
                    contract_address,
                    contract_type: ContractType::Erc721 as i32,
                    name: row
                        .try_get::<Option<String>, _>("name")?
                        .unwrap_or_default(),
                    symbol: row
                        .try_get::<Option<String>, _>("symbol")?
                        .unwrap_or_default(),
                    decimals: 0,
                    metadata: Vec::new(),
                    total_supply,
                    traits: String::new(),
                    token_metadata: row
                        .try_get::<Option<String>, _>("token_metadata")?
                        .unwrap_or_default()
                        .into_bytes(),
                });
            }
        }

        if include_erc1155 {
            let table = self.scoped_table("erc1155", "token_metadata");
            let uris = self.scoped_table("erc1155", "token_uris");
            let mut builder = QueryBuilder::<Any>::new(format!(
                "SELECT tm.token, tm.name, tm.symbol, tm.total_supply, \
                        COALESCE((SELECT metadata_json FROM {uris} tu WHERE tu.token = tm.token ORDER BY tu.token_id LIMIT 1), '') AS token_metadata \
                 FROM {table} tm"
            ));
            push_blob_in_filter(&mut builder, "tm.token", &query.contract_addresses);
            for row in builder.build().fetch_all(&mut *conn).await? {
                let contract_address = row.try_get::<Vec<u8>, _>("token")?; //TODO: Can it just be a vec?
                items.push(types::TokenContract {
                    contract_address,
                    contract_type: ContractType::Erc1155 as i32,
                    name: row
                        .try_get::<Option<String>, _>("name")?
                        .unwrap_or_default(),
                    symbol: row
                        .try_get::<Option<String>, _>("symbol")?
                        .unwrap_or_default(),
                    decimals: 0,
                    metadata: Vec::new(),
                    total_supply: canonical_optional_u256_bytes_from_db(
                        row.try_get::<Option<Vec<u8>>, _>("total_supply")?,
                    )?,
                    traits: String::new(),
                    token_metadata: row
                        .try_get::<Option<String>, _>("token_metadata")?
                        .unwrap_or_default()
                        .into_bytes(),
                });
            }
        }

        items.sort_by(|a, b| a.contract_address.cmp(&b.contract_address));
        items.truncate(limit);
        Ok((items, String::new()))
    }

    async fn load_tokens(&self, query: &types::TokenQuery) -> Result<(Vec<types::Token>, String)> {
        let limit = Self::pagination_limit(query.pagination.as_ref(), 100);
        let mut items = Vec::new();
        let mut conn = self.acquire_initialized_connection().await?;

        if self.state.has_erc20 && query.token_ids.is_empty() {
            let table = self.scoped_table("erc20", "token_metadata");
            let mut builder = QueryBuilder::<Any>::new(format!(
                "SELECT token, name, symbol, decimals FROM {table}"
            ));
            push_blob_in_filter(&mut builder, "token", &query.contract_addresses);
            for row in builder.build().fetch_all(&mut *conn).await? {
                let contract_address = row.try_get::<Vec<u8>, _>("token")?; //TODO: Can it just be a vec?
                items.push(types::Token {
                    token_id: None,
                    contract_address,
                    name: row
                        .try_get::<Option<String>, _>("name")?
                        .unwrap_or_default(),
                    symbol: row
                        .try_get::<Option<String>, _>("symbol")?
                        .unwrap_or_default(),
                    decimals: row
                        .try_get::<Option<String>, _>("decimals")?
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or_default(),
                    metadata: Vec::new(),
                    total_supply: None,
                });
            }
        }

        if self.state.has_erc721 {
            items.extend(
                self.load_nft_tokens(&mut conn, "erc721", query, ContractType::Erc721)
                    .await?,
            );
        }
        if self.state.has_erc1155 {
            items.extend(
                self.load_nft_tokens(&mut conn, "erc1155", query, ContractType::Erc1155)
                    .await?,
            );
        }
        items.sort_by(|a, b| {
            a.contract_address
                .cmp(&b.contract_address)
                .then_with(|| a.token_id.cmp(&b.token_id))
        });
        items.truncate(limit);
        Ok((items, String::new()))
    }

    async fn load_nft_tokens(
        &self,
        conn: &mut PoolConnection<Any>,
        schema: &str,
        query: &types::TokenQuery,
        _kind: ContractType,
    ) -> Result<Vec<types::Token>> {
        let token_metadata = self.scoped_table(schema, "token_metadata");
        let token_uris = self.scoped_table(schema, "token_uris");
        let token_attributes = self.scoped_table(schema, "token_attributes");
        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT tu.token, tu.token_id, tu.metadata_json, tm.name, tm.symbol, tm.total_supply \
             FROM {token_uris} tu \
             LEFT JOIN {token_metadata} tm ON tm.token = tu.token"
        ));
        let mut has_where = false;
        has_where |= push_blob_in_filter(&mut builder, "tu.token", &query.contract_addresses);
        if !query.token_ids.is_empty() {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
                has_where = true;
            }
            builder.push("hex(tu.token_id) IN (");
            {
                let mut separated = builder.separated(", ");
                for token_id in &query.token_ids {
                    separated.push_bind(hex::encode_upper(token_id));
                }
            }
            builder.push(")");
        }
        for (index, filter) in query.attribute_filters.iter().enumerate() {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
                has_where = true;
            }
            builder.push(format!(
                "EXISTS (SELECT 1 FROM {token_attributes} ta{index} WHERE ta{index}.token = tu.token AND ta{index}.token_id = tu.token_id AND ta{index}.key = "
            ));
            builder.push_bind(filter.trait_name.clone());
            builder.push(" AND ta");
            builder.push(index.to_string());
            builder.push(".value = ");
            builder.push_bind(filter.trait_value.clone());
            builder.push(")");
        }

        let rows = builder.build().fetch_all(&mut **conn).await?;
        rows.into_iter()
            .map(|row| {
                let token_id =
                    canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("token_id")?)?;
                let contract_address = row.try_get::<Vec<u8>, _>("token")?; // TODO: can it just be a vec?
                Ok(types::Token {
                    token_id: Some(token_id),
                    contract_address,
                    name: row
                        .try_get::<Option<String>, _>("name")?
                        .unwrap_or_default(),
                    symbol: row
                        .try_get::<Option<String>, _>("symbol")?
                        .unwrap_or_default(),
                    decimals: 0,
                    metadata: row
                        .try_get::<Option<String>, _>("metadata_json")?
                        .unwrap_or_default()
                        .into_bytes(),
                    total_supply: canonical_optional_u256_bytes_from_db(
                        row.try_get::<Option<Vec<u8>>, _>("total_supply")?,
                    )?,
                })
            })
            .collect()
    }

    async fn load_token_balances(
        &self,
        query: &types::TokenBalanceQuery,
    ) -> Result<(Vec<types::TokenBalance>, String)> {
        let limit = Self::pagination_limit(query.pagination.as_ref(), 100);
        let mut items = Vec::with_capacity(limit.saturating_mul(3));
        let acquire_start = Instant::now();
        let mut conn = self.acquire_initialized_connection().await?;
        ::metrics::histogram!(
            "torii_ecs_retrieve_token_balances_stage_seconds",
            "stage" => "acquire",
            "backend" => self.state.backend.as_str()
        )
        .record(acquire_start.elapsed().as_secs_f64());
        tracing::debug!(
            target: "torii::ecs_sink",
            limit,
            account_filters = query.account_addresses.len(),
            contract_filters = query.contract_addresses.len(),
            token_id_filters = query.token_ids.len(),
            acquire_ms = acquire_start.elapsed().as_millis(),
            "RetrieveTokenBalances acquired initialized connection"
        );

        let normalized_token_ids = normalize_token_filter_values(&query.token_ids);

        if self.state.has_erc20 {
            let table = self.scoped_table("erc20", "balances");
            let mut builder =
                QueryBuilder::<Any>::new(format!("SELECT wallet, token, balance FROM {table}"));
            let mut has_where = false;
            has_where = push_blob_in_filter_with_mode(
                &mut builder,
                "wallet",
                &query.account_addresses,
                has_where,
            );
            push_blob_in_filter_with_mode(
                &mut builder,
                "token",
                &query.contract_addresses,
                has_where,
            );
            builder.push(" ORDER BY token ASC, wallet ASC");
            builder.push(" LIMIT ");
            builder.push_bind(limit as i64);

            let query_start = Instant::now();
            let result = builder.build().fetch_all(&mut *conn).await;
            ::metrics::histogram!(
                "torii_ecs_retrieve_token_balances_stage_seconds",
                "stage" => "erc20_query",
                "backend" => self.state.backend.as_str()
            )
            .record(query_start.elapsed().as_secs_f64());
            match result {
                Ok(rows) => {
                    tracing::debug!(
                        target: "torii::ecs_sink",
                        rows = rows.len(),
                        elapsed_ms = query_start.elapsed().as_millis(),
                        "RetrieveTokenBalances queried ERC20 balances"
                    );
                    for row in rows {
                        let balance =
                            canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("balance")?)?;
                        let account_address = row.try_get::<Vec<u8>, _>("wallet")?; // TODO: can it just be a vec?
                        let contract_address = row.try_get::<Vec<u8>, _>("token")?; // TODO: can it just be a vec?
                        items.push(types::TokenBalance {
                            balance,
                            account_address,
                            contract_address,
                            token_id: None,
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "torii::ecs_sink",
                        error = %e,
                        "Failed to query erc20.balances — returning empty ERC20 balances"
                    );
                }
            }
        }

        if self.state.has_erc721 {
            let table = self.scoped_table("erc721", "nft_ownership");
            let mut builder =
                QueryBuilder::<Any>::new(format!("SELECT owner, token, token_id FROM {table}"));
            let mut has_where = false;
            has_where = push_blob_in_filter_with_mode(
                &mut builder,
                "owner",
                &query.account_addresses,
                has_where,
            );
            has_where = push_blob_in_filter_with_mode(
                &mut builder,
                "token",
                &query.contract_addresses,
                has_where,
            );
            push_blob_in_filter_with_mode(
                &mut builder,
                "token_id",
                &normalized_token_ids,
                has_where,
            );
            builder.push(" ORDER BY token ASC, owner ASC, ");
            builder.push(sql_blob_numeric_order_expr(self.state.backend, "token_id"));
            builder.push(" ASC, token_id ASC");
            builder.push(" LIMIT ");
            builder.push_bind(limit as i64);

            let query_start = Instant::now();
            let result = builder.build().fetch_all(&mut *conn).await;
            ::metrics::histogram!(
                "torii_ecs_retrieve_token_balances_stage_seconds",
                "stage" => "erc721_query",
                "backend" => self.state.backend.as_str()
            )
            .record(query_start.elapsed().as_secs_f64());
            match result {
                Ok(rows) => {
                    tracing::debug!(
                        target: "torii::ecs_sink",
                        rows = rows.len(),
                        elapsed_ms = query_start.elapsed().as_millis(),
                        "RetrieveTokenBalances queried ERC721 ownership"
                    );
                    for row in rows {
                        let account_address = row.try_get::<Vec<u8>, _>("owner")?; // TODO: can it just be a vec?
                        let contract_address = row.try_get::<Vec<u8>, _>("token")?; // TODO: can it just be a vec?
                        let token_id =
                            canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("token_id")?)?;
                        items.push(types::TokenBalance {
                            balance: u256_bytes_from_u64(1),
                            account_address,
                            contract_address,
                            token_id: Some(token_id),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "torii::ecs_sink",
                        error = %e,
                        "Failed to query erc721.nft_ownership — returning empty ERC721 balances"
                    );
                }
            }
        }

        if self.state.has_erc1155 {
            let table = self.scoped_table("erc1155", "erc1155_balances");
            let mut builder = QueryBuilder::<Any>::new(format!(
                "SELECT wallet, contract, token_id, balance FROM {table}"
            ));
            let mut has_where = false;
            has_where = push_blob_in_filter_with_mode(
                &mut builder,
                "wallet",
                &query.account_addresses,
                has_where,
            );
            has_where = push_blob_in_filter_with_mode(
                &mut builder,
                "contract",
                &query.contract_addresses,
                has_where,
            );
            push_blob_in_filter_with_mode(
                &mut builder,
                "token_id",
                &normalized_token_ids,
                has_where,
            );
            builder.push(" ORDER BY contract ASC, wallet ASC, ");
            builder.push(sql_blob_numeric_order_expr(self.state.backend, "token_id"));
            builder.push(" ASC, token_id ASC");
            builder.push(" LIMIT ");
            builder.push_bind(limit as i64);

            let query_start = Instant::now();
            let result = builder.build().fetch_all(&mut *conn).await;
            ::metrics::histogram!(
                "torii_ecs_retrieve_token_balances_stage_seconds",
                "stage" => "erc1155_query",
                "backend" => self.state.backend.as_str()
            )
            .record(query_start.elapsed().as_secs_f64());
            match result {
                Ok(rows) => {
                    tracing::debug!(
                        target: "torii::ecs_sink",
                        rows = rows.len(),
                        elapsed_ms = query_start.elapsed().as_millis(),
                        "RetrieveTokenBalances queried ERC1155 balances"
                    );
                    for row in rows {
                        let balance =
                            canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("balance")?)?;
                        let account_address = row.try_get::<Vec<u8>, _>("wallet")?; // TODO: can it just be a vec?
                        let contract_address = row.try_get::<Vec<u8>, _>("contract")?; // TODO: can it just be a vec?
                        let token_id =
                            canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("token_id")?)?;
                        items.push(types::TokenBalance {
                            balance,
                            account_address,
                            contract_address,
                            token_id: Some(token_id),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "torii::ecs_sink",
                        error = %e,
                        "Failed to query erc1155.erc1155_balances — returning empty ERC1155 balances"
                    );
                }
            }
        }

        items.sort_by(|a, b| {
            a.contract_address
                .cmp(&b.contract_address)
                .then_with(|| a.account_address.cmp(&b.account_address))
                .then_with(|| a.token_id.cmp(&b.token_id))
        });
        items.truncate(limit);
        tracing::debug!(
            target: "torii::ecs_sink",
            balances = items.len(),
            "RetrieveTokenBalances merged and truncated results"
        );
        Ok((items, String::new()))
    }

    async fn load_token_transfers(
        &self,
        query: &types::TokenTransferQuery,
    ) -> Result<(Vec<types::TokenTransfer>, String)> {
        let limit = Self::pagination_limit(query.pagination.as_ref(), 100);
        let mut items = Vec::new();
        let mut conn = self.acquire_initialized_connection().await?;

        if self.state.has_erc20 {
            items.extend(self.load_erc20_transfers(&mut conn, query).await?);
        }
        if self.state.has_erc721 {
            items.extend(self.load_erc721_transfers(&mut conn, query).await?);
        }
        if self.state.has_erc1155 {
            items.extend(self.load_erc1155_transfers(&mut conn, query).await?);
        }

        items.sort_by(|a, b| {
            b.executed_at
                .cmp(&a.executed_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        items.truncate(limit);
        Ok((items, String::new()))
    }

    async fn load_transactions(
        &self,
        query: &types::TransactionQuery,
    ) -> Result<(Vec<types::Transaction>, String)> {
        let limit = Self::pagination_limit(query.pagination.as_ref(), 100);
        let filter = query.filter.clone().unwrap_or_default();
        if !filter.caller_addresses.is_empty()
            || !filter.entrypoints.is_empty()
            || !filter.model_selectors.is_empty()
        {
            return Ok((Vec::new(), String::new()));
        }

        let mut by_hash: HashMap<Vec<u8>, (u64, u64)> = HashMap::new();
        for (hash, block_number, block_timestamp) in
            self.load_transaction_rows_from_events(&filter).await?
        {
            let entry = by_hash
                .entry(hash)
                .or_insert((block_number, block_timestamp));
            if (block_number, block_timestamp) > *entry {
                *entry = (block_number, block_timestamp);
            }
        }
        for (hash, block_number, block_timestamp) in self
            .load_transaction_rows_from_token_tables(&filter)
            .await?
        {
            let entry = by_hash
                .entry(hash)
                .or_insert((block_number, block_timestamp));
            if (block_number, block_timestamp) > *entry {
                *entry = (block_number, block_timestamp);
            }
        }

        let mut transactions = by_hash
            .into_iter()
            .map(
                |(transaction_hash, (block_number, block_timestamp))| types::Transaction {
                    transaction_hash,
                    sender_address: Vec::new(),
                    calldata: Vec::new(),
                    max_fee: Vec::new(),
                    signature: Vec::new(),
                    nonce: Vec::new(),
                    block_number,
                    transaction_type: String::new(),
                    block_timestamp,
                    calls: Vec::new(),
                    unique_models: Vec::new(),
                },
            )
            .collect::<Vec<_>>();
        transactions.sort_by(|a, b| {
            b.block_number
                .cmp(&a.block_number)
                .then_with(|| b.block_timestamp.cmp(&a.block_timestamp))
                .then_with(|| a.transaction_hash.cmp(&b.transaction_hash))
        });
        if !filter.transaction_hashes.is_empty() {
            let wanted = filter
                .transaction_hashes
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            transactions.retain(|tx| wanted.contains(&tx.transaction_hash));
        }
        transactions.truncate(limit);
        Ok((transactions, String::new()))
    }

    async fn load_transaction_rows_from_events(
        &self,
        filter: &types::TransactionFilter,
    ) -> Result<Vec<(Vec<u8>, u64, u64)>> {
        let mut builder = QueryBuilder::<Any>::new(
            "SELECT transaction_hash, block_number, executed_at, world_address FROM torii_ecs_events",
        );
        let mut has_where = if let Some(from_block) = filter.from_block {
            builder.push(" WHERE block_number >= ");
            builder.push_bind(from_block as i64);
            true
        } else {
            false
        };
        if let Some(to_block) = filter.to_block {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
                has_where = true;
            }
            builder.push("block_number <= ");
            builder.push_bind(to_block as i64);
        }
        if !filter.contract_addresses.is_empty() {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
            }
            builder.push("world_address IN (");
            {
                let mut separated = builder.separated(", ");
                for contract in &filter.contract_addresses {
                    separated.push_bind(hex::encode(contract)); // TODO: Is okay?
                                                                // separated.push_bind(felt_hex(felt_from_bytes(contract)?));
                }
            }
            builder.push(")");
        }
        let rows = builder.build().fetch_all(&self.state.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    felt_from_hex(&row.try_get::<String, _>("transaction_hash")?)?.into(),
                    row.try_get::<i64, _>("block_number")? as u64,
                    row.try_get::<i64, _>("executed_at")? as u64,
                ))
            })
            .collect()
    }

    async fn load_transaction_rows_from_token_tables(
        &self,
        filter: &types::TransactionFilter,
    ) -> Result<Vec<(Vec<u8>, u64, u64)>> {
        let mut rows = Vec::new();
        if self.state.has_erc20 {
            rows.extend(
                self.load_token_tx_rows("erc20", "transfers", "token", filter)
                    .await?,
            );
        }
        if self.state.has_erc721 {
            rows.extend(
                self.load_token_tx_rows("erc721", "nft_transfers", "token", filter)
                    .await?,
            );
        }
        if self.state.has_erc1155 {
            rows.extend(
                self.load_token_tx_rows("erc1155", "token_transfers", "token", filter)
                    .await?,
            );
        }
        Ok(rows)
    }

    async fn load_token_tx_rows(
        &self,
        schema: &str,
        table_name: &str,
        contract_column: &str,
        filter: &types::TransactionFilter,
    ) -> Result<Vec<(Vec<u8>, u64, u64)>> {
        let table = self.scoped_table(schema, table_name);
        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT tx_hash, block_number, COALESCE(timestamp, '0') AS timestamp, {contract_column} FROM {table}"
        ));
        let mut has_where = if let Some(from_block) = filter.from_block {
            builder.push(" WHERE block_number >= ");
            builder.push_bind(from_block as i64);
            true
        } else {
            false
        };
        if let Some(to_block) = filter.to_block {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
                has_where = true;
            }
            builder.push("block_number <= ");
            builder.push_bind(to_block as i64);
        }
        if !filter.contract_addresses.is_empty() {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
            }
            builder.push(format!("hex({contract_column}) IN ("));
            {
                let mut separated = builder.separated(", ");
                for contract in &filter.contract_addresses {
                    separated.push_bind(hex::encode_upper(contract));
                }
            }
            builder.push(")");
        }
        let rows = builder.build().fetch_all(&self.state.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("tx_hash")?,
                    row.try_get::<String, _>("block_number")?
                        .parse::<u64>()
                        .unwrap_or(0),
                    row.try_get::<String, _>("timestamp")?
                        .parse::<u64>()
                        .unwrap_or(0),
                ))
            })
            .collect()
    }

    async fn load_erc20_transfers(
        &self,
        conn: &mut PoolConnection<Any>,
        query: &types::TokenTransferQuery,
    ) -> Result<Vec<types::TokenTransfer>> {
        let table = self.scoped_table("erc20", "transfers");
        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT id, token, from_addr, to_addr, amount, COALESCE(timestamp, '0') AS timestamp FROM {table}"
        ));
        push_transfer_filters(&mut builder, query, "token", "from_addr", "to_addr", None);
        let rows = builder.build().fetch_all(&mut **conn).await?;
        rows.into_iter()
            .map(|row| {
                let contract_address = row.try_get::<Vec<u8>, _>("token")?; // TODO: can it just be a vec?
                let from_address = row.try_get::<Vec<u8>, _>("from_addr")?; // TODO: can it just be a vec?
                let to_address = row.try_get::<Vec<u8>, _>("to_addr")?; // TODO: can it just be a vec?
                let amount = canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("amount")?)?;
                Ok(types::TokenTransfer {
                    id: format!("erc20:{}", row.try_get::<i64, _>("id")?),
                    contract_address,
                    from_address,
                    to_address,
                    amount,
                    token_id: None,
                    executed_at: row
                        .try_get::<String, _>("timestamp")?
                        .parse::<u64>()
                        .unwrap_or(0),
                    event_id: None,
                })
            })
            .collect()
    }

    async fn load_erc721_transfers(
        &self,
        conn: &mut PoolConnection<Any>,
        query: &types::TokenTransferQuery,
    ) -> Result<Vec<types::TokenTransfer>> {
        let table = self.scoped_table("erc721", "nft_transfers");
        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT id, token, from_addr, to_addr, token_id, COALESCE(timestamp, '0') AS timestamp FROM {table}"
        ));
        push_transfer_filters(
            &mut builder,
            query,
            "token",
            "from_addr",
            "to_addr",
            Some("token_id"),
        );
        let rows = builder.build().fetch_all(&mut **conn).await?;
        rows.into_iter()
            .map(|row| {
                let contract_address = row.try_get::<Vec<u8>, _>("token")?; // TODO: can it just be a vec?
                let from_address = row.try_get::<Vec<u8>, _>("from_addr")?; // TODO: can it just be a vec?
                let to_address = row.try_get::<Vec<u8>, _>("to_addr")?; // TODO: can it just be a vec?
                let token_id =
                    canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("token_id")?)?;
                Ok(types::TokenTransfer {
                    id: format!("erc721:{}", row.try_get::<i64, _>("id")?),
                    contract_address,
                    from_address,
                    to_address,
                    amount: u256_bytes_from_u64(1),
                    token_id: Some(token_id),
                    executed_at: row
                        .try_get::<String, _>("timestamp")?
                        .parse::<u64>()
                        .unwrap_or(0),
                    event_id: None,
                })
            })
            .collect()
    }

    async fn load_erc1155_transfers(
        &self,
        conn: &mut PoolConnection<Any>,
        query: &types::TokenTransferQuery,
    ) -> Result<Vec<types::TokenTransfer>> {
        let table = self.scoped_table("erc1155", "token_transfers");
        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT id, token, from_addr, to_addr, token_id, amount, COALESCE(timestamp, '0') AS timestamp FROM {table}"
        ));
        push_transfer_filters(
            &mut builder,
            query,
            "token",
            "from_addr",
            "to_addr",
            Some("token_id"),
        );
        let rows = builder.build().fetch_all(&mut **conn).await?;
        rows.into_iter()
            .map(|row| {
                let contract_address = row.try_get::<Vec<u8>, _>("token")?; // TODO: can it just be a vec?
                let from_address = row.try_get::<Vec<u8>, _>("from_addr")?; // TODO: can it just be a vec?
                let to_address = row.try_get::<Vec<u8>, _>("to_addr")?; // TODO: can it just be a vec?
                let token_id =
                    canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("token_id")?)?;
                let amount = canonical_u256_bytes_from_db(&row.try_get::<Vec<u8>, _>("amount")?)?;
                Ok(types::TokenTransfer {
                    id: format!("erc1155:{}", row.try_get::<i64, _>("id")?),
                    contract_address,
                    from_address,
                    to_address,
                    amount,
                    token_id: Some(token_id),
                    executed_at: row
                        .try_get::<String, _>("timestamp")?
                        .parse::<u64>()
                        .unwrap_or(0),
                    event_id: None,
                })
            })
            .collect()
    }

    async fn snapshot_token_seen(
        &self,
        contract_addresses: &[Vec<u8>],
        token_ids: &[Vec<u8>],
    ) -> Result<SeenCache> {
        let (tokens, _) = self
            .load_tokens(&types::TokenQuery {
                contract_addresses: contract_addresses.to_vec(),
                token_ids: token_ids.to_vec(),
                attribute_filters: Vec::new(),
                pagination: Some(types::Pagination {
                    cursor: String::new(),
                    limit: 1000,
                    direction: PaginationDirection::Forward as i32,
                    order_by: Vec::new(),
                }),
            })
            .await?;
        Ok(SeenCache::from_items(
            tokens
                .into_iter()
                .map(|token| token_subscription_key(&token)),
            SUBSCRIPTION_SEEN_CACHE_CAPACITY,
        ))
    }

    async fn snapshot_token_balance_state(
        &self,
        account_addresses: &[Vec<u8>],
        contract_addresses: &[Vec<u8>],
        token_ids: &[Vec<u8>],
    ) -> Result<HashMap<String, Vec<u8>>> {
        let (balances, _) = self
            .load_token_balances(&types::TokenBalanceQuery {
                account_addresses: account_addresses.to_vec(),
                contract_addresses: contract_addresses.to_vec(),
                token_ids: token_ids.to_vec(),
                pagination: Some(types::Pagination {
                    cursor: String::new(),
                    limit: 1000,
                    direction: PaginationDirection::Forward as i32,
                    order_by: Vec::new(),
                }),
            })
            .await?;
        Ok(token_balance_subscription_state(&balances))
    }

    async fn snapshot_token_transfer_seen(
        &self,
        account_addresses: &[Vec<u8>],
        contract_addresses: &[Vec<u8>],
        token_ids: &[Vec<u8>],
    ) -> Result<SeenCache> {
        let (transfers, _) = self
            .load_token_transfers(&types::TokenTransferQuery {
                account_addresses: account_addresses.to_vec(),
                contract_addresses: contract_addresses.to_vec(),
                token_ids: token_ids.to_vec(),
                pagination: Some(types::Pagination {
                    cursor: String::new(),
                    limit: 1000,
                    direction: PaginationDirection::Forward as i32,
                    order_by: Vec::new(),
                }),
            })
            .await?;
        Ok(SeenCache::from_items(
            transfers.into_iter().map(|transfer| transfer.id),
            SUBSCRIPTION_SEEN_CACHE_CAPACITY,
        ))
    }

    async fn snapshot_transaction_seen(
        &self,
        filter: &types::TransactionFilter,
    ) -> Result<SeenCache> {
        let (transactions, _) = self
            .load_transactions(&types::TransactionQuery {
                filter: Some(filter.clone()),
                pagination: Some(types::Pagination {
                    cursor: String::new(),
                    limit: 1000,
                    direction: PaginationDirection::Forward as i32,
                    order_by: Vec::new(),
                }),
            })
            .await?;
        Ok(SeenCache::from_items(
            transactions
                .into_iter()
                .map(|transaction| hex::encode(transaction.transaction_hash)),
            SUBSCRIPTION_SEEN_CACHE_CAPACITY,
        ))
    }

    fn spawn_token_poll(&self, subscription_id: u64) {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                let (contract_addresses, token_ids, seen, sender) = {
                    let subscriptions = service.state.token_subscriptions.lock().await;
                    let Some(subscription) = subscriptions.get(&subscription_id) else {
                        break;
                    };
                    (
                        subscription.contract_addresses.clone(),
                        subscription.token_ids.clone(),
                        subscription.seen.snapshot(),
                        subscription.sender.clone(),
                    )
                };
                let tokens = match service
                    .load_tokens(&types::TokenQuery {
                        contract_addresses,
                        token_ids,
                        attribute_filters: Vec::new(),
                        pagination: Some(types::Pagination {
                            cursor: String::new(),
                            limit: 1000,
                            direction: PaginationDirection::Forward as i32,
                            order_by: Vec::new(),
                        }),
                    })
                    .await
                {
                    Ok((tokens, _)) => tokens,
                    Err(_) => continue,
                };
                let mut sent = Vec::new();
                for token in tokens {
                    let key = token_subscription_key(&token);
                    if seen.contains(&key) {
                        continue;
                    }
                    if sender
                        .send(Ok(SubscribeTokensResponse {
                            subscription_id,
                            token: Some(token),
                        }))
                        .await
                        .is_err()
                    {
                        service
                            .state
                            .token_subscriptions
                            .lock()
                            .await
                            .remove(&subscription_id);
                        return;
                    }
                    sent.push(key);
                }
                if !sent.is_empty() {
                    if let Some(subscription) = service
                        .state
                        .token_subscriptions
                        .lock()
                        .await
                        .get_mut(&subscription_id)
                    {
                        subscription.seen.extend(sent);
                    }
                }
            }
        });
    }

    fn spawn_token_balance_poll(&self, subscription_id: u64) {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                let (account_addresses, contract_addresses, token_ids, latest, sender) = {
                    let subscriptions = service.state.token_balance_subscriptions.lock().await;
                    let Some(subscription) = subscriptions.get(&subscription_id) else {
                        break;
                    };
                    (
                        subscription.account_addresses.clone(),
                        subscription.contract_addresses.clone(),
                        subscription.token_ids.clone(),
                        subscription.latest.clone(),
                        subscription.sender.clone(),
                    )
                };
                let balances = match service
                    .load_token_balances(&types::TokenBalanceQuery {
                        account_addresses,
                        contract_addresses,
                        token_ids,
                        pagination: Some(types::Pagination {
                            cursor: String::new(),
                            limit: 1000,
                            direction: PaginationDirection::Forward as i32,
                            order_by: Vec::new(),
                        }),
                    })
                    .await
                {
                    Ok((balances, _)) => balances,
                    Err(_) => continue,
                };
                let current = token_balance_subscription_state(&balances);
                for balance in balances {
                    let identity = token_balance_identity_key(&balance);
                    let fingerprint = token_balance_state_fingerprint(&balance);
                    if latest.get(&identity) == Some(&fingerprint) {
                        continue;
                    }
                    if sender
                        .send(Ok(SubscribeTokenBalancesResponse {
                            subscription_id,
                            balance: Some(balance),
                        }))
                        .await
                        .is_err()
                    {
                        service
                            .state
                            .token_balance_subscriptions
                            .lock()
                            .await
                            .remove(&subscription_id);
                        return;
                    }
                }
                if let Some(subscription) = service
                    .state
                    .token_balance_subscriptions
                    .lock()
                    .await
                    .get_mut(&subscription_id)
                {
                    subscription.latest = current;
                }
            }
        });
    }

    fn spawn_token_transfer_poll(&self, subscription_id: u64) {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                let (account_addresses, contract_addresses, token_ids, seen, sender) = {
                    let subscriptions = service.state.token_transfer_subscriptions.lock().await;
                    let Some(subscription) = subscriptions.get(&subscription_id) else {
                        break;
                    };
                    (
                        subscription.account_addresses.clone(),
                        subscription.contract_addresses.clone(),
                        subscription.token_ids.clone(),
                        subscription.seen.snapshot(),
                        subscription.sender.clone(),
                    )
                };
                let transfers = match service
                    .load_token_transfers(&types::TokenTransferQuery {
                        account_addresses,
                        contract_addresses,
                        token_ids,
                        pagination: Some(types::Pagination {
                            cursor: String::new(),
                            limit: 1000,
                            direction: PaginationDirection::Forward as i32,
                            order_by: Vec::new(),
                        }),
                    })
                    .await
                {
                    Ok((transfers, _)) => transfers,
                    Err(_) => continue,
                };
                let mut sent = Vec::new();
                for transfer in transfers {
                    if seen.contains(&transfer.id) {
                        continue;
                    }
                    if sender
                        .send(Ok(SubscribeTokenTransfersResponse {
                            subscription_id,
                            transfer: Some(transfer.clone()),
                        }))
                        .await
                        .is_err()
                    {
                        service
                            .state
                            .token_transfer_subscriptions
                            .lock()
                            .await
                            .remove(&subscription_id);
                        return;
                    }
                    sent.push(transfer.id);
                }
                if !sent.is_empty() {
                    if let Some(subscription) = service
                        .state
                        .token_transfer_subscriptions
                        .lock()
                        .await
                        .get_mut(&subscription_id)
                    {
                        subscription.seen.extend(sent);
                    }
                }
            }
        });
    }

    fn spawn_transaction_poll(&self, subscription_id: u64) {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                let (filter, seen, sender) = {
                    let subscriptions = service.state.transaction_subscriptions.lock().await;
                    let Some(subscription) = subscriptions.get(&subscription_id) else {
                        break;
                    };
                    (
                        subscription.filter.clone(),
                        subscription.seen.snapshot(),
                        subscription.sender.clone(),
                    )
                };
                let transactions = match service
                    .load_transactions(&types::TransactionQuery {
                        filter: Some(filter),
                        pagination: Some(types::Pagination {
                            cursor: String::new(),
                            limit: 1000,
                            direction: PaginationDirection::Forward as i32,
                            order_by: Vec::new(),
                        }),
                    })
                    .await
                {
                    Ok((transactions, _)) => transactions,
                    Err(_) => continue,
                };
                let mut sent = Vec::new();
                for transaction in transactions {
                    let key = hex::encode(&transaction.transaction_hash);
                    if seen.contains(&key) {
                        continue;
                    }
                    if sender
                        .send(Ok(SubscribeTransactionsResponse {
                            transaction: Some(transaction),
                        }))
                        .await
                        .is_err()
                    {
                        service
                            .state
                            .transaction_subscriptions
                            .lock()
                            .await
                            .remove(&subscription_id);
                        return;
                    }
                    sent.push(key);
                }
                if !sent.is_empty() {
                    if let Some(subscription) = service
                        .state
                        .transaction_subscriptions
                        .lock()
                        .await
                        .get_mut(&subscription_id)
                    {
                        subscription.seen.extend(sent);
                    }
                }
            }
        });
    }

    async fn load_worlds(&self, requested: &[Felt]) -> Result<Vec<types::World>> {
        let requested = requested.iter().copied().collect::<HashSet<_>>();
        let mut by_world: HashMap<Vec<u8>, Vec<types::Model>> = HashMap::new();
        for table in self.load_managed_tables(None).await? {
            if !requested.is_empty() && !requested.contains(&table.world_address) {
                continue;
            }
            by_world
                .entry(table.world_address.into())
                .or_default()
                .push(model_from_table(&table));
        }

        let mut worlds = by_world
            .into_iter()
            .map(|(world_address, models)| types::World {
                world_address: felt_from_bytes(&world_address)
                    .unwrap_or_default()
                    .to_string(),
                models,
            })
            .collect::<Vec<_>>();
        worlds.sort_by(|a, b| a.world_address.cmp(&b.world_address));
        Ok(worlds)
    }

    async fn put_managed_table(&self, world_address: Felt, table: DojoTable) {
        if self.state.managed_tables.lock().await.is_none() {
            let _ = self.load_managed_table_map().await;
        }
        let mut managed_tables = self.state.managed_tables.lock().await;
        if let Some(tables) = managed_tables.as_ref() {
            let mut next = (**tables).clone();
            let key = felt_hex(table.id);
            let kind = next.get(&key).map_or(TableKind::Entity, |table| table.kind);
            next.insert(
                key,
                ManagedTable {
                    world_address,
                    table,
                    kind,
                },
            );
            *managed_tables = Some(Arc::new(next));
        }
    }

    async fn update_managed_table_kind(
        &self,
        table_id: Felt,
        world_address: Felt,
        kind: TableKind,
    ) {
        let mut managed_tables = self.state.managed_tables.lock().await;
        let Some(tables) = managed_tables.as_ref() else {
            return;
        };
        let mut next = (**tables).clone();
        let key = felt_hex(table_id);
        if let Some(table) = next.get_mut(&key) {
            table.world_address = world_address;
            table.kind = kind;
            *managed_tables = Some(Arc::new(next));
        } else {
            *managed_tables = None;
        }
    }

    async fn load_managed_table(&self, table_id: Felt) -> Result<Option<ManagedTable>> {
        let key = felt_hex(table_id);
        Ok(self.load_managed_table_map().await?.get(&key).cloned())
    }

    async fn load_managed_table_map(&self) -> Result<Arc<HashMap<String, ManagedTable>>> {
        {
            let managed_tables = self.state.managed_tables.lock().await;
            if let Some(tables) = managed_tables.as_ref() {
                return Ok(Arc::clone(tables));
            }
        }

        let kinds = self.load_table_kind_rows().await?;
        let tables = self.load_dojo_tables().await?;
        let mut managed = HashMap::with_capacity(tables.len());
        for table in tables {
            let key = felt_hex(table.id);
            let (world_address, table_kind) = if let Some((world, kind_name)) = kinds.get(&key) {
                let kind = TableKind::from_str(kind_name).unwrap_or(TableKind::Entity);
                (felt_from_hex(world)?, kind)
            } else {
                (Felt::ZERO, TableKind::Entity)
            };
            managed.insert(
                key,
                ManagedTable {
                    world_address,
                    table,
                    kind: table_kind,
                },
            );
        }

        let mut managed_tables = self.state.managed_tables.lock().await;
        let managed = Arc::new(managed);
        *managed_tables = Some(Arc::clone(&managed));
        Ok(managed)
    }

    async fn load_table_kind_rows(&self) -> Result<HashMap<String, (String, String)>> {
        let table_kind_sql = "SELECT table_id, world_address, kind FROM torii_ecs_table_kinds";
        let kind_rows = sqlx::query(table_kind_sql)
            .fetch_all(&self.state.pool)
            .await?;
        let mut kinds = HashMap::new();
        for row in kind_rows {
            kinds.insert(
                row.try_get::<String, _>("table_id")?,
                (
                    row.try_get::<String, _>("world_address")?,
                    row.try_get::<String, _>("kind")?,
                ),
            );
        }
        Ok(kinds)
    }

    async fn load_dojo_tables(&self) -> Result<Vec<DojoTable>> {
        match self.state.backend {
            DbBackend::Sqlite => {
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(SqliteConnectOptions::from_str(&self.state.database_url)?)
                    .await?;
                let store = SqliteStore(pool);
                Ok(store.read_tables(&[]).await?)
            }
            DbBackend::Postgres => {
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&self.state.database_url)
                    .await?;
                Ok(pool.read_tables(&[]).await?)
            }
        }
    }

    async fn load_managed_tables(&self, kind: Option<TableKind>) -> Result<Vec<ManagedTable>> {
        Ok(self
            .load_managed_table_map()
            .await?
            .values()
            .filter(|table| kind.is_none_or(|required| table.kind == required))
            .cloned()
            .collect())
    }

    async fn load_entity_page(
        &self,
        kind: TableKind,
        query: &types::Query,
    ) -> Result<(Vec<types::Entity>, String)> {
        let world_filters = query
            .world_addresses
            .iter()
            .map(|bytes| felt_from_bytes(bytes))
            .collect::<Result<Vec<_>>>()?;
        let world_filter_set = world_filters.iter().copied().collect::<HashSet<_>>();
        let model_filter_set = query
            .models
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let mut model_map = HashMap::new();
        let mut all_model_map = HashMap::new();
        for table in self.load_managed_tables(Some(kind)).await? {
            if !world_filter_set.is_empty() && !world_filter_set.contains(&table.world_address) {
                continue;
            }

            let table_id = felt_hex(table.table.id);
            if model_filter_set.is_empty() || model_filter_set.contains(table.table.name.as_str()) {
                model_map.insert(table_id.clone(), table.clone());
            }
            all_model_map.insert(table_id, table);
        }

        if model_map.is_empty() {
            return Ok((Vec::new(), String::new()));
        }
        let table_ids = model_map.keys().cloned().collect::<Vec<_>>();
        let table_id_filter = if query.models.is_empty() {
            None
        } else {
            Some(table_ids.as_slice())
        };
        let models_for_entity = if table_id_filter.is_some() {
            &all_model_map
        } else {
            &model_map
        };
        let member_pushdown = member_pushdown_from_clause(query.clause.as_ref(), &model_map);
        let target_limit = query.pagination.as_ref().map_or(100, |pagination| {
            if pagination.limit == 0 {
                100
            } else {
                pagination.limit as usize
            }
        });

        let direction = query
            .pagination
            .as_ref()
            .map_or(PaginationDirection::Forward as i32, |pagination| {
                pagination.direction
            });
        let cursor = query
            .pagination
            .as_ref()
            .map_or(String::new(), |pagination| pagination.cursor.clone());

        let mut items = Vec::new();
        let mut next_cursor = String::new();
        let mut candidate_cursor = cursor;

        let mut conn = self.acquire_query_connection("load_entity_page").await?;

        loop {
            let keys_start = Instant::now();
            let mut candidate_keys = self
                .load_entity_page_keys(
                    &mut conn,
                    kind,
                    &world_filters,
                    table_id_filter,
                    member_pushdown.as_ref(),
                    &candidate_cursor,
                    direction,
                    target_limit.saturating_add(1),
                )
                .await?;
            ::metrics::histogram!("torii_ecs_load_entity_page_meta_seconds")
                .record(keys_start.elapsed().as_secs_f64());

            if candidate_keys.is_empty() {
                next_cursor.clear();
                break;
            }

            let has_more_candidates = candidate_keys.len() > target_limit;
            if has_more_candidates {
                candidate_keys.truncate(target_limit);
            }
            ::metrics::gauge!("torii_ecs_load_entity_page_candidates")
                .set(candidate_keys.len() as f64);

            let batch_cursor = candidate_keys
                .last()
                .map(|(_, entity_id)| entity_id.trim_start_matches("0x").to_string())
                .unwrap_or_default();

            for chunk in candidate_keys.chunks(256) {
                let rows_start = Instant::now();
                let rows = self
                    .load_entity_page_rows(&mut conn, kind, &world_filters, None, chunk)
                    .await?;
                ::metrics::histogram!("torii_ecs_load_entity_page_rows_seconds")
                    .record(rows_start.elapsed().as_secs_f64());
                ::metrics::counter!("torii_ecs_load_entity_page_rows_total")
                    .increment(rows.len() as u64);

                let mut row_map = HashMap::<(String, String), Vec<(String, String)>>::new();
                let mut meta_map =
                    HashMap::<(String, String), HashMap<String, EntityMetaRow>>::new();
                for row in rows {
                    let key = (row.world_address.clone(), row.entity_id.clone());
                    meta_map.entry(key.clone()).or_default().insert(
                        row.table_id.clone(),
                        EntityMetaRow {
                            created_at: row.created_at,
                            updated_at: row.updated_at,
                            executed_at: row.executed_at,
                            deleted: false,
                        },
                    );
                    row_map
                        .entry(key)
                        .or_default()
                        .push((row.table_id, row.row_json));
                }

                for key in chunk {
                    let Some(meta) = meta_map.get(key) else {
                        continue;
                    };
                    let Some(entity) =
                        build_entity_from_snapshot(key, meta, row_map.get(key), models_for_entity)?
                    else {
                        continue;
                    };
                    let entity = entity.into_proto();
                    if query
                        .clause
                        .as_ref()
                        .is_some_and(|clause| !entity_matches_clause(&entity, clause))
                    {
                        continue;
                    }
                    items.push(entity);
                    if items.len() >= target_limit {
                        break;
                    }
                }

                if items.len() >= target_limit {
                    break;
                }
            }

            if items.len() >= target_limit {
                next_cursor = if has_more_candidates {
                    batch_cursor
                } else {
                    String::new()
                };
                break;
            }

            if !has_more_candidates {
                next_cursor.clear();
                break;
            }

            candidate_cursor = batch_cursor;
            next_cursor.clone_from(&candidate_cursor);
        }

        if query.no_hashed_keys {
            for entity in &mut items {
                entity.hashed_keys.clear();
            }
        }
        Ok((items, next_cursor))
    }

    #[allow(dead_code)]
    async fn load_entity_page_from_storage(
        &self,
        kind: TableKind,
        query: &types::Query,
    ) -> Result<(Vec<types::Entity>, String)> {
        let world_filters = query
            .world_addresses
            .iter()
            .map(|bytes| felt_from_bytes(bytes))
            .collect::<Result<Vec<_>>>()?;
        let world_filter_set = world_filters.iter().copied().collect::<HashSet<_>>();
        let model_filter_set = query
            .models
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let models = self
            .load_managed_tables(Some(kind))
            .await?
            .into_iter()
            .filter(|table| {
                (world_filter_set.is_empty() || world_filter_set.contains(&table.world_address))
                    && (model_filter_set.is_empty()
                        || model_filter_set.contains(table.table.name.as_str()))
            })
            .collect::<Vec<_>>();

        let mut entities: HashMap<(Vec<u8>, Vec<u8>), EntityAggregate> = HashMap::new();
        for table in models {
            let rows = self.fetch_table_rows(&table).await?;
            let meta = self
                .load_entity_meta(table.kind, table.world_address, table.table.id.into())
                .await?;
            for row in rows {
                let entity_id = row
                    .get(&table.table.primary.name)
                    .cloned()
                    .unwrap_or(Value::Null);
                let entity_felt = value_to_primary_felt(&entity_id, &table.table.primary.type_def)
                    .unwrap_or(Felt::ZERO);
                let entity_key: Vec<u8> = entity_felt.into();
                if meta
                    .get(&felt_hex(entity_felt))
                    .is_some_and(|meta| meta.deleted)
                {
                    continue;
                }

                let model = row_to_model_struct(&table.table, &row)?;
                let aggregate = entities
                    .entry((table.world_address.into(), entity_key.clone()))
                    .or_insert_with(|| EntityAggregate {
                        world_address: table.world_address.into(),
                        hashed_keys: entity_key.clone(),
                        ..EntityAggregate::default()
                    });
                aggregate.models.push(model);
                if let Some(meta) = meta.get(&felt_hex(entity_felt)) {
                    if aggregate.created_at == 0 || meta.created_at < aggregate.created_at {
                        aggregate.created_at = meta.created_at;
                    }
                    aggregate.updated_at = aggregate.updated_at.max(meta.updated_at);
                    aggregate.executed_at = aggregate.executed_at.max(meta.executed_at);
                }
            }
        }

        let mut items = entities
            .into_values()
            .map(EntityAggregate::into_proto)
            .filter(|entity| {
                query
                    .clause
                    .as_ref()
                    .is_none_or(|clause| entity_matches_clause(entity, clause))
            })
            .collect::<Vec<_>>();
        items.sort_by(|a, b| a.hashed_keys.cmp(&b.hashed_keys));
        if query
            .pagination
            .as_ref()
            .is_some_and(|p| p.direction == PaginationDirection::Backward as i32)
        {
            items.reverse();
        }

        if let Some(pagination) = &query.pagination {
            if !pagination.cursor.is_empty() {
                items.retain(|entity| {
                    let key = hex::encode(&entity.hashed_keys);
                    if pagination.direction == PaginationDirection::Backward as i32 {
                        key < pagination.cursor
                    } else {
                        key > pagination.cursor
                    }
                });
            }
            let limit = if pagination.limit == 0 {
                100
            } else {
                pagination.limit as usize
            };
            let next_cursor = items
                .get(limit.saturating_sub(1))
                .map(|entity| hex::encode(&entity.hashed_keys))
                .unwrap_or_default();
            items.truncate(limit);
            if query.no_hashed_keys {
                for entity in &mut items {
                    entity.hashed_keys.clear();
                }
            }
            return Ok((items, next_cursor));
        }

        if query.no_hashed_keys {
            for entity in &mut items {
                entity.hashed_keys.clear();
            }
        }
        Ok((items, String::new()))
    }

    async fn load_entity_by_id(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<Option<EntityAggregate>> {
        if let Some(entity) = self
            .load_entity_snapshot(kind, world_address, entity_id)
            .await?
        {
            return Ok(Some(entity));
        }

        let query = types::Query {
            clause: Some(types::Clause {
                clause_type: Some(ClauseType::HashedKeys(types::HashedKeysClause {
                    hashed_keys: vec![entity_id.into()],
                })),
            }),
            no_hashed_keys: false,
            models: vec![],
            pagination: None,
            historical: false,
            world_addresses: vec![world_address.into()],
        };
        Ok(self
            .load_entity_page(kind, &query)
            .await?
            .0
            .into_iter()
            .next()
            .map(|entity| EntityAggregate {
                world_address: entity.world_address,
                hashed_keys: entity.hashed_keys,
                models: entity.models,
                created_at: entity.created_at,
                updated_at: entity.updated_at,
                executed_at: entity.executed_at,
            }))
    }

    async fn load_entity_page_keys(
        &self,
        conn: &mut PoolConnection<Any>,
        kind: TableKind,
        world_filters: &[Felt],
        table_ids: Option<&[String]>,
        member_pushdown: Option<&MemberPushdown>,
        cursor: &str,
        direction: i32,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        if table_ids.is_some_and(<[String]>::is_empty) {
            return Ok(Vec::new());
        }
        let limit = limit.clamp(1, 200);

        let cursor_op = if direction == PaginationDirection::Backward as i32 {
            "<"
        } else {
            ">"
        };
        let order = if direction == PaginationDirection::Backward as i32 {
            "DESC"
        } else {
            "ASC"
        };

        let single_table = table_ids.is_some_and(|ids| ids.len() == 1);
        let needs_group_by = table_ids.is_none_or(|ids| ids.len() > 1);

        let mut builder = QueryBuilder::<Any>::new(
            "SELECT m.world_address, m.entity_id \
             FROM torii_ecs_entity_meta m \
             WHERE m.kind = ",
        );
        builder.push_bind(kind.as_str());
        if let Some(table_ids) = table_ids {
            builder.push(" AND m.table_id IN (");
            {
                let mut separated = builder.separated(", ");
                for table_id in table_ids {
                    separated.push_bind(table_id);
                }
            }
            builder.push(")");
        }
        if !world_filters.is_empty() {
            builder.push(" AND m.world_address IN (");
            {
                let mut separated = builder.separated(", ");
                for world_address in world_filters {
                    separated.push_bind(felt_hex(*world_address));
                }
            }
            builder.push(")");
        }
        match self.state.backend {
            DbBackend::Sqlite => {
                builder.push(" AND m.deleted = 0");
            }
            DbBackend::Postgres => {
                builder.push(" AND m.deleted = FALSE");
            }
        }
        if !cursor.is_empty() {
            builder.push(" AND m.entity_id ");
            builder.push(cursor_op);
            builder.push(" ");
            builder.push_bind(format!("0x{cursor}"));
        }
        if let Some(member_pushdown) = member_pushdown {
            append_member_pushdown_sql(&mut builder, self.state.backend, member_pushdown);
        }
        if needs_group_by && !single_table {
            builder.push(" GROUP BY m.world_address, m.entity_id");
        }
        builder.push(" ORDER BY m.entity_id ");
        builder.push(order);
        builder.push(", m.world_address ");
        builder.push(order);
        builder.push(" LIMIT ");
        builder.push_bind(limit as i64);

        let start = Instant::now();
        let rows = builder.build().fetch_all(&mut **conn).await?;
        ::metrics::histogram!(
            "torii_ecs_load_entity_page_keys_query_seconds",
            "backend" => self.state.backend.as_str()
        )
        .record(start.elapsed().as_secs_f64());
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("world_address")?,
                    row.try_get::<String, _>("entity_id")?,
                ))
            })
            .collect()
    }

    async fn load_entity_page_rows(
        &self,
        conn: &mut PoolConnection<Any>,
        kind: TableKind,
        _world_filters: &[Felt],
        table_ids: Option<&[String]>,
        entity_keys: &[(String, String)],
    ) -> Result<Vec<EntityModelRow>> {
        if entity_keys.is_empty() || table_ids.is_some_and(<[String]>::is_empty) {
            return Ok(Vec::new());
        }

        let deleted_sql = match self.state.backend {
            DbBackend::Sqlite => "m.deleted = 0",
            DbBackend::Postgres => "m.deleted = FALSE",
        };

        let mut builder = QueryBuilder::<Any>::new(format!(
            "SELECT em.world_address, em.table_id, em.entity_id, em.row_json, \
                    m.created_at, m.updated_at, m.executed_at \
             FROM torii_ecs_entity_models em \
             JOIN torii_ecs_entity_meta m \
               ON m.kind = em.kind AND m.world_address = em.world_address \
              AND m.table_id = em.table_id AND m.entity_id = em.entity_id \
              AND {deleted_sql} \
             WHERE em.kind = "
        ));
        builder.push_bind(kind.as_str());
        if let Some(table_ids) = table_ids {
            builder.push(" AND em.table_id IN (");
            {
                let mut separated = builder.separated(", ");
                for table_id in table_ids {
                    separated.push_bind(table_id);
                }
            }
            builder.push(")");
        }
        append_entity_key_pairs_filter(
            &mut builder,
            "em.world_address",
            "em.entity_id",
            entity_keys,
        );

        let start = Instant::now();
        let rows = builder.build().fetch_all(&mut **conn).await?;
        ::metrics::histogram!(
            "torii_ecs_load_entity_page_rows_query_seconds",
            "backend" => self.state.backend.as_str()
        )
        .record(start.elapsed().as_secs_f64());
        let mut output = Vec::new();
        for row in rows {
            output.push(EntityModelRow {
                world_address: row.try_get("world_address")?,
                table_id: row.try_get("table_id")?,
                entity_id: row.try_get("entity_id")?,
                row_json: row.try_get("row_json")?,
                created_at: row.try_get::<i64, _>("created_at")? as u64,
                updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                executed_at: row.try_get::<i64, _>("executed_at")? as u64,
            });
        }
        Ok(output)
    }

    async fn load_entity_snapshot(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<Option<EntityAggregate>> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "SELECT table_id, row_json
                 FROM torii_ecs_entity_models
                 WHERE kind = ?1 AND world_address = ?2 AND entity_id = ?3"
            }
            DbBackend::Postgres => {
                "SELECT table_id, row_json
                 FROM torii_ecs_entity_models
                 WHERE kind = $1 AND world_address = $2 AND entity_id = $3"
            }
        };
        let rows = sqlx::query(sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(entity_id))
            .fetch_all(&self.state.pool)
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }

        let meta = self
            .load_entity_meta_rows(kind, world_address, entity_id)
            .await?;
        let managed_tables = self.load_managed_table_map().await?;
        let mut aggregate = EntityAggregate {
            world_address: world_address.into(),
            hashed_keys: entity_id.into(),
            ..EntityAggregate::default()
        };

        for row in rows {
            let table_id = row.try_get::<String, _>("table_id")?;
            let Some(table) = managed_tables.get(&table_id) else {
                continue;
            };
            let Some(table_meta) = meta.get(&table_id) else {
                continue;
            };
            if table_meta.deleted {
                continue;
            }

            let row_json: String = row.try_get("row_json")?;
            let object: Map<String, Value> = serde_json::from_str(&row_json)?;
            aggregate
                .models
                .push(row_to_model_struct(&table.table, &object)?);
            if aggregate.created_at == 0 || table_meta.created_at < aggregate.created_at {
                aggregate.created_at = table_meta.created_at;
            }
            aggregate.updated_at = aggregate.updated_at.max(table_meta.updated_at);
            aggregate.executed_at = aggregate.executed_at.max(table_meta.executed_at);
        }

        if aggregate.models.is_empty() {
            return Ok(None);
        }
        Ok(Some(aggregate))
    }

    async fn load_entity_meta_rows(
        &self,
        kind: TableKind,
        world_address: Felt,
        entity_id: Felt,
    ) -> Result<HashMap<String, EntityMetaRow>> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => "SELECT table_id, created_at, updated_at, executed_at,
                        CAST(deleted AS INTEGER) AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = ? AND world_address = ? AND entity_id = ?"
                .to_string(),
            DbBackend::Postgres => "SELECT table_id, created_at, updated_at, executed_at,
                        CASE WHEN deleted THEN 1 ELSE 0 END AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = $1 AND world_address = $2 AND entity_id = $3"
                .to_string(),
        };
        let rows = sqlx::query(&sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(entity_id))
            .fetch_all(&self.state.pool)
            .await?;
        let mut meta = HashMap::new();
        for row in rows {
            meta.insert(
                row.try_get::<String, _>("table_id")?,
                EntityMetaRow {
                    created_at: row.try_get::<i64, _>("created_at")? as u64,
                    updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                    executed_at: row.try_get::<i64, _>("executed_at")? as u64,
                    deleted: row.try_get::<i64, _>("deleted")? != 0,
                },
            );
        }
        Ok(meta)
    }

    #[allow(dead_code)]
    async fn load_entity_meta(
        &self,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
    ) -> Result<HashMap<String, EntityMetaRow>> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => "SELECT entity_id, created_at, updated_at, executed_at,
                        CAST(deleted AS INTEGER) AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = ? AND world_address = ? AND table_id = ?"
                .to_string(),
            DbBackend::Postgres => "SELECT entity_id, created_at, updated_at, executed_at,
                        CASE WHEN deleted THEN 1 ELSE 0 END AS deleted
                 FROM torii_ecs_entity_meta
                 WHERE kind = $1 AND world_address = $2 AND table_id = $3"
                .to_string(),
        };
        let rows = sqlx::query(&sql)
            .bind(kind.as_str())
            .bind(felt_hex(world_address))
            .bind(felt_hex(table_id))
            .fetch_all(&self.state.pool)
            .await?;
        let mut meta = HashMap::new();
        for row in rows {
            meta.insert(
                row.try_get::<String, _>("entity_id")?,
                EntityMetaRow {
                    created_at: row.try_get::<i64, _>("created_at")? as u64,
                    updated_at: row.try_get::<i64, _>("updated_at")? as u64,
                    executed_at: row.try_get::<i64, _>("executed_at")? as u64,
                    deleted: row.try_get::<i64, _>("deleted")? != 0,
                },
            );
        }
        Ok(meta)
    }

    #[allow(dead_code)]
    async fn fetch_table_rows(&self, table: &ManagedTable) -> Result<Vec<Map<String, Value>>> {
        match self.state.backend {
            DbBackend::Sqlite => {
                let sql = format!(
                    "SELECT * FROM \"{}\"",
                    table.table.name.replace('"', "\"\"")
                );
                let rows = sqlx::query(&sql).fetch_all(&self.state.pool).await?;
                rows.into_iter()
                    .map(|row| any_row_to_json_map(&row, &table.table))
                    .collect()
            }
            DbBackend::Postgres => {
                let sql = format!(
                    "SELECT row_to_json(t)::text AS data FROM (SELECT * FROM \"{}\") t",
                    table.table.name.replace('"', "\"\"")
                );
                let rows = sqlx::query(&sql).fetch_all(&self.state.pool).await?;
                rows.into_iter()
                    .map(|row| {
                        let data: String = row.try_get("data")?;
                        let value: Value = serde_json::from_str(&data)?;
                        value
                            .as_object()
                            .cloned()
                            .ok_or_else(|| anyhow!("row_to_json must return an object"))
                    })
                    .collect()
            }
        }
    }

    async fn load_events_page(
        &self,
        query: &types::EventQuery,
    ) -> Result<(Vec<types::Event>, String)> {
        let pagination = query.pagination.clone().unwrap_or(types::Pagination {
            cursor: String::new(),
            limit: 100,
            direction: PaginationDirection::Forward as i32,
            order_by: Vec::new(),
        });
        let limit = if pagination.limit == 0 {
            100
        } else {
            pagination.limit as usize
        };
        let direction_is_backward = pagination.direction == PaginationDirection::Backward as i32;
        let order_sql = if direction_is_backward { "DESC" } else { "ASC" };

        let mut builder = QueryBuilder::<Any>::new(
            "SELECT event_id, transaction_hash, keys_json, data_json \
             FROM torii_ecs_events",
        );
        if !pagination.cursor.is_empty() {
            builder.push(" WHERE event_id ");
            builder.push(if direction_is_backward { "< " } else { "> " });
            builder.push_bind(pagination.cursor.clone());
        }
        builder.push(" ORDER BY event_id ");
        builder.push(order_sql);

        let fetch_limit = if query.keys.is_some() {
            limit.saturating_mul(20).max(100)
        } else {
            limit
        };
        builder.push(" LIMIT ");
        builder.push_bind(fetch_limit as i64);

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        ::metrics::counter!("torii_ecs_retrieve_events_rows_fetched_total")
            .increment(rows.len() as u64);

        let mut events = Vec::new();
        for row in rows {
            let keys_hex: Vec<String> =
                serde_json::from_str(&row.try_get::<String, _>("keys_json")?)?;
            let data_hex: Vec<String> =
                serde_json::from_str(&row.try_get::<String, _>("data_json")?)?;
            let event = types::Event {
                keys: keys_hex
                    .into_iter()
                    .map(|value| Felt::from_hex(&value).map(|felt| felt.to_be_bytes_vec()))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
                data: data_hex
                    .into_iter()
                    .map(|value| Felt::from_hex(&value).map(|felt| felt.to_be_bytes_vec()))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
                transaction_hash: Felt::from_hex(&row.try_get::<String, _>("transaction_hash")?)?
                    .to_be_bytes_vec(),
            };
            if query
                .keys
                .as_ref()
                .is_none_or(|keys| match_keys(&event.keys, std::slice::from_ref(keys)))
            {
                events.push((row.try_get::<String, _>("event_id")?, event));
            }
            if events.len() >= limit {
                break;
            }
        }
        ::metrics::counter!("torii_ecs_retrieve_events_rows_decoded_total")
            .increment(events.len() as u64);

        if direction_is_backward {
            events.reverse();
        }
        let next_cursor = events
            .last()
            .map(|(cursor, _)| cursor.clone())
            .unwrap_or_default();
        Ok((
            events.into_iter().map(|(_, event)| event).collect(),
            next_cursor,
        ))
    }

    async fn next_subscription_id() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }
}

fn record_active_entity_subscriptions(kind: TableKind, count: usize) {
    match kind {
        TableKind::Entity => {
            ::metrics::gauge!("torii_ecs_entity_subscriptions_active").set(count as f64);
        }
        TableKind::EventMessage => {
            ::metrics::gauge!("torii_ecs_event_message_subscriptions_active").set(count as f64);
        }
    }
}

fn record_active_entity_subscription_groups(kind: TableKind, count: usize) {
    match kind {
        TableKind::Entity => {
            ::metrics::gauge!("torii_ecs_entity_subscription_groups_active").set(count as f64);
        }
        TableKind::EventMessage => {
            ::metrics::gauge!("torii_ecs_event_message_subscription_groups_active")
                .set(count as f64);
        }
    }
}

#[tonic::async_trait]
impl World for EcsService {
    type SubscribeContractsStream = ManagedSubscriptionStream<SubscribeContractsResponse>;
    type SubscribeEntitiesStream = ManagedSubscriptionStream<SubscribeEntityResponse>;
    type SubscribeEventMessagesStream = ManagedSubscriptionStream<SubscribeEntityResponse>;
    type SubscribeTokenBalancesStream = ManagedSubscriptionStream<SubscribeTokenBalancesResponse>;
    type SubscribeTokensStream = ManagedSubscriptionStream<SubscribeTokensResponse>;
    type SubscribeTokenTransfersStream = ManagedSubscriptionStream<SubscribeTokenTransfersResponse>;
    type SubscribeEventsStream = ManagedSubscriptionStream<SubscribeEventsResponse>;
    type SubscribeTransactionsStream = ManagedSubscriptionStream<SubscribeTransactionsResponse>;

    async fn subscribe_contracts(
        &self,
        request: Request<SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let (sender, receiver) = mpsc::channel(256);
        for contract in self.load_contracts(&query).await.map_err(internal_status)? {
            let _ = sender
                .send(Ok(SubscribeContractsResponse {
                    contract: Some(contract),
                }))
                .await;
        }
        let subscription_id = Self::next_subscription_id().await;
        self.state
            .contract_subscriptions
            .lock()
            .await
            .insert(subscription_id, ContractSubscription { query, sender });
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::Contract,
            subscription_id,
            receiver,
        )))
    }

    async fn worlds(
        &self,
        request: Request<WorldsRequest>,
    ) -> Result<Response<WorldsResponse>, Status> {
        let start = Instant::now();
        let world_addresses = request
            .into_inner()
            .world_addresses
            .iter()
            .map(|bytes| felt_from_bytes(bytes))
            .collect::<Result<Vec<_>>>()
            .map_err(internal_status)?;
        let worlds = self.load_worlds(&world_addresses).await;
        ::metrics::histogram!("torii_ecs_worlds_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let worlds = worlds.map_err(internal_status)?;
        Ok(Response::new(WorldsResponse { worlds }))
    }

    async fn subscribe_entities(
        &self,
        request: Request<SubscribeEntitiesRequest>,
    ) -> Result<Response<Self::SubscribeEntitiesStream>, Status> {
        let start = Instant::now();
        ::metrics::counter!("torii_ecs_subscribe_entities_requests_total").increment(1);
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(2096);
        sender
            .try_send(Ok(SubscribeEntityResponse {
                entity: None,
                subscription_id,
            }))
            .map_err(|_| Status::internal("subscription setup failed"))?;
        self.state
            .insert_entity_subscription(
                TableKind::Entity,
                subscription_id,
                EntitySubscription {
                    clause: request.clause,
                    world_addresses: request.world_addresses.into_iter().collect(),
                    sender,
                },
            )
            .await;
        ::metrics::histogram!("torii_ecs_subscribe_entities_setup_seconds")
            .record(start.elapsed().as_secs_f64());
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::Entity(TableKind::Entity),
            subscription_id,
            receiver,
        )))
    }

    async fn update_entities_subscription(
        &self,
        request: Request<UpdateEntitiesSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let subscription_id = request.subscription_id;
        let clause = request.clause;
        let world_addresses = request.world_addresses.into_iter().collect();
        self.state
            .update_entity_subscription(TableKind::Entity, subscription_id, clause, world_addresses)
            .await;
        Ok(Response::new(()))
    }

    async fn retrieve_entities(
        &self,
        request: Request<RetrieveEntitiesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let start = Instant::now();
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let result = self.load_entity_page(TableKind::Entity, &query).await;
        ::metrics::histogram!("torii_ecs_retrieve_entities_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let (entities, next_cursor) = result.map_err(internal_status)?;
        ::metrics::counter!("torii_ecs_retrieve_entities_returned_total")
            .increment(entities.len() as u64);
        Ok(Response::new(RetrieveEntitiesResponse {
            next_cursor,
            entities,
        }))
    }

    async fn subscribe_event_messages(
        &self,
        request: Request<SubscribeEntitiesRequest>,
    ) -> Result<Response<Self::SubscribeEventMessagesStream>, Status> {
        let start = Instant::now();
        ::metrics::counter!("torii_ecs_subscribe_event_messages_requests_total").increment(1);
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .try_send(Ok(SubscribeEntityResponse {
                entity: None,
                subscription_id,
            }))
            .map_err(|_| Status::internal("subscription setup failed"))?;
        self.state
            .insert_entity_subscription(
                TableKind::EventMessage,
                subscription_id,
                EntitySubscription {
                    clause: request.clause,
                    world_addresses: request.world_addresses.into_iter().collect(),
                    sender,
                },
            )
            .await;
        ::metrics::histogram!("torii_ecs_subscribe_event_messages_setup_seconds")
            .record(start.elapsed().as_secs_f64());
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::Entity(TableKind::EventMessage),
            subscription_id,
            receiver,
        )))
    }

    async fn update_event_messages_subscription(
        &self,
        request: Request<UpdateEntitiesSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let subscription_id = request.subscription_id;
        let clause = request.clause;
        let world_addresses = request.world_addresses.into_iter().collect();
        self.state
            .update_entity_subscription(
                TableKind::EventMessage,
                subscription_id,
                clause,
                world_addresses,
            )
            .await;
        Ok(Response::new(()))
    }

    async fn retrieve_event_messages(
        &self,
        request: Request<RetrieveEntitiesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let start = Instant::now();
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let result = self.load_entity_page(TableKind::EventMessage, &query).await;
        ::metrics::histogram!("torii_ecs_retrieve_event_messages_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let (entities, next_cursor) = result.map_err(internal_status)?;
        ::metrics::counter!("torii_ecs_retrieve_event_messages_returned_total")
            .increment(entities.len() as u64);
        Ok(Response::new(RetrieveEntitiesResponse {
            next_cursor,
            entities,
        }))
    }

    async fn subscribe_token_balances(
        &self,
        request: Request<SubscribeTokenBalancesRequest>,
    ) -> Result<Response<Self::SubscribeTokenBalancesStream>, Status> {
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeTokenBalancesResponse {
                subscription_id,
                balance: None,
            }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        let latest = self
            .snapshot_token_balance_state(
                &request.account_addresses,
                &request.contract_addresses,
                &request.token_ids,
            )
            .await
            .map_err(internal_status)?;
        self.state.token_balance_subscriptions.lock().await.insert(
            subscription_id,
            TokenBalanceSubscription {
                contract_addresses: request.contract_addresses,
                account_addresses: request.account_addresses,
                token_ids: request.token_ids,
                latest,
                sender,
            },
        );
        self.spawn_token_balance_poll(subscription_id);
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::TokenBalance,
            subscription_id,
            receiver,
        )))
    }

    async fn update_token_balances_subscription(
        &self,
        request: Request<UpdateTokenBalancesSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let latest = self
            .snapshot_token_balance_state(
                &request.account_addresses,
                &request.contract_addresses,
                &request.token_ids,
            )
            .await
            .map_err(internal_status)?;
        if let Some(subscription) = self
            .state
            .token_balance_subscriptions
            .lock()
            .await
            .get_mut(&request.subscription_id)
        {
            subscription.contract_addresses = request.contract_addresses;
            subscription.account_addresses = request.account_addresses;
            subscription.token_ids = request.token_ids;
            subscription.latest = latest;
        }
        Ok(Response::new(()))
    }

    async fn subscribe_tokens(
        &self,
        request: Request<SubscribeTokensRequest>,
    ) -> Result<Response<Self::SubscribeTokensStream>, Status> {
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeTokensResponse {
                subscription_id,
                token: None,
            }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        let seen = self
            .snapshot_token_seen(&request.contract_addresses, &request.token_ids)
            .await
            .map_err(internal_status)?;
        self.state.token_subscriptions.lock().await.insert(
            subscription_id,
            TokenSubscription {
                contract_addresses: request.contract_addresses,
                token_ids: request.token_ids,
                seen,
                sender,
            },
        );
        self.spawn_token_poll(subscription_id);
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::Token,
            subscription_id,
            receiver,
        )))
    }

    async fn update_tokens_subscription(
        &self,
        request: Request<UpdateTokenSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let seen = self
            .snapshot_token_seen(&request.contract_addresses, &request.token_ids)
            .await
            .map_err(internal_status)?;
        if let Some(subscription) = self
            .state
            .token_subscriptions
            .lock()
            .await
            .get_mut(&request.subscription_id)
        {
            subscription.contract_addresses = request.contract_addresses;
            subscription.token_ids = request.token_ids;
            subscription.seen = seen;
        }
        Ok(Response::new(()))
    }

    async fn subscribe_token_transfers(
        &self,
        request: Request<SubscribeTokenTransfersRequest>,
    ) -> Result<Response<Self::SubscribeTokenTransfersStream>, Status> {
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeTokenTransfersResponse {
                subscription_id,
                transfer: None,
            }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        let seen = self
            .snapshot_token_transfer_seen(
                &request.account_addresses,
                &request.contract_addresses,
                &request.token_ids,
            )
            .await
            .map_err(internal_status)?;
        self.state.token_transfer_subscriptions.lock().await.insert(
            subscription_id,
            TokenTransferSubscription {
                contract_addresses: request.contract_addresses,
                account_addresses: request.account_addresses,
                token_ids: request.token_ids,
                seen,
                sender,
            },
        );
        self.spawn_token_transfer_poll(subscription_id);
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::TokenTransfer,
            subscription_id,
            receiver,
        )))
    }

    async fn update_token_transfers_subscription(
        &self,
        request: Request<UpdateTokenTransfersSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let seen = self
            .snapshot_token_transfer_seen(
                &request.account_addresses,
                &request.contract_addresses,
                &request.token_ids,
            )
            .await
            .map_err(internal_status)?;
        if let Some(subscription) = self
            .state
            .token_transfer_subscriptions
            .lock()
            .await
            .get_mut(&request.subscription_id)
        {
            subscription.contract_addresses = request.contract_addresses;
            subscription.account_addresses = request.account_addresses;
            subscription.token_ids = request.token_ids;
            subscription.seen = seen;
        }
        Ok(Response::new(()))
    }

    async fn retrieve_events(
        &self,
        request: Request<RetrieveEventsRequest>,
    ) -> Result<Response<RetrieveEventsResponse>, Status> {
        let start = Instant::now();
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let result = self.load_events_page(&query).await;
        ::metrics::histogram!("torii_ecs_retrieve_events_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let (events, next_cursor) = result.map_err(internal_status)?;
        ::metrics::counter!("torii_ecs_retrieve_events_returned_total")
            .increment(events.len() as u64);
        Ok(Response::new(RetrieveEventsResponse {
            next_cursor,
            events,
        }))
    }

    async fn subscribe_events(
        &self,
        request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeEventsResponse { event: None }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        self.state.event_subscriptions.lock().await.insert(
            subscription_id,
            EventSubscription {
                keys: request.keys,
                sender,
            },
        );
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::Event,
            subscription_id,
            receiver,
        )))
    }

    async fn retrieve_tokens(
        &self,
        request: Request<RetrieveTokensRequest>,
    ) -> Result<Response<RetrieveTokensResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let (tokens, next_cursor) = self.load_tokens(&query).await.map_err(internal_status)?;
        Ok(Response::new(RetrieveTokensResponse {
            next_cursor,
            tokens,
        }))
    }

    async fn retrieve_token_transfers(
        &self,
        request: Request<RetrieveTokenTransfersRequest>,
    ) -> Result<Response<RetrieveTokenTransfersResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let (transfers, next_cursor) = self
            .load_token_transfers(&query)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RetrieveTokenTransfersResponse {
            next_cursor,
            transfers,
        }))
    }

    async fn retrieve_token_balances(
        &self,
        request: Request<RetrieveTokenBalancesRequest>,
    ) -> Result<Response<RetrieveTokenBalancesResponse>, Status> {
        let start = Instant::now();
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let result = self.load_token_balances(&query).await;
        ::metrics::histogram!("torii_ecs_retrieve_token_balances_latency_seconds")
            .record(start.elapsed().as_secs_f64());
        let (balances, next_cursor) = result.map_err(internal_status)?;
        ::metrics::counter!("torii_ecs_retrieve_token_balances_returned_total")
            .increment(balances.len() as u64);
        Ok(Response::new(RetrieveTokenBalancesResponse {
            next_cursor,
            balances,
        }))
    }

    async fn retrieve_transactions(
        &self,
        request: Request<RetrieveTransactionsRequest>,
    ) -> Result<Response<RetrieveTransactionsResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let (transactions, next_cursor) = self
            .load_transactions(&query)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RetrieveTransactionsResponse {
            next_cursor,
            transactions,
        }))
    }

    async fn subscribe_transactions(
        &self,
        request: Request<SubscribeTransactionsRequest>,
    ) -> Result<Response<Self::SubscribeTransactionsStream>, Status> {
        let request = request.into_inner();
        let subscription_id = Self::next_subscription_id().await;
        let (sender, receiver) = mpsc::channel(256);
        sender
            .send(Ok(SubscribeTransactionsResponse { transaction: None }))
            .await
            .map_err(|_| Status::internal("subscription setup failed"))?;
        let filter = request.filter.unwrap_or_default();
        let seen = self
            .snapshot_transaction_seen(&filter)
            .await
            .map_err(internal_status)?;
        self.state.transaction_subscriptions.lock().await.insert(
            subscription_id,
            TransactionSubscription {
                filter,
                seen,
                sender,
            },
        );
        self.spawn_transaction_poll(subscription_id);
        Ok(Response::new(ManagedSubscriptionStream::new(
            Arc::clone(&self.state),
            SubscriptionKind::Transaction,
            subscription_id,
            receiver,
        )))
    }

    async fn retrieve_contracts(
        &self,
        request: Request<RetrieveContractsRequest>,
    ) -> Result<Response<RetrieveContractsResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let contracts = self.load_contracts(&query).await.map_err(internal_status)?;
        Ok(Response::new(RetrieveContractsResponse { contracts }))
    }

    async fn retrieve_controllers(
        &self,
        request: Request<RetrieveControllersRequest>,
    ) -> Result<Response<RetrieveControllersResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let (controllers, next_cursor) = self
            .load_controllers(&query)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RetrieveControllersResponse {
            next_cursor,
            controllers,
        }))
    }

    async fn retrieve_token_contracts(
        &self,
        request: Request<RetrieveTokenContractsRequest>,
    ) -> Result<Response<RetrieveTokenContractsResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Missing query argument"))?;
        let (token_contracts, next_cursor) = self
            .load_token_contracts(&query)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RetrieveTokenContractsResponse {
            next_cursor,
            token_contracts,
        }))
    }

    async fn execute_sql(
        &self,
        request: Request<types::SqlQueryRequest>,
    ) -> Result<Response<types::SqlQueryResponse>, Status> {
        let query = request.into_inner().query;
        let mut conn = self
            .acquire_initialized_connection()
            .await
            .map_err(internal_status)?;
        self.ensure_sql_compat_views_on_connection(&mut conn)
            .await
            .map_err(internal_status)?;
        let rows = sqlx::query(&query)
            .fetch_all(&mut *conn)
            .await
            .map_err(|error| Status::invalid_argument(format!("Query error: {error}")))?;
        Ok(Response::new(types::SqlQueryResponse {
            rows: rows.iter().map(row_to_sql_row).collect(),
        }))
    }
}

#[derive(Clone, Copy)]
struct EntityMetaRow {
    created_at: u64,
    updated_at: u64,
    executed_at: u64,
    deleted: bool,
}

struct EntityModelRow {
    world_address: String,
    table_id: String,
    entity_id: String,
    row_json: String,
    created_at: u64,
    updated_at: u64,
    executed_at: u64,
}

struct SnapshotJsonSerializer;

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

fn append_entity_key_pairs_filter<'a>(
    builder: &mut QueryBuilder<'a, Any>,
    world_column: &str,
    entity_column: &str,
    entity_keys: &'a [(String, String)],
) {
    builder.push(" AND (");
    builder.push(world_column);
    builder.push(", ");
    builder.push(entity_column);
    builder.push(") IN (");
    for (i, (world_address, entity_id)) in entity_keys.iter().enumerate() {
        if i > 0 {
            builder.push(", ");
        }
        builder.push("(");
        builder.push_bind(world_address);
        builder.push(", ");
        builder.push_bind(entity_id);
        builder.push(")");
    }
    builder.push(")");
}

fn sql_hex_blob_expr(backend: DbBackend, column: &str, width: usize) -> String {
    match backend {
        DbBackend::Sqlite => {
            let zeros = "0".repeat(width);
            format!("'0x' || lower(substr('{zeros}' || hex({column}), -{width}, {width}))")
        }
        DbBackend::Postgres => {
            format!("'0x' || lower(lpad(encode({column}, 'hex'), {width}, '0'))")
        }
    }
}

fn sql_hex_literal_expr(backend: DbBackend, value: &str, width: usize) -> String {
    match backend {
        DbBackend::Sqlite => {
            let zeros = "0".repeat(width);
            format!("'0x' || substr('{zeros}' || '{value}', -{width}, {width})")
        }
        DbBackend::Postgres => format!("'0x' || lpad('{value}', {width}, '0')"),
    }
}

async fn attach_sqlite_databases(
    conn: &mut AnyConnection,
    erc20_url: Option<&str>,
    erc721_url: Option<&str>,
    erc1155_url: Option<&str>,
) -> sqlx::Result<()> {
    for (schema, url) in [
        ("erc20", erc20_url),
        ("erc721", erc721_url),
        ("erc1155", erc1155_url),
    ] {
        if let Some(url) = url {
            let options = SqliteConnectOptions::from_str(url)?;
            attach_sqlite_database(conn, schema, &options).await?;
        }
    }
    Ok(())
}

async fn attach_sqlite_database(
    conn: &mut AnyConnection,
    schema: &str,
    options: &SqliteConnectOptions,
) -> sqlx::Result<()> {
    let attached =
        sqlx::query_scalar::<Any, i64>("SELECT 1 FROM pragma_database_list WHERE name = ? LIMIT 1")
            .bind(schema)
            .fetch_optional(&mut *conn)
            .await?
            .is_some();
    if attached {
        return Ok(());
    }
    let path = options.get_filename();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if !path.exists() {
        fs::File::create(path)?;
    }
    let path = path.to_string_lossy().replace('\'', "''");
    // sqlite-dynamic-ok: ATTACH requires the database path and schema identifier in SQL text.
    sqlx::query(&format!("ATTACH DATABASE '{path}' AS {schema}"))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

fn sqlite_master_preview_sql(schema: &str) -> &'static str {
    match schema {
        "erc20" => "SELECT name FROM erc20.sqlite_master WHERE type='table' LIMIT 5",
        "erc721" => "SELECT name FROM erc721.sqlite_master WHERE type='table' LIMIT 5",
        "erc1155" => "SELECT name FROM erc1155.sqlite_master WHERE type='table' LIMIT 5",
        _ => "SELECT name FROM sqlite_master WHERE 1 = 0",
    }
}

fn get_db_backend(url: &str) -> DbBackend {
    if url.starts_with("postgres") {
        DbBackend::Postgres
    } else {
        DbBackend::Sqlite
    }
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
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(options.to_url_lossy().to_string())
}

impl CairoTypeSerialization for SnapshotJsonSerializer {
    fn serialize_byte_array<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8],
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("0x{}", hex::encode(value)))
    }

    fn serialize_felt<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8; 32],
    ) -> Result<S::Ok, S::Error> {
        self.serialize_byte_array(serializer, value)
    }

    fn serialize_eth_address<S: Serializer>(
        &self,
        serializer: S,
        value: &[u8; 20],
    ) -> Result<S::Ok, S::Error> {
        self.serialize_byte_array(serializer, value)
    }

    fn serialize_u256<S: Serializer>(&self, serializer: S, value: U256) -> Result<S::Ok, S::Error> {
        let limbs = value.0;
        let corrected = U256([limbs[3], limbs[2], limbs[1], limbs[0]]);
        let bytes = corrected.to_big_endian();
        self.serialize_byte_array(serializer, &bytes)
    }

    fn serialize_u512<S: Serializer>(&self, serializer: S, value: U512) -> Result<S::Ok, S::Error> {
        let limbs = value.0;
        let corrected = U512([
            limbs[7], limbs[6], limbs[5], limbs[4], limbs[3], limbs[2], limbs[1], limbs[0],
        ]);
        let bytes = corrected.to_big_endian();
        self.serialize_byte_array(serializer, &bytes)
    }

    fn serialize_tuple<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        tuple: &'a TupleDef,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_map(Some(tuple.elements.len()))?;
        for (index, element) in tuple.elements.iter().enumerate() {
            seq.serialize_entry(&format!("_{index}"), &element.to_de_se(data, self))?;
        }
        seq.end()
    }

    fn serialize_variant<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        name: &str,
        type_def: &'a TypeDef,
    ) -> Result<S::Ok, S::Error> {
        if type_def == &TypeDef::None {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("variant", name)?;
            map
        } else {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("variant", name)?;
            map.serialize_entry(&format!("_{name}"), &type_def.to_de_se(data, self))?;
            map
        }
        .end()
    }

    fn serialize_result<'a, S: Serializer>(
        &'a self,
        data: &mut impl CairoDeserializer,
        serializer: S,
        result: &'a ResultDef,
        is_ok: bool,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("is_ok", &is_ok)?;
        if is_ok {
            map.serialize_entry("Ok", &result.ok.to_de_se(data, self))?;
        } else {
            map.serialize_entry("Err", &result.err.to_de_se(data, self))?;
        }
        map.end()
    }
}

fn model_from_table(table: &ManagedTable) -> types::Model {
    let (namespace, name) = split_table_name(&table.table.name);
    types::Model {
        selector: table.table.id.to_bytes_be().to_vec(),
        namespace,
        name,
        packed_size: table.table.columns.len() as u32,
        unpacked_size: table.table.columns.len() as u32,
        class_hash: Vec::new(),
        layout: Vec::new(),
        schema: serde_json::to_vec(&table.table).unwrap_or_default(),
        contract_address: Vec::new(),
        use_legacy_store: table.table.legacy,
        world_address: Vec::<u8>::from(table.world_address),
    }
}

fn split_table_name(name: &str) -> (String, String) {
    match name.split_once('-') {
        Some((namespace, model)) => (namespace.to_string(), model.to_string()),
        None => (String::new(), name.to_string()),
    }
}

fn row_to_model_struct(table: &DojoTable, row: &Map<String, Value>) -> Result<types::Struct> {
    let children = table
        .key_fields
        .iter()
        .chain(table.value_fields.iter())
        .filter_map(|column_id| table.columns.get(column_id))
        .map(|column| {
            let value = row.get(&column.name).unwrap_or(&Value::Null);
            Ok(types::Member {
                name: column.name.clone(),
                ty: Some(type_to_proto_value(&column.type_def, value)?),
                key: column.attributes.has_attribute("key"),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(types::Struct {
        name: table.name.clone(),
        children,
    })
}

fn record_to_json_map(
    table: &DojoTable,
    columns: &[Felt],
    record: &Record,
) -> Result<Map<String, Value>> {
    let schema_table = table.to_schema();
    let columns_by_id = schema_table
        .columns
        .iter()
        .cloned()
        .map(|column| (Felt::from(column.id), column))
        .collect::<HashMap<_, _>>();
    let schema_columns = columns
        .iter()
        .map(|column_id| {
            columns_by_id
                .get(column_id)
                .ok_or_else(|| anyhow!("column {column_id:#x} not found in table {}", table.name))
        })
        .collect::<Result<Vec<&ColumnDef>>>()?
        .into_iter()
        .cloned()
        .map(|column| ColumnInfo {
            name: column.name,
            attributes: column.attributes,
            type_def: column.type_def,
        })
        .collect::<Vec<_>>();
    let primary = schema_table.primary.into();
    let schema =
        torii_introspect::tables::RecordSchema::new(&primary, schema_columns.iter().collect());

    let mut bytes = Vec::new();
    let mut serializer = JsonSerializer::new(&mut bytes);
    schema.parse_records_with_metadata(
        std::slice::from_ref(record),
        &(),
        &mut serializer,
        &SnapshotJsonSerializer,
    )?;

    let value = serde_json::from_slice::<Vec<Value>>(&bytes)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("record serializer returned no rows"))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("record serializer must return an object"))
}

fn build_entity_from_snapshot(
    key: &(String, String),
    meta: &HashMap<String, EntityMetaRow>,
    rows: Option<&Vec<(String, String)>>,
    models: &HashMap<String, ManagedTable>,
) -> Result<Option<EntityAggregate>> {
    let Some(rows) = rows else {
        return Ok(None);
    };

    let mut aggregate = EntityAggregate {
        world_address: felt_like_string_to_bytes(&key.0)?,
        hashed_keys: felt_like_string_to_bytes(&key.1)?,
        ..EntityAggregate::default()
    };

    let build_start = Instant::now();
    for (table_id, row_json) in rows {
        let Some(table_meta) = meta.get(table_id) else {
            continue;
        };
        if table_meta.deleted {
            continue;
        }
        let Some(table) = models.get(table_id) else {
            continue;
        };

        let row_decode_start = Instant::now();
        let object: Map<String, Value> = serde_json::from_str(row_json)?;
        aggregate
            .models
            .push(row_to_model_struct(&table.table, &object)?);
        ::metrics::histogram!("torii_ecs_entity_snapshot_row_decode_seconds")
            .record(row_decode_start.elapsed().as_secs_f64());
        if aggregate.created_at == 0 || table_meta.created_at < aggregate.created_at {
            aggregate.created_at = table_meta.created_at;
        }
        aggregate.updated_at = aggregate.updated_at.max(table_meta.updated_at);
        aggregate.executed_at = aggregate.executed_at.max(table_meta.executed_at);
    }

    if aggregate.models.is_empty() {
        return Ok(None);
    }
    ::metrics::histogram!("torii_ecs_build_entity_from_snapshot_seconds")
        .record(build_start.elapsed().as_secs_f64());
    Ok(Some(aggregate))
}

#[allow(dead_code)]
fn any_row_to_json_map(row: &sqlx::any::AnyRow, table: &DojoTable) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    let primary_name = &table.primary.name;
    map.insert(
        primary_name.clone(),
        read_row_value(row, primary_name, &TypeDef::from(&table.primary.type_def))?,
    );
    for column in table.columns.values() {
        map.insert(
            column.name.clone(),
            read_row_value(row, &column.name, &column.type_def)?,
        );
    }
    Ok(map)
}

#[allow(dead_code)]
fn read_row_value(row: &sqlx::any::AnyRow, name: &str, type_def: &TypeDef) -> Result<Value> {
    if is_integer_type(type_def) {
        let value = row.try_get::<Option<i64>, _>(name)?;
        return Ok(value.map_or(Value::Null, |value| Value::Number(value.into())));
    }

    let value = row.try_get::<Option<String>, _>(name)?;
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    if is_json_text_type(type_def) {
        Ok(serde_json::from_str(&value).unwrap_or(Value::String(value)))
    } else {
        Ok(Value::String(value))
    }
}

#[allow(dead_code)]
fn is_integer_type(type_def: &TypeDef) -> bool {
    matches!(
        type_def,
        TypeDef::Bool
            | TypeDef::I8
            | TypeDef::I16
            | TypeDef::I32
            | TypeDef::I64
            | TypeDef::U8
            | TypeDef::U16
            | TypeDef::U32
    )
}

#[allow(dead_code)]
fn is_json_text_type(type_def: &TypeDef) -> bool {
    matches!(
        type_def,
        TypeDef::Tuple(_)
            | TypeDef::Array(_)
            | TypeDef::FixedArray(_)
            | TypeDef::Struct(_)
            | TypeDef::Enum(_)
            | TypeDef::Option(_)
            | TypeDef::Nullable(_)
            | TypeDef::Result(_)
            | TypeDef::Felt252Dict(_)
    )
}

fn type_to_proto_value(type_def: &TypeDef, value: &Value) -> Result<types::Ty> {
    use types::ty::TyType;

    let ty_type = match type_def {
        TypeDef::Bool => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::Bool(
                value.as_bool().unwrap_or_default(),
            )),
        }),
        TypeDef::I8 | TypeDef::I16 | TypeDef::I32 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::I32(
                value.as_i64().unwrap_or_default() as i32,
            )),
        }),
        TypeDef::I64 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::I64(
                value.as_i64().unwrap_or_default(),
            )),
        }),
        TypeDef::U8 | TypeDef::U16 | TypeDef::U32 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::U32(
                value.as_u64().unwrap_or_default() as u32,
            )),
        }),
        TypeDef::U64 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::U64(value_as_u64(value))),
        }),
        TypeDef::Felt252
        | TypeDef::ClassHash
        | TypeDef::ContractAddress
        | TypeDef::StorageAddress
        | TypeDef::StorageBaseAddress => {
            let bytes = value_as_felt_bytes(value)?;
            let primitive = match type_def {
                TypeDef::ClassHash => types::primitive::PrimitiveType::ClassHash(bytes),
                TypeDef::ContractAddress
                | TypeDef::StorageAddress
                | TypeDef::StorageBaseAddress => {
                    types::primitive::PrimitiveType::ContractAddress(bytes)
                }
                _ => types::primitive::PrimitiveType::Felt252(bytes),
            };
            TyType::Primitive(types::Primitive {
                primitive_type: Some(primitive),
            })
        }
        TypeDef::EthAddress => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::EthAddress(
                value_as_fixed_unsigned_bytes(value, 20)?,
            )),
        }),
        TypeDef::U128 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::U128(
                value_as_fixed_unsigned_bytes(value, 16)?,
            )),
        }),
        TypeDef::I128 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::I128(
                value_as_fixed_signed_bytes(value, 16)?,
            )),
        }),
        TypeDef::U256 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::U256(
                value_as_fixed_unsigned_bytes(value, 32)?,
            )),
        }),
        TypeDef::U512 => TyType::Primitive(types::Primitive {
            primitive_type: Some(types::primitive::PrimitiveType::U256(
                value_as_fixed_unsigned_bytes(value, 64)?,
            )),
        }),
        TypeDef::Utf8String
        | TypeDef::ShortUtf8
        | TypeDef::ByteArray
        | TypeDef::ByteArrayEncoded(_)
        | TypeDef::Bytes31
        | TypeDef::Bytes31Encoded(_) => TyType::Bytearray(value_as_string(value)),
        TypeDef::Struct(def) => {
            let object = value.as_object();
            TyType::Struct(types::Struct {
                name: def.name.clone(),
                children: def
                    .members
                    .iter()
                    .map(|member| {
                        let member_value = object
                            .and_then(|object| object.get(&member.name))
                            .unwrap_or(&Value::Null);
                        Ok(types::Member {
                            name: member.name.clone(),
                            ty: Some(type_to_proto_value(&member.type_def, member_value)?),
                            key: member.attributes.has_attribute("key"),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        TypeDef::Tuple(def) => TyType::Tuple(types::Array {
            children: value
                .as_array()
                .map_or(&[] as &[Value], Vec::as_slice)
                .iter()
                .zip(def.elements.iter())
                .map(|(item, inner)| type_to_proto_value(inner, item))
                .collect::<Result<Vec<_>>>()?,
        }),
        TypeDef::Array(def) => TyType::Array(types::Array {
            children: value
                .as_array()
                .map_or(&[] as &[Value], Vec::as_slice)
                .iter()
                .map(|item| type_to_proto_value(&def.type_def, item))
                .collect::<Result<Vec<_>>>()?,
        }),
        TypeDef::FixedArray(def) => TyType::FixedSizeArray(types::FixedSizeArray {
            children: value
                .as_array()
                .map_or(&[] as &[Value], Vec::as_slice)
                .iter()
                .map(|item| type_to_proto_value(&def.type_def, item))
                .collect::<Result<Vec<_>>>()?,
            size: def.size,
        }),
        TypeDef::Enum(def) => {
            let object = value.as_object();
            let variant_name = object
                .and_then(|object| object.get("option"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let (option, options) = def
                .order
                .iter()
                .enumerate()
                .filter_map(|(index, selector)| {
                    def.variants.get(selector).map(|variant| (index, variant))
                })
                .map(|(index, variant)| {
                    let ty = object
                        .and_then(|object| object.get(&variant.name))
                        .map(|value| type_to_proto_value(&variant.type_def, value))
                        .transpose()?
                        .unwrap_or_default();
                    Ok::<_, anyhow::Error>((
                        if variant.name == variant_name {
                            index as u32
                        } else {
                            u32::MAX
                        },
                        types::EnumOption {
                            name: variant.name.clone(),
                            ty: Some(ty),
                        },
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .fold(
                    (0, Vec::new()),
                    |(selected, mut options), (index, option)| {
                        let selected = if index == u32::MAX { selected } else { index };
                        options.push(option);
                        (selected, options)
                    },
                );
            TyType::Enum(types::Enum {
                name: def.name.clone(),
                option,
                options,
            })
        }
        TypeDef::Option(def) => {
            if value.is_null() {
                TyType::Bytearray(String::new())
            } else {
                return type_to_proto_value(&def.type_def, value);
            }
        }
        TypeDef::Nullable(def) => {
            if value.is_null() {
                TyType::Bytearray(String::new())
            } else {
                return type_to_proto_value(&def.type_def, value);
            }
        }
        TypeDef::Result(def) => {
            let object = value.as_object();
            if let Some(ok) = object.and_then(|object| object.get("Ok")) {
                return type_to_proto_value(&def.ok, ok);
            }
            if let Some(err) = object.and_then(|object| object.get("Err")) {
                return type_to_proto_value(&def.err, err);
            }
            TyType::Bytearray(value_as_string(value))
        }
        TypeDef::None | TypeDef::Felt252Dict(_) | TypeDef::Ref(_) | TypeDef::Custom(_) => {
            TyType::Bytearray(value_as_string(value))
        }
    };

    Ok(types::Ty {
        ty_type: Some(ty_type),
    })
}

fn value_as_u64(value: &Value) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_i64().map(|value| value as u64))
        .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
        .unwrap_or_default()
}

fn value_as_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn value_as_felt_bytes(value: &Value) -> Result<Vec<u8>> {
    if value.is_null() {
        return Ok(Felt::ZERO.into());
    }

    let string = value_as_string(value);
    let trimmed = string.trim();
    if trimmed.is_empty() {
        return Ok(Felt::ZERO.into());
    }

    felt_like_string_to_bytes(trimmed)
}

fn value_as_fixed_unsigned_bytes(value: &Value, width: usize) -> Result<Vec<u8>> {
    if let Some(value) = value.as_str() {
        let trimmed = value.trim();
        if let Some(bytes) = decode_prefixed_hex_to_padded_bytes(trimmed, width)? {
            return Ok(bytes);
        }
    }

    let unsigned = value_as_big_uint(value)?;
    let bytes = unsigned.to_bytes_be();
    if bytes.len() > width {
        anyhow::bail!("value does not fit in {width} bytes");
    }

    let mut out = vec![0_u8; width];
    out[width - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}

fn value_as_fixed_signed_bytes(value: &Value, width: usize) -> Result<Vec<u8>> {
    let signed = value_as_big_int(value)?;
    let bytes = signed.to_signed_bytes_be();
    if bytes.len() > width {
        anyhow::bail!("value does not fit in {width} bytes");
    }

    let pad = if signed.sign() == Sign::Minus {
        0xff
    } else {
        0x00
    };
    let mut out = vec![pad; width];
    out[width - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}

fn value_as_big_uint(value: &Value) -> Result<BigUint> {
    let parsed = value_as_big_int(value)?;
    parsed
        .to_biguint()
        .ok_or_else(|| anyhow!("expected unsigned value, got negative integer"))
}

fn value_as_big_int(value: &Value) -> Result<BigInt> {
    if value.is_null() {
        return Ok(BigInt::from(0_u8));
    }

    if let Some(value) = value.as_i64() {
        return Ok(BigInt::from(value));
    }

    if let Some(value) = value.as_u64() {
        return Ok(BigInt::from(value));
    }

    parse_big_int(value_as_string(value).trim())
}

fn parse_big_int(raw: &str) -> Result<BigInt> {
    if raw.is_empty() {
        return Ok(BigInt::from(0_u8));
    }

    if let Some(hex) = raw.strip_prefix("-0x") {
        let magnitude = parse_big_uint_with_radix(hex, 16)?;
        return Ok(-BigInt::from(magnitude));
    }

    if let Some(hex) = raw.strip_prefix("0x") {
        let magnitude = parse_big_uint_with_radix(hex, 16)?;
        return Ok(BigInt::from(magnitude));
    }

    if raw.contains(['e', 'E']) {
        return parse_scientific_big_int(raw);
    }

    BigInt::parse_bytes(raw.as_bytes(), 10)
        .ok_or_else(|| anyhow!("failed to parse integer value: {raw}"))
}

fn parse_scientific_big_int(raw: &str) -> Result<BigInt> {
    let (mantissa, exponent) = raw
        .split_once(['e', 'E'])
        .ok_or_else(|| anyhow!("failed to parse integer value: {raw}"))?;
    let exponent: i64 = exponent
        .parse()
        .map_err(|_| anyhow!("failed to parse integer value: {raw}"))?;

    let (negative, mantissa) = if let Some(rest) = mantissa.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = mantissa.strip_prefix('+') {
        (false, rest)
    } else {
        (false, mantissa)
    };

    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fractional}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(BigInt::from(0_u8));
    }

    let scale = exponent - fractional.len() as i64;
    let magnitude = if scale >= 0 {
        let zeros = "0".repeat(scale as usize);
        BigInt::parse_bytes(format!("{digits}{zeros}").as_bytes(), 10)
    } else {
        let split_at = digits
            .len()
            .checked_sub((-scale) as usize)
            .ok_or_else(|| anyhow!("failed to parse integer value: {raw}"))?;
        let (integer, remainder) = digits.split_at(split_at);
        if remainder.bytes().any(|byte| byte != b'0') {
            return Err(anyhow!("failed to parse integer value: {raw}"));
        }
        let integer = if integer.is_empty() { "0" } else { integer };
        BigInt::parse_bytes(integer.as_bytes(), 10)
    }
    .ok_or_else(|| anyhow!("failed to parse integer value: {raw}"))?;

    Ok(if negative { -magnitude } else { magnitude })
}

fn parse_big_uint_with_radix(raw: &str, radix: u32) -> Result<BigUint> {
    if raw.is_empty() {
        return Ok(BigUint::default());
    }

    BigUint::parse_bytes(raw.as_bytes(), radix)
        .ok_or_else(|| anyhow!("failed to parse integer value: {raw}"))
}

fn felt_like_string_to_bytes(raw: &str) -> Result<Vec<u8>> {
    let trimmed = raw.trim();
    if let Some(bytes) = decode_prefixed_hex_to_padded_bytes(trimmed, 32)? {
        return Ok(bytes);
    }

    Ok(felt_from_hex(trimmed)?.into())
}

fn decode_prefixed_hex_to_padded_bytes(raw: &str, width: usize) -> Result<Option<Vec<u8>>> {
    let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) else {
        return Ok(None);
    };

    if hex.is_empty() {
        return Ok(Some(vec![0_u8; width]));
    }

    let mut normalized = String::with_capacity(hex.len() + (hex.len() % 2));
    if hex.len() % 2 != 0 {
        normalized.push('0');
    }
    normalized.push_str(hex);

    let bytes = hex::decode(&normalized)
        .map_err(|err| anyhow!("failed to parse hex value {raw}: {err}"))?;
    if bytes.len() > width {
        anyhow::bail!("value does not fit in {width} bytes");
    }

    let mut out = vec![0_u8; width];
    out[width - bytes.len()..].copy_from_slice(&bytes);
    Ok(Some(out))
}

fn push_blob_in_filter(
    builder: &mut QueryBuilder<'_, Any>,
    column: &str,
    values: &[Vec<u8>],
) -> bool {
    if values.is_empty() {
        return false;
    }
    builder.push(format!(" WHERE hex({column}) IN ("));
    {
        let mut separated = builder.separated(", ");
        for value in values {
            separated.push_bind(hex::encode_upper(value));
        }
    }
    builder.push(")");
    true
}

fn push_blob_in_filter_with_mode(
    builder: &mut QueryBuilder<'_, Any>,
    column: &str,
    values: &[Vec<u8>],
    has_where: bool,
) -> bool {
    if values.is_empty() {
        return has_where;
    }
    if has_where {
        builder.push(" AND ");
    } else {
        builder.push(" WHERE ");
    }
    builder.push(column);
    builder.push(" IN (");
    {
        let mut separated = builder.separated(", ");
        for value in values {
            separated.push_bind(value.clone());
        }
    }
    builder.push(")");
    true
}

fn normalize_token_filter_values(values: &[Vec<u8>]) -> Vec<Vec<u8>> {
    values
        .iter()
        .map(|value| {
            let first_non_zero = value.iter().position(|byte| *byte != 0);
            match first_non_zero {
                Some(index) => value[index..].to_vec(),
                None => vec![0],
            }
        })
        .collect()
}

fn sql_blob_numeric_order_expr(backend: DbBackend, column: &str) -> String {
    match backend {
        DbBackend::Sqlite => format!("length({column})"),
        DbBackend::Postgres => format!("octet_length({column})"),
    }
}

fn push_transfer_filters(
    builder: &mut QueryBuilder<'_, Any>,
    query: &types::TokenTransferQuery,
    contract_column: &str,
    from_column: &str,
    to_column: &str,
    token_id_column: Option<&str>,
) {
    let mut has_where = if query.contract_addresses.is_empty() {
        false
    } else {
        builder.push(format!(" WHERE hex({contract_column}) IN ("));
        {
            let mut separated = builder.separated(", ");
            for contract in &query.contract_addresses {
                separated.push_bind(hex::encode_upper(contract));
            }
        }
        builder.push(")");
        true
    };
    if !query.account_addresses.is_empty() {
        if has_where {
            builder.push(" AND ");
        } else {
            builder.push(" WHERE ");
            has_where = true;
        }
        builder.push(format!("(hex({from_column}) IN ("));
        {
            let mut separated = builder.separated(", ");
            for account in &query.account_addresses {
                separated.push_bind(hex::encode_upper(account));
            }
        }
        builder.push(format!(") OR hex({to_column}) IN ("));
        {
            let mut separated = builder.separated(", ");
            for account in &query.account_addresses {
                separated.push_bind(hex::encode_upper(account));
            }
        }
        builder.push("))");
    }
    if let Some(token_id_column) = token_id_column {
        if !query.token_ids.is_empty() {
            if has_where {
                builder.push(" AND ");
            } else {
                builder.push(" WHERE ");
            }
            builder.push(format!("hex({token_id_column}) IN ("));
            {
                let mut separated = builder.separated(", ");
                for token_id in &query.token_ids {
                    separated.push_bind(hex::encode_upper(token_id));
                }
            }
            builder.push(")");
        }
    }
}

fn token_subscription_key(token: &types::Token) -> String {
    format!(
        "{}:{}",
        hex::encode(&token.contract_address),
        token.token_id.as_ref().map(hex::encode).unwrap_or_default()
    )
}

fn token_balance_identity_key(balance: &types::TokenBalance) -> String {
    format!(
        "{}:{}:{}",
        hex::encode(&balance.contract_address),
        hex::encode(&balance.account_address),
        balance
            .token_id
            .as_ref()
            .map(hex::encode)
            .unwrap_or_default()
    )
}

fn token_balance_state_fingerprint(balance: &types::TokenBalance) -> Vec<u8> {
    balance.balance.clone()
}

fn token_balance_subscription_state(balances: &[types::TokenBalance]) -> HashMap<String, Vec<u8>> {
    balances
        .iter()
        .map(|balance| {
            (
                token_balance_identity_key(balance),
                token_balance_state_fingerprint(balance),
            )
        })
        .collect()
}

fn canonical_optional_u256_bytes_from_db(bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>> {
    bytes
        .as_deref()
        .map(canonical_u256_bytes_from_db)
        .transpose()
}

fn canonical_u256_bytes_from_db(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() > 32 {
        return Err(anyhow!("U256 payload exceeds 32 bytes"));
    }

    let value = if bytes.is_empty() {
        U256::zero()
    } else {
        U256::from_big_endian(bytes)
    };
    Ok(value.to_big_endian().to_vec())
}

fn u256_bytes_from_u64(value: u64) -> Vec<u8> {
    let mut out = vec![0_u8; 32];
    out[24..].copy_from_slice(&value.to_be_bytes());
    out
}

fn row_to_sql_row(row: &sqlx::any::AnyRow) -> types::SqlRow {
    let mut fields = HashMap::new();
    for (index, column) in row.columns().iter().enumerate() {
        let value_type = if let Ok(value) = row.try_get::<Option<String>, _>(index) {
            value.map(types::sql_value::ValueType::Text)
        } else if let Ok(value) = row.try_get::<Option<i64>, _>(index) {
            value.map(types::sql_value::ValueType::Integer)
        } else if let Ok(value) = row.try_get::<Option<f64>, _>(index) {
            value.map(types::sql_value::ValueType::Real)
        } else if let Ok(value) = row.try_get::<Option<Vec<u8>>, _>(index) {
            value.map(types::sql_value::ValueType::Blob)
        } else if let Ok(value) = row.try_get::<Option<bool>, _>(index) {
            value.map(|value| types::sql_value::ValueType::Text(value.to_string()))
        } else {
            None
        };
        fields.insert(
            column.name().to_string(),
            types::SqlValue {
                value_type: Some(value_type.unwrap_or(types::sql_value::ValueType::Null(true))),
            },
        );
    }
    types::SqlRow { fields }
}

#[allow(dead_code)]
fn value_to_primary_felt(value: &Value, type_def: &PrimaryTypeDef) -> Result<Felt> {
    match type_def {
        PrimaryTypeDef::Bool => Ok(Felt::from(value.as_bool().unwrap_or_default() as u64)),
        PrimaryTypeDef::U8
        | PrimaryTypeDef::U16
        | PrimaryTypeDef::U32
        | PrimaryTypeDef::U64
        | PrimaryTypeDef::U128
        | PrimaryTypeDef::I8
        | PrimaryTypeDef::I16
        | PrimaryTypeDef::I32
        | PrimaryTypeDef::I64
        | PrimaryTypeDef::I128 => Ok(Felt::from(value_as_u64(value))),
        _ => felt_from_hex(&value_as_string(value)),
    }
}

fn member_pushdown_from_clause(
    clause: Option<&types::Clause>,
    models: &HashMap<String, ManagedTable>,
) -> Option<MemberPushdown> {
    let ClauseType::Member(member) = clause?.clause_type.as_ref()? else {
        return None;
    };
    if member.member.contains('.') {
        return None;
    }
    if !member
        .member
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return None;
    }

    let expected = scalar_from_member_value(member.value.as_ref()?)?;
    if matches!(expected, ScalarValue::Bytes(_)) {
        return None;
    }

    let table = models
        .values()
        .find(|table| table.table.name == member.model)?;
    if !table
        .table
        .columns
        .values()
        .any(|column| column.name == member.member)
    {
        return None;
    }

    Some(MemberPushdown {
        table_id: felt_hex(table.table.id),
        member: member.member.clone(),
        operator: member.operator,
        expected,
    })
}

fn sql_operator(operator: i32) -> Option<&'static str> {
    match operator {
        value if value == ComparisonOperator::Eq as i32 => Some("="),
        value if value == ComparisonOperator::Neq as i32 => Some("!="),
        value if value == ComparisonOperator::Gt as i32 => Some(">"),
        value if value == ComparisonOperator::Gte as i32 => Some(">="),
        value if value == ComparisonOperator::Lt as i32 => Some("<"),
        value if value == ComparisonOperator::Lte as i32 => Some("<="),
        _ => None,
    }
}

fn append_member_pushdown_sql(
    builder: &mut QueryBuilder<'_, Any>,
    backend: DbBackend,
    pushdown: &MemberPushdown,
) {
    let Some(operator) = sql_operator(pushdown.operator) else {
        return;
    };

    builder.push(" AND EXISTS (SELECT 1 FROM torii_ecs_entity_models em WHERE em.kind = m.kind");
    builder.push(
        " AND em.world_address = m.world_address AND em.table_id = m.table_id AND em.entity_id = m.entity_id",
    );
    builder.push(" AND em.table_id = ");
    builder.push_bind(pushdown.table_id.clone());

    match (&pushdown.expected, backend) {
        (ScalarValue::Bool(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS INTEGER) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(i64::from(*value));
        }
        (ScalarValue::Bool(value), DbBackend::Postgres) => {
            builder.push(" AND CASE WHEN lower((em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("')) = 'true' THEN 1 ELSE 0 END ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(i64::from(*value));
        }
        (ScalarValue::Signed(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS INTEGER) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value);
        }
        (ScalarValue::Signed(value), DbBackend::Postgres) => {
            builder.push(" AND CAST((em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("') AS BIGINT) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value);
        }
        (ScalarValue::Unsigned(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS INTEGER) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value as i64);
        }
        (ScalarValue::Unsigned(value), DbBackend::Postgres) => {
            builder.push(" AND CAST((em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("') AS BIGINT) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(*value as i64);
        }
        (ScalarValue::Text(value), DbBackend::Sqlite) => {
            builder.push(" AND CAST(json_extract(em.row_json, '$.");
            builder.push(pushdown.member.as_str());
            builder.push("') AS TEXT) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(value.clone());
        }
        (ScalarValue::Text(value), DbBackend::Postgres) => {
            builder.push(" AND (em.row_json::jsonb ->> '");
            builder.push(pushdown.member.as_str());
            builder.push("') ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(value.clone());
        }
        (ScalarValue::Bytes(_), _) => {}
    }

    builder.push(")");
}

fn entity_matches_clause(entity: &types::Entity, clause: &types::Clause) -> bool {
    match &clause.clause_type {
        Some(ClauseType::HashedKeys(hashed)) => {
            hashed.hashed_keys.is_empty() || hashed.hashed_keys.contains(&entity.hashed_keys)
        }
        Some(ClauseType::Keys(keys)) => entity_matches_keys_clause(entity, keys),
        Some(ClauseType::Member(member)) => entity.models.iter().any(|model| {
            model.name == member.model
                && model_member_matches(
                    model,
                    &member.member,
                    member.operator,
                    member.value.as_ref(),
                )
        }),
        Some(ClauseType::Composite(composite)) => match composite.operator {
            value if value == LogicalOperator::And as i32 => composite
                .clauses
                .iter()
                .all(|clause| entity_matches_clause(entity, clause)),
            _ => composite
                .clauses
                .iter()
                .any(|clause| entity_matches_clause(entity, clause)),
        },
        None => true,
    }
}

fn match_keys(keys: &[Vec<u8>], clauses: &[types::KeysClause]) -> bool {
    clauses.is_empty()
        || clauses
            .iter()
            .any(|clause| keys_clause_matches_entity_keys(keys, clause))
}

fn entity_matches_keys_clause(entity: &types::Entity, clause: &types::KeysClause) -> bool {
    if clause.models.is_empty() {
        return keys_clause_matches_entity_keys(&entity_key_values(entity), clause);
    }

    entity
        .models
        .iter()
        .filter(|model| clause.models.contains(&model.name))
        .filter_map(model_key_values)
        .any(|keys| keys_clause_matches_entity_keys(&keys, clause))
}

fn keys_clause_matches_entity_keys(entity_keys: &[Vec<u8>], clause: &types::KeysClause) -> bool {
    if clause.pattern_matching == PatternMatching::FixedLen as i32
        && entity_keys.len() != clause.keys.len()
    {
        return false;
    }

    entity_keys.iter().enumerate().all(|(index, key)| {
        clause
            .keys
            .get(index)
            .is_none_or(|expected| expected.is_empty() || expected == key)
    })
}

fn model_member_matches(
    model: &types::Struct,
    path: &str,
    operator: i32,
    value: Option<&types::MemberValue>,
) -> bool {
    let mut current = find_member_value(model, path);
    let Some(current) = current.take() else {
        return false;
    };
    let Some(value) = value else {
        return false;
    };
    compare_member_value(current, operator, value)
}

fn find_member_value<'a>(model: &'a types::Struct, path: &str) -> Option<&'a types::Ty> {
    let mut members = &model.children;
    let parts = path.split('.').collect::<Vec<_>>();
    let mut current = None;
    for (index, part) in parts.iter().enumerate() {
        let member = members.iter().find(|member| member.name == *part)?;
        current = member.ty.as_ref();
        if index + 1 == parts.len() {
            return current;
        }
        let ty = current?.ty_type.as_ref()?;
        match ty {
            types::ty::TyType::Struct(value) => {
                members = &value.children;
            }
            _ => return None,
        }
    }
    current
}

fn compare_member_value(current: &types::Ty, operator: i32, expected: &types::MemberValue) -> bool {
    let current_value = scalar_from_ty(current);
    let expected_value = scalar_from_member_value(expected);
    match (current_value, expected_value) {
        (Some(current), Some(expected)) => compare_scalars(&current, operator, &expected),
        _ => false,
    }
}

fn scalar_from_ty(value: &types::Ty) -> Option<ScalarValue> {
    match value.ty_type.as_ref()? {
        types::ty::TyType::Primitive(primitive) => match primitive.primitive_type.as_ref()? {
            types::primitive::PrimitiveType::Bool(value) => Some(ScalarValue::Bool(*value)),
            types::primitive::PrimitiveType::I32(value) => Some(ScalarValue::Signed(*value as i64)),
            types::primitive::PrimitiveType::I64(value) => Some(ScalarValue::Signed(*value)),
            types::primitive::PrimitiveType::U32(value) => {
                Some(ScalarValue::Unsigned(*value as u64))
            }
            types::primitive::PrimitiveType::U64(value) => Some(ScalarValue::Unsigned(*value)),
            types::primitive::PrimitiveType::Felt252(value)
            | types::primitive::PrimitiveType::ClassHash(value)
            | types::primitive::PrimitiveType::ContractAddress(value)
            | types::primitive::PrimitiveType::EthAddress(value)
            | types::primitive::PrimitiveType::U256(value)
            | types::primitive::PrimitiveType::U128(value)
            | types::primitive::PrimitiveType::I128(value) => {
                Some(ScalarValue::Bytes(value.clone()))
            }
            _ => None,
        },
        types::ty::TyType::Bytearray(value) => Some(ScalarValue::Text(value.clone())),
        _ => None,
    }
}

fn scalar_from_member_value(value: &types::MemberValue) -> Option<ScalarValue> {
    match value.value_type.as_ref()? {
        ValueType::Primitive(primitive) => scalar_from_ty(&types::Ty {
            ty_type: Some(types::ty::TyType::Primitive(primitive.clone())),
        }),
        ValueType::String(value) => Some(ScalarValue::Text(value.clone())),
        ValueType::List(_) => None,
    }
}

#[derive(Clone)]
enum ScalarValue {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Text(String),
    Bytes(Vec<u8>),
}

struct MemberPushdown {
    table_id: String,
    member: String,
    operator: i32,
    expected: ScalarValue,
}

fn compare_scalars(current: &ScalarValue, operator: i32, expected: &ScalarValue) -> bool {
    match (current, expected) {
        (ScalarValue::Bool(current), ScalarValue::Bool(expected)) => match operator {
            value if value == ComparisonOperator::Eq as i32 => current == expected,
            value if value == ComparisonOperator::Neq as i32 => current != expected,
            _ => false,
        },
        (ScalarValue::Signed(current), ScalarValue::Signed(expected)) => {
            compare_ord(current, operator, expected)
        }
        (ScalarValue::Unsigned(current), ScalarValue::Unsigned(expected)) => {
            compare_ord(current, operator, expected)
        }
        (ScalarValue::Text(current), ScalarValue::Text(expected)) => {
            compare_ord(current, operator, expected)
        }
        (ScalarValue::Bytes(current), ScalarValue::Bytes(expected)) => {
            compare_ord(current, operator, expected)
        }
        _ => false,
    }
}

fn compare_ord<T: PartialEq + PartialOrd>(current: &T, operator: i32, expected: &T) -> bool {
    match operator {
        value if value == ComparisonOperator::Eq as i32 => current == expected,
        value if value == ComparisonOperator::Neq as i32 => current != expected,
        value if value == ComparisonOperator::Gt as i32 => current > expected,
        value if value == ComparisonOperator::Gte as i32 => current >= expected,
        value if value == ComparisonOperator::Lt as i32 => current < expected,
        value if value == ComparisonOperator::Lte as i32 => current <= expected,
        _ => false,
    }
}

fn entity_key_values(entity: &types::Entity) -> Vec<Vec<u8>> {
    entity
        .models
        .iter()
        .find_map(model_key_values)
        .unwrap_or_default()
}

fn model_key_values(model: &types::Struct) -> Option<Vec<Vec<u8>>> {
    let keys = model
        .children
        .iter()
        .filter(|member| member.key)
        .filter_map(|member| {
            scalar_from_ty(member.ty.as_ref()?).map(|value| match value {
                ScalarValue::Bool(value) => vec![u8::from(value)],
                ScalarValue::Signed(value) => value.to_be_bytes().to_vec(),
                ScalarValue::Unsigned(value) => value.to_be_bytes().to_vec(),
                ScalarValue::Text(value) => value.into_bytes(),
                ScalarValue::Bytes(value) => value,
            })
        })
        .collect::<Vec<_>>();
    (!keys.is_empty()).then_some(keys)
}

fn contract_matches_query(contract: &types::Contract, query: &types::ContractQuery) -> bool {
    (query.contract_addresses.is_empty()
        || query
            .contract_addresses
            .contains(&contract.contract_address))
        && (query.contract_types.is_empty()
            || query.contract_types.contains(&contract.contract_type))
}

fn felt_hex<T: Into<Felt>>(value: T) -> String {
    let value: Felt = value.into();
    format!("0x{}", hex::encode(<[u8; 32]>::from(value)))
}

fn felt_from_bytes(value: &[u8]) -> Result<Felt> {
    Felt::from_be_bytes_slice(value).map_err(|err| anyhow!("invalid felt bytes: {err}"))
}

fn felt_from_hex(value: &str) -> Result<Felt> {
    Felt::from_str(value).map_err(|err| anyhow!("invalid felt {value}: {err}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use introspect_types::{Attribute, ColumnDef, PrimaryDef, PrimaryTypeDef, TypeDef};
    use tokio::time::{timeout, Duration};
    use tokio_stream::StreamExt;

    fn test_db_path(name: &str) -> String {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        format!("sqlite:file:torii-ecs-sink-{name}-{nonce}?mode=memory&cache=shared")
    }

    fn temp_sqlite_url(name: &str) -> String {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("torii-ecs-sink-{name}-{nonce}.db"));
        format!("sqlite://{}", path.display())
    }

    fn sql_rows_to_maps(rows: &[types::SqlRow]) -> Vec<BTreeMap<String, String>> {
        rows.iter()
            .map(|row| {
                row.fields
                    .iter()
                    .map(|(name, value)| {
                        let value = match value.value_type.as_ref() {
                            Some(types::sql_value::ValueType::Text(value)) => value.clone(),
                            Some(types::sql_value::ValueType::Integer(value)) => value.to_string(),
                            Some(types::sql_value::ValueType::Real(value)) => value.to_string(),
                            Some(types::sql_value::ValueType::Blob(value)) => {
                                format!("0x{}", hex::encode(value))
                            }
                            Some(types::sql_value::ValueType::Null(_)) | None => String::new(),
                        };
                        (name.clone(), value)
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .collect()
    }

    async fn explain_query_plan_details(conn: &mut PoolConnection<Any>, sql: &str) -> Vec<String> {
        // sqlite-dynamic-ok: test-only helper that needs to wrap arbitrary SQL in EXPLAIN QUERY PLAN.
        sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
            .fetch_all(&mut **conn)
            .await
            .expect("explain query plan")
            .into_iter()
            .map(|row| row.try_get::<String, _>("detail").expect("detail column"))
            .collect()
    }

    async fn wait_for_entity_subscription_count(
        service: &EcsService,
        kind: TableKind,
        expected: usize,
    ) {
        timeout(Duration::from_secs(1), async {
            loop {
                let count = service
                    .state
                    .entity_subscription_registry(kind)
                    .read()
                    .await
                    .subscription_count();
                if count == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription count timeout");
    }

    async fn wait_for_entity_subscription_group_count(
        service: &EcsService,
        kind: TableKind,
        expected: usize,
    ) {
        timeout(Duration::from_secs(1), async {
            loop {
                let count = service
                    .state
                    .entity_subscription_registry(kind)
                    .read()
                    .await
                    .group_count();
                if count == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription group count timeout");
    }

    async fn wait_for_subscription_count(
        service: &EcsService,
        kind: SubscriptionKind,
        expected: usize,
    ) {
        timeout(Duration::from_secs(1), async {
            loop {
                let count = match kind {
                    SubscriptionKind::Entity(kind) => service
                        .state
                        .entity_subscription_registry(kind)
                        .read()
                        .await
                        .subscription_count(),
                    SubscriptionKind::Event => service.state.event_subscriptions.lock().await.len(),
                    SubscriptionKind::Contract => {
                        service.state.contract_subscriptions.lock().await.len()
                    }
                    SubscriptionKind::Token => service.state.token_subscriptions.lock().await.len(),
                    SubscriptionKind::TokenBalance => {
                        service.state.token_balance_subscriptions.lock().await.len()
                    }
                    SubscriptionKind::TokenTransfer => service
                        .state
                        .token_transfer_subscriptions
                        .lock()
                        .await
                        .len(),
                    SubscriptionKind::Transaction => {
                        service.state.transaction_subscriptions.lock().await.len()
                    }
                };
                if count == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription count timeout");
    }

    fn member_bool_clause(model: &str, member: &str, value: bool) -> types::Clause {
        types::Clause {
            clause_type: Some(ClauseType::Member(types::MemberClause {
                model: model.to_string(),
                member: member.to_string(),
                operator: ComparisonOperator::Eq as i32,
                value: Some(types::MemberValue {
                    value_type: Some(ValueType::Primitive(types::Primitive {
                        primitive_type: Some(types::primitive::PrimitiveType::Bool(value)),
                    })),
                }),
            })),
        }
    }

    #[test]
    fn serializes_u128_as_fixed_width_u128() {
        let input = serde_json::json!(10000_u64);
        let ty = type_to_proto_value(&TypeDef::U128, &input).expect("u128");

        let types::ty::TyType::Primitive(primitive) = ty.ty_type.expect("primitive") else {
            panic!("expected primitive");
        };
        let Some(types::primitive::PrimitiveType::U128(bytes)) = primitive.primitive_type else {
            panic!("expected u128 primitive");
        };

        assert_eq!(bytes.len(), 16);
        assert_eq!(
            u128::from_be_bytes(bytes.try_into().expect("16 bytes")),
            10_000
        );
    }

    #[test]
    fn serializes_eth_address_as_20_bytes() {
        let input = serde_json::json!("0x11223344556677889900aabbccddeeff00112233");
        let ty = type_to_proto_value(&TypeDef::EthAddress, &input).expect("eth address");

        let types::ty::TyType::Primitive(primitive) = ty.ty_type.expect("primitive") else {
            panic!("expected primitive");
        };
        let Some(types::primitive::PrimitiveType::EthAddress(bytes)) = primitive.primitive_type
        else {
            panic!("expected eth_address primitive");
        };

        assert_eq!(bytes.len(), 20);
        assert_eq!(
            hex::encode(bytes),
            "11223344556677889900aabbccddeeff00112233"
        );
    }

    #[test]
    fn serializes_u256_as_fixed_width_u256() {
        let input = serde_json::json!("0x1234");
        let ty = type_to_proto_value(&TypeDef::U256, &input).expect("u256");

        let types::ty::TyType::Primitive(primitive) = ty.ty_type.expect("primitive") else {
            panic!("expected primitive");
        };
        let Some(types::primitive::PrimitiveType::U256(bytes)) = primitive.primitive_type else {
            panic!("expected u256 primitive");
        };

        assert_eq!(bytes.len(), 32);
        assert_eq!(&bytes[30..], &[0x12, 0x34]);
        assert!(bytes[..30].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn parses_scientific_notation_integer_strings() {
        let value = parse_big_int("1.5495265022047578e+21").expect("scientific integer");

        assert_eq!(value.to_string(), "1549526502204757800000");
    }

    #[test]
    fn seen_cache_caps_growth() {
        let mut cache = SeenCache::with_capacity(2);
        cache.extend(["first".to_string(), "second".to_string()]);
        assert!(cache.contains("first"));
        assert!(cache.contains("second"));

        cache.extend(["third".to_string()]);
        assert!(!cache.contains("first"));
        assert!(cache.contains("second"));
        assert!(cache.contains("third"));
    }

    async fn seed_entity(
        service: &EcsService,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        table_name: &str,
        entity_id: Felt,
        open: bool,
    ) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_tables (
                owner BLOB NOT NULL,
                id BLOB NOT NULL,
                name TEXT NOT NULL,
                attributes TEXT NOT NULL,
                keys_json TEXT NOT NULL,
                values_json TEXT NOT NULL,
                legacy INTEGER NOT NULL,
                created_at INTEGER,
                updated_at INTEGER,
                created_block INTEGER,
                updated_block INTEGER,
                created_tx BLOB,
                updated_tx BLOB,
                PRIMARY KEY(owner, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_tables");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_columns (
                owner BLOB NOT NULL,
                table_id BLOB NOT NULL,
                id BLOB NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY(owner, table_id, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_columns");

        let table = CreateTable {
            id: table_id.into(),
            name: table_name.to_string(),
            attributes: vec![],
            primary: PrimaryDef {
                name: "entity_id".to_string(),
                attributes: vec![],
                type_def: PrimaryTypeDef::Felt252,
            },
            columns: vec![ColumnDef {
                id: Felt::from(1_u64).into(),
                name: "open".to_string(),
                attributes: vec![],
                type_def: TypeDef::Bool,
            }],
            append_only: false,
        };

        service.cache_created_table(world_address, &table).await;
        service
            .record_table_kind(world_address, table_id, kind)
            .await
            .expect("record table kind");
        service
            .upsert_entity_meta(kind, world_address, table_id, entity_id, 1, false)
            .await
            .expect("upsert entity meta");

        let row_json = serde_json::json!({
            "entity_id": felt_hex(entity_id),
            "open": open
        })
        .to_string();
        sqlx::query(
            "INSERT INTO torii_ecs_entity_models (
                kind, world_address, table_id, entity_id, row_json, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                row_json = excluded.row_json,
                updated_at = excluded.updated_at",
        )
        .bind(kind.as_str())
        .bind(felt_hex(world_address))
        .bind(felt_hex(table_id))
        .bind(felt_hex(entity_id))
        .bind(row_json)
        .bind(1_i64)
        .execute(&service.state.pool)
        .await
        .expect("insert entity model");
    }

    async fn seed_model_with_bool(
        service: &EcsService,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        table_name: &str,
        entity_id: Felt,
        field_name: &str,
        field_value: bool,
    ) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_tables (
                owner BLOB NOT NULL,
                id BLOB NOT NULL,
                name TEXT NOT NULL,
                attributes TEXT NOT NULL,
                keys_json TEXT NOT NULL,
                values_json TEXT NOT NULL,
                legacy INTEGER NOT NULL,
                created_at INTEGER,
                updated_at INTEGER,
                created_block INTEGER,
                updated_block INTEGER,
                created_tx BLOB,
                updated_tx BLOB,
                PRIMARY KEY(owner, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_tables");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_columns (
                owner BLOB NOT NULL,
                table_id BLOB NOT NULL,
                id BLOB NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY(owner, table_id, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_columns");

        let table = CreateTable {
            id: table_id.into(),
            name: table_name.to_string(),
            attributes: vec![],
            primary: PrimaryDef {
                name: "entity_id".to_string(),
                attributes: vec![],
                type_def: PrimaryTypeDef::Felt252,
            },
            columns: vec![ColumnDef {
                id: Felt::from(1_u64).into(),
                name: field_name.to_string(),
                attributes: vec![],
                type_def: TypeDef::Bool,
            }],
            append_only: false,
        };

        service.cache_created_table(world_address, &table).await;
        service
            .record_table_kind(world_address, table_id, kind)
            .await
            .expect("record table kind");
        service
            .upsert_entity_meta(kind, world_address, table_id, entity_id, 1, false)
            .await
            .expect("upsert entity meta");

        let row_json = serde_json::json!({
            "entity_id": felt_hex(entity_id),
            field_name: field_value
        })
        .to_string();
        sqlx::query(
            "INSERT INTO torii_ecs_entity_models (
                kind, world_address, table_id, entity_id, row_json, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                row_json = excluded.row_json,
                updated_at = excluded.updated_at",
        )
        .bind(kind.as_str())
        .bind(felt_hex(world_address))
        .bind(felt_hex(table_id))
        .bind(felt_hex(entity_id))
        .bind(row_json)
        .bind(1_i64)
        .execute(&service.state.pool)
        .await
        .expect("insert entity model");
    }

    fn keyed_member(name: &str, value: Felt) -> types::Member {
        types::Member {
            name: name.to_string(),
            key: true,
            ty: Some(types::Ty {
                ty_type: Some(types::ty::TyType::Primitive(types::Primitive {
                    primitive_type: Some(types::primitive::PrimitiveType::Felt252(value.into())),
                })),
            }),
        }
    }

    async fn seed_entity_with_custom_keys(
        service: &EcsService,
        kind: TableKind,
        world_address: Felt,
        table_id: Felt,
        table_name: &str,
        entity_id: Felt,
        key_field_name: &str,
        key_value: Felt,
    ) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_tables (
                owner BLOB NOT NULL,
                id BLOB NOT NULL,
                name TEXT NOT NULL,
                attributes TEXT NOT NULL,
                keys_json TEXT NOT NULL,
                values_json TEXT NOT NULL,
                legacy INTEGER NOT NULL,
                created_at INTEGER,
                updated_at INTEGER,
                created_block INTEGER,
                updated_block INTEGER,
                created_tx BLOB,
                updated_tx BLOB,
                PRIMARY KEY(owner, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_tables");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dojo_columns (
                owner BLOB NOT NULL,
                table_id BLOB NOT NULL,
                id BLOB NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY(owner, table_id, id)
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create dojo_columns");

        let table = CreateTable {
            id: table_id.into(),
            name: table_name.to_string(),
            attributes: vec![],
            primary: PrimaryDef {
                name: "entity_id".to_string(),
                attributes: vec![],
                type_def: PrimaryTypeDef::Felt252,
            },
            columns: vec![ColumnDef {
                id: Felt::from(1_u64).into(),
                name: key_field_name.to_string(),
                attributes: vec![Attribute::new_empty("key".to_string())],
                type_def: TypeDef::Felt252,
            }],
            append_only: false,
        };

        service.cache_created_table(world_address, &table).await;
        service
            .record_table_kind(world_address, table_id, kind)
            .await
            .expect("record table kind");
        service
            .upsert_entity_meta(kind, world_address, table_id, entity_id, 1, false)
            .await
            .expect("upsert entity meta");

        let row_json = serde_json::json!({
            "entity_id": felt_hex(entity_id),
            key_field_name: felt_hex(key_value)
        })
        .to_string();
        sqlx::query(
            "INSERT INTO torii_ecs_entity_models (
                kind, world_address, table_id, entity_id, row_json, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(kind, world_address, table_id, entity_id) DO UPDATE SET
                row_json = excluded.row_json,
                updated_at = excluded.updated_at",
        )
        .bind(kind.as_str())
        .bind(felt_hex(world_address))
        .bind(felt_hex(table_id))
        .bind(felt_hex(entity_id))
        .bind(row_json)
        .bind(1_i64)
        .execute(&service.state.pool)
        .await
        .expect("insert entity model");
    }

    #[tokio::test]
    async fn subscribe_entities_update_flow() {
        let db_path = test_db_path("entities-sub");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(10_u64);
        let table = Felt::from(20_u64);
        let entity = Felt::from(30_u64);
        seed_entity(
            &service,
            TableKind::Entity,
            world,
            table,
            "test-Lobby",
            entity,
            true,
        )
        .await;

        let response = service
            .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                clause: Some(member_bool_clause("test-Lobby", "open", true)),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe entities");
        let mut stream = response.into_inner();

        let setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        let subscription_id = setup.subscription_id;
        assert!(setup.entity.is_none());

        service
            .publish_entity_update(TableKind::Entity, world, entity)
            .await
            .expect("publish entity update");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("entity timeout")
            .expect("entity frame")
            .expect("entity ok");
        assert_eq!(matched.subscription_id, subscription_id);
        let matched_entity = matched.entity.expect("entity payload");
        assert_eq!(matched_entity.world_address, Vec::<u8>::from(world));

        service
            .update_entities_subscription(Request::new(UpdateEntitiesSubscriptionRequest {
                subscription_id,
                clause: Some(member_bool_clause("test-Lobby", "open", false)),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("update entities subscription");

        service
            .publish_entity_update(TableKind::Entity, world, entity)
            .await
            .expect("publish filtered entity update");
        let filtered = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(filtered.is_err(), "entity should be filtered out");
    }

    #[tokio::test]
    async fn subscribe_entities_drop_cleans_up_subscription() {
        let db_path = test_db_path("entities-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(42_u64);

        let response = service
            .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                clause: None,
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe entities");
        let mut stream = response.into_inner();

        let _setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        wait_for_entity_subscription_count(&service, TableKind::Entity, 1).await;

        drop(stream);

        wait_for_entity_subscription_count(&service, TableKind::Entity, 0).await;
    }

    #[tokio::test]
    async fn subscribe_entities_repeated_drop_keeps_registry_flat() {
        let db_path = test_db_path("entities-drop-loop");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(43_u64);

        for _ in 0..5 {
            let response = service
                .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                    clause: None,
                    world_addresses: vec![world.into()],
                }))
                .await
                .expect("subscribe entities");
            let mut stream = response.into_inner();

            let _setup = timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("setup timeout")
                .expect("setup frame")
                .expect("setup ok");
            wait_for_entity_subscription_count(&service, TableKind::Entity, 1).await;

            drop(stream);

            wait_for_entity_subscription_count(&service, TableKind::Entity, 0).await;
        }
    }

    #[tokio::test]
    async fn subscribe_entities_identical_filters_share_group() {
        let db_path = test_db_path("entities-shared-group");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(44_u64);
        let clause = member_bool_clause("test-Lobby", "open", true);

        let response_a = service
            .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                clause: Some(clause.clone()),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe entities a");
        let response_b = service
            .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                clause: Some(clause),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe entities b");

        let mut stream_a = response_a.into_inner();
        let mut stream_b = response_b.into_inner();

        let _setup_a = timeout(Duration::from_secs(1), stream_a.next())
            .await
            .expect("setup timeout a")
            .expect("setup frame a")
            .expect("setup ok a");
        let _setup_b = timeout(Duration::from_secs(1), stream_b.next())
            .await
            .expect("setup timeout b")
            .expect("setup frame b")
            .expect("setup ok b");

        wait_for_entity_subscription_count(&service, TableKind::Entity, 2).await;
        wait_for_entity_subscription_group_count(&service, TableKind::Entity, 1).await;

        drop(stream_a);
        wait_for_entity_subscription_count(&service, TableKind::Entity, 1).await;
        wait_for_entity_subscription_group_count(&service, TableKind::Entity, 1).await;

        drop(stream_b);
        wait_for_entity_subscription_count(&service, TableKind::Entity, 0).await;
        wait_for_entity_subscription_group_count(&service, TableKind::Entity, 0).await;
    }

    #[tokio::test]
    async fn update_entities_subscription_moves_between_groups() {
        let db_path = test_db_path("entities-update-groups");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(45_u64);

        let response_a = service
            .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                clause: Some(member_bool_clause("test-Lobby", "open", true)),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe entities a");
        let response_b = service
            .subscribe_entities(Request::new(SubscribeEntitiesRequest {
                clause: Some(member_bool_clause("test-Lobby", "open", true)),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe entities b");

        let mut stream_a = response_a.into_inner();
        let mut stream_b = response_b.into_inner();
        let setup_a = timeout(Duration::from_secs(1), stream_a.next())
            .await
            .expect("setup timeout a")
            .expect("setup frame a")
            .expect("setup ok a");
        let _setup_b = timeout(Duration::from_secs(1), stream_b.next())
            .await
            .expect("setup timeout b")
            .expect("setup frame b")
            .expect("setup ok b");

        wait_for_entity_subscription_group_count(&service, TableKind::Entity, 1).await;

        service
            .update_entities_subscription(Request::new(UpdateEntitiesSubscriptionRequest {
                subscription_id: setup_a.subscription_id,
                clause: Some(member_bool_clause("test-Lobby", "open", false)),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("update entities subscription");

        wait_for_entity_subscription_count(&service, TableKind::Entity, 2).await;
        wait_for_entity_subscription_group_count(&service, TableKind::Entity, 2).await;

        drop(stream_a);
        drop(stream_b);
        wait_for_entity_subscription_count(&service, TableKind::Entity, 0).await;
        wait_for_entity_subscription_group_count(&service, TableKind::Entity, 0).await;
    }

    #[tokio::test]
    async fn subscribe_event_messages_update_flow() {
        let db_path = test_db_path("event-messages-sub");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(101_u64);
        let table = Felt::from(201_u64);
        let entity = Felt::from(301_u64);
        seed_entity(
            &service,
            TableKind::EventMessage,
            world,
            table,
            "test-EventMessage",
            entity,
            true,
        )
        .await;

        let response = service
            .subscribe_event_messages(Request::new(SubscribeEntitiesRequest {
                clause: Some(member_bool_clause("test-EventMessage", "open", true)),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe event messages");
        let mut stream = response.into_inner();

        let setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        let subscription_id = setup.subscription_id;
        assert!(setup.entity.is_none());

        service
            .publish_entity_update(TableKind::EventMessage, world, entity)
            .await
            .expect("publish event message update");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("event message timeout")
            .expect("event message frame")
            .expect("event message ok");
        assert_eq!(matched.subscription_id, subscription_id);
        assert!(matched.entity.is_some());

        service
            .update_event_messages_subscription(Request::new(UpdateEntitiesSubscriptionRequest {
                subscription_id,
                clause: Some(member_bool_clause("test-EventMessage", "open", false)),
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("update event messages subscription");

        service
            .publish_entity_update(TableKind::EventMessage, world, entity)
            .await
            .expect("publish filtered event message update");
        let filtered = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(filtered.is_err(), "event message should be filtered out");
    }

    #[tokio::test]
    async fn subscribe_event_messages_drop_cleans_up_subscription() {
        let db_path = test_db_path("event-messages-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(44_u64);

        let response = service
            .subscribe_event_messages(Request::new(SubscribeEntitiesRequest {
                clause: None,
                world_addresses: vec![world.into()],
            }))
            .await
            .expect("subscribe event messages");
        let mut stream = response.into_inner();

        let _setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        wait_for_entity_subscription_count(&service, TableKind::EventMessage, 1).await;

        drop(stream);

        wait_for_entity_subscription_count(&service, TableKind::EventMessage, 0).await;
    }

    #[tokio::test]
    async fn subscribe_events_keys_filter() {
        let db_path = test_db_path("events-sub");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(1_u64);
        let tx1 = Felt::from(2_u64);
        let tx2 = Felt::from(3_u64);
        let key_match = Felt::from(444_u64);

        let response = service
            .subscribe_events(Request::new(SubscribeEventsRequest {
                keys: vec![types::KeysClause {
                    keys: vec![key_match.into()],
                    pattern_matching: PatternMatching::VariableLen as i32,
                    models: vec![],
                }],
            }))
            .await
            .expect("subscribe events");
        let mut stream = response.into_inner();

        let setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        assert!(setup.event.is_none());

        service
            .store_event(
                world,
                tx1,
                1,
                1,
                &[Felt::from(999_u64)],
                &[Felt::from(7_u64)],
                0,
            )
            .await
            .expect("store non matching event");
        let no_match = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(
            no_match.is_err(),
            "non matching event should be filtered out"
        );

        service
            .store_event(world, tx2, 1, 1, &[key_match, Felt::from(123_u64)], &[], 1)
            .await
            .expect("store matching event");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("matched timeout")
            .expect("matched frame")
            .expect("matched ok");
        let event = matched.event.expect("event payload");
        assert_eq!(event.transaction_hash, Vec::<u8>::from(tx2));
        assert_eq!(
            event.keys.first().cloned(),
            Some(Vec::<u8>::from(key_match))
        );
    }

    #[tokio::test]
    async fn subscribe_events_drop_cleans_up_subscription() {
        let db_path = test_db_path("events-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");

        let response = service
            .subscribe_events(Request::new(SubscribeEventsRequest { keys: vec![] }))
            .await
            .expect("subscribe events");
        let mut stream = response.into_inner();

        let _setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        wait_for_subscription_count(&service, SubscriptionKind::Event, 1).await;

        drop(stream);

        wait_for_subscription_count(&service, SubscriptionKind::Event, 0).await;
    }

    #[tokio::test]
    async fn subscribe_contracts_query_filter() {
        let db_path = test_db_path("contracts-sub");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world_contract = Felt::from(1111_u64);
        let other_contract = Felt::from(2222_u64);

        let response = service
            .subscribe_contracts(Request::new(SubscribeContractsRequest {
                query: Some(types::ContractQuery {
                    contract_addresses: vec![world_contract.into()],
                    contract_types: vec![ContractType::World as i32],
                }),
            }))
            .await
            .expect("subscribe contracts");
        let mut stream = response.into_inner();

        service
            .record_contract_progress(other_contract, ContractType::World, 7, 77, None)
            .await
            .expect("record other contract progress");
        let no_match = timeout(Duration::from_millis(150), stream.next()).await;
        assert!(
            no_match.is_err(),
            "non matching contract should be filtered out"
        );

        service
            .record_contract_progress(world_contract, ContractType::World, 9, 99, None)
            .await
            .expect("record world contract progress");
        let matched = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("matched timeout")
            .expect("matched frame")
            .expect("matched ok");
        let contract = matched.contract.expect("contract payload");
        assert_eq!(contract.contract_address, Vec::<u8>::from(world_contract));
        assert_eq!(contract.contract_type, ContractType::World as i32);
    }

    #[tokio::test]
    async fn subscribe_contracts_drop_cleans_up_subscription() {
        let db_path = test_db_path("contracts-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");

        let response = service
            .subscribe_contracts(Request::new(SubscribeContractsRequest {
                query: Some(types::ContractQuery {
                    contract_addresses: vec![],
                    contract_types: vec![],
                }),
            }))
            .await
            .expect("subscribe contracts");
        let stream = response.into_inner();

        wait_for_subscription_count(&service, SubscriptionKind::Contract, 1).await;

        drop(stream);

        wait_for_subscription_count(&service, SubscriptionKind::Contract, 0).await;
    }

    #[tokio::test]
    async fn subscribe_transactions_drop_cleans_up_subscription() {
        let db_path = test_db_path("transactions-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");

        let response = service
            .subscribe_transactions(Request::new(SubscribeTransactionsRequest { filter: None }))
            .await
            .expect("subscribe transactions");
        let mut stream = response.into_inner();

        let _setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        wait_for_subscription_count(&service, SubscriptionKind::Transaction, 1).await;

        drop(stream);

        wait_for_subscription_count(&service, SubscriptionKind::Transaction, 0).await;
    }

    #[tokio::test]
    async fn subscribe_tokens_drop_cleans_up_subscription() {
        let db_path = test_db_path("tokens-drop-cleanup");
        let erc20_url = temp_sqlite_url("erc20-tokens-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), Some(&erc20_url), None, None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        sqlx::query(
            "CREATE TABLE erc20.token_metadata (
                token BLOB PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                decimals TEXT,
                total_supply BLOB
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create erc20 token metadata");

        let response = service
            .subscribe_tokens(Request::new(SubscribeTokensRequest {
                contract_addresses: vec![],
                token_ids: vec![],
            }))
            .await
            .expect("subscribe tokens");
        let mut stream = response.into_inner();

        let _setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        wait_for_subscription_count(&service, SubscriptionKind::Token, 1).await;

        drop(stream);

        wait_for_subscription_count(&service, SubscriptionKind::Token, 0).await;
    }

    #[tokio::test]
    async fn subscribe_token_balances_emits_updates_for_existing_balance_rows() {
        let db_path = test_db_path("token-balances-live-updates");
        let erc20_url = temp_sqlite_url("erc20-token-balances-live-updates");
        let service = EcsService::new(&db_path, Some(1), Some(&erc20_url), None, None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        sqlx::query(
            "CREATE TABLE erc20.balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL,
                last_tx_hash BLOB NOT NULL
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create erc20 balances");

        let token: Vec<u8> = Felt::from(0x99_u64).into();
        let wallet: Vec<u8> = Felt::from(0x55_u64).into();
        let original_balance = u256_bytes_from_u64(1);
        let updated_balance = u256_bytes_from_u64(5);

        sqlx::query(
            "INSERT INTO erc20.balances (token, wallet, balance, last_block, last_tx_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&token)
        .bind(&wallet)
        .bind(&original_balance)
        .bind("1")
        .bind(vec![0_u8; 32])
        .execute(&service.state.pool)
        .await
        .expect("insert erc20 balance");

        let response = service
            .subscribe_token_balances(Request::new(SubscribeTokenBalancesRequest {
                account_addresses: vec![wallet.clone()],
                contract_addresses: vec![token.clone()],
                token_ids: vec![],
            }))
            .await
            .expect("subscribe token balances");
        let mut stream = response.into_inner();

        let setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        let subscription_id = setup.subscription_id;
        assert!(setup.balance.is_none());

        sqlx::query(
            "UPDATE erc20.balances
             SET balance = ?1, last_block = ?2, last_tx_hash = ?3
             WHERE token = ?4 AND wallet = ?5",
        )
        .bind(&updated_balance)
        .bind("2")
        .bind(vec![1_u8; 32])
        .bind(&token)
        .bind(&wallet)
        .execute(&service.state.pool)
        .await
        .expect("update erc20 balance");

        let first_update = timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("first update timeout")
            .expect("first update frame")
            .expect("first update ok");
        assert_eq!(first_update.subscription_id, subscription_id);
        assert_eq!(
            first_update.balance.expect("first balance payload").balance,
            updated_balance
        );

        sqlx::query(
            "UPDATE erc20.balances
             SET balance = ?1, last_block = ?2, last_tx_hash = ?3
             WHERE token = ?4 AND wallet = ?5",
        )
        .bind(&original_balance)
        .bind("3")
        .bind(vec![2_u8; 32])
        .bind(&token)
        .bind(&wallet)
        .execute(&service.state.pool)
        .await
        .expect("restore erc20 balance");

        let second_update = timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("second update timeout")
            .expect("second update frame")
            .expect("second update ok");
        assert_eq!(second_update.subscription_id, subscription_id);
        assert_eq!(
            second_update
                .balance
                .expect("second balance payload")
                .balance,
            original_balance
        );
    }

    #[tokio::test]
    async fn subscribe_token_balances_drop_cleans_up_subscription() {
        let db_path = test_db_path("token-balances-drop-cleanup");
        let erc20_url = temp_sqlite_url("erc20-token-balances-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), Some(&erc20_url), None, None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        sqlx::query(
            "CREATE TABLE erc20.balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL,
                last_tx_hash BLOB NOT NULL
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create erc20 balances");

        let response = service
            .subscribe_token_balances(Request::new(SubscribeTokenBalancesRequest {
                account_addresses: vec![],
                contract_addresses: vec![],
                token_ids: vec![],
            }))
            .await
            .expect("subscribe token balances");
        let mut stream = response.into_inner();

        let _setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        wait_for_subscription_count(&service, SubscriptionKind::TokenBalance, 1).await;

        drop(stream);

        wait_for_subscription_count(&service, SubscriptionKind::TokenBalance, 0).await;
    }

    #[tokio::test]
    async fn subscribe_token_transfers_drop_cleans_up_subscription() {
        let db_path = test_db_path("token-transfers-drop-cleanup");
        let erc20_url = temp_sqlite_url("erc20-token-transfers-drop-cleanup");
        let service = EcsService::new(&db_path, Some(1), Some(&erc20_url), None, None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        sqlx::query(
            "CREATE TABLE erc20.transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                amount BLOB NOT NULL,
                timestamp TEXT
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create erc20 transfers");

        let response = service
            .subscribe_token_transfers(Request::new(SubscribeTokenTransfersRequest {
                account_addresses: vec![],
                contract_addresses: vec![],
                token_ids: vec![],
            }))
            .await
            .expect("subscribe token transfers");
        let mut stream = response.into_inner();

        let _setup = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("setup timeout")
            .expect("setup frame")
            .expect("setup ok");
        wait_for_subscription_count(&service, SubscriptionKind::TokenTransfer, 1).await;

        drop(stream);

        wait_for_subscription_count(&service, SubscriptionKind::TokenTransfer, 0).await;
    }

    #[test]
    fn entity_keys_clause_scopes_key_extraction_to_requested_models() {
        let entity = types::Entity {
            hashed_keys: vec![],
            models: vec![
                types::Struct {
                    name: "NUMS-Other".to_string(),
                    children: vec![keyed_member("other_id", Felt::from(0x111_u64))],
                },
                types::Struct {
                    name: "NUMS-Config".to_string(),
                    children: vec![keyed_member("config_id", Felt::from(0x222_u64))],
                },
            ],
            created_at: 0,
            updated_at: 0,
            executed_at: 0,
            world_address: vec![],
        };
        let clause = types::Clause {
            clause_type: Some(ClauseType::Keys(types::KeysClause {
                models: vec!["NUMS-Config".to_string()],
                pattern_matching: PatternMatching::VariableLen as i32,
                keys: vec![Felt::from(0x222_u64).into()],
            })),
        };

        assert!(entity_matches_clause(&entity, &clause));
    }

    #[tokio::test]
    async fn retrieve_entities_without_filters_uses_bounded_query() {
        let db_path = test_db_path("retrieve-entities");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(900_u64);

        seed_entity(
            &service,
            TableKind::Entity,
            world,
            Felt::from(901_u64),
            "test-ModelA",
            Felt::from(1001_u64),
            true,
        )
        .await;
        seed_entity(
            &service,
            TableKind::Entity,
            world,
            Felt::from(902_u64),
            "test-ModelB",
            Felt::from(1001_u64),
            true,
        )
        .await;

        let response = service
            .retrieve_entities(Request::new(RetrieveEntitiesRequest {
                query: Some(types::Query {
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                    world_addresses: vec![],
                    models: vec![],
                    clause: None,
                    no_hashed_keys: false,
                    historical: false,
                }),
            }))
            .await
            .expect("retrieve entities")
            .into_inner();

        assert_eq!(response.entities.len(), 1);
        assert!(!response.next_cursor.is_empty() || response.entities.len() < 10);
    }

    #[tokio::test]
    async fn retrieve_entities_includes_co_located_models_for_matching_entity() {
        let db_path = test_db_path("retrieve-entities-colocated");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(910_u64);
        let entity = Felt::from(1010_u64);

        seed_model_with_bool(
            &service,
            TableKind::Entity,
            world,
            Felt::from(911_u64),
            "NUMS-QuestDefinition",
            entity,
            "enabled",
            true,
        )
        .await;
        seed_model_with_bool(
            &service,
            TableKind::Entity,
            world,
            Felt::from(912_u64),
            "NUMS-QuestAssociation",
            entity,
            "linked",
            true,
        )
        .await;

        let response = service
            .retrieve_entities(Request::new(RetrieveEntitiesRequest {
                query: Some(types::Query {
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                    world_addresses: vec![world.into()],
                    models: vec![],
                    clause: Some(member_bool_clause("NUMS-QuestDefinition", "enabled", true)),
                    no_hashed_keys: false,
                    historical: false,
                }),
            }))
            .await
            .expect("retrieve entities")
            .into_inner();

        assert_eq!(response.entities.len(), 1);
        let model_names = response.entities[0]
            .models
            .iter()
            .map(|model| model.name.clone())
            .collect::<HashSet<_>>();
        assert_eq!(model_names.len(), 2);
        assert!(model_names.contains("NUMS-QuestDefinition"));
        assert!(model_names.contains("NUMS-QuestAssociation"));
    }

    #[tokio::test]
    async fn retrieve_event_messages_includes_co_located_models_for_matching_entity() {
        let db_path = test_db_path("retrieve-event-messages-colocated");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(910_u64);
        let entity = Felt::from(10_110_u64);

        seed_model_with_bool(
            &service,
            TableKind::EventMessage,
            world,
            Felt::from(9_111_u64),
            "NUMS-QuestDefinition",
            entity,
            "enabled",
            true,
        )
        .await;
        seed_model_with_bool(
            &service,
            TableKind::EventMessage,
            world,
            Felt::from(9_112_u64),
            "NUMS-QuestAssociation",
            entity,
            "linked",
            true,
        )
        .await;

        let response = service
            .retrieve_event_messages(Request::new(RetrieveEntitiesRequest {
                query: Some(types::Query {
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                    world_addresses: vec![world.into()],
                    models: vec![],
                    clause: Some(member_bool_clause("NUMS-QuestDefinition", "enabled", true)),
                    no_hashed_keys: false,
                    historical: false,
                }),
            }))
            .await
            .expect("retrieve event messages")
            .into_inner();

        assert_eq!(response.entities.len(), 1);
        let model_names = response.entities[0]
            .models
            .iter()
            .map(|model| model.name.clone())
            .collect::<HashSet<_>>();
        assert_eq!(model_names.len(), 2);
        assert!(model_names.contains("NUMS-QuestDefinition"));
        assert!(model_names.contains("NUMS-QuestAssociation"));
    }

    #[tokio::test]
    async fn retrieve_entities_keys_clause_fills_first_page_with_matches() {
        let db_path = test_db_path("retrieve-entities-keys-page");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let world = Felt::from(920_u64);
        let matching_key = Felt::from(0xdead_u64);

        for index in 0..100_u64 {
            let entity_id = Felt::from(index + 1);
            seed_entity_with_custom_keys(
                &service,
                TableKind::Entity,
                world,
                Felt::from(index + 1_000),
                "NUMS-Config",
                entity_id,
                "config_id",
                Felt::from(0x100_u64 + index),
            )
            .await;
        }

        let matching_entity = Felt::from(10_000_u64);
        seed_entity_with_custom_keys(
            &service,
            TableKind::Entity,
            world,
            Felt::from(9_999_u64),
            "NUMS-Config",
            matching_entity,
            "config_id",
            matching_key,
        )
        .await;

        let response = service
            .retrieve_entities(Request::new(RetrieveEntitiesRequest {
                query: Some(types::Query {
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 100,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                    world_addresses: vec![world.into()],
                    models: vec![],
                    clause: Some(types::Clause {
                        clause_type: Some(ClauseType::Keys(types::KeysClause {
                            models: vec!["NUMS-Config".to_string()],
                            pattern_matching: PatternMatching::VariableLen as i32,
                            keys: vec![matching_key.into()],
                        })),
                    }),
                    no_hashed_keys: false,
                    historical: false,
                }),
            }))
            .await
            .expect("retrieve entities")
            .into_inner();

        assert_eq!(response.entities.len(), 1);
        assert_eq!(
            response.entities[0].hashed_keys,
            Vec::<u8>::from(matching_entity)
        );
        assert!(response.next_cursor.is_empty());
    }

    #[tokio::test]
    async fn execute_sql_exposes_legacy_compat_views() {
        let db_path = test_db_path("sql-compat");
        let erc20_url = temp_sqlite_url("erc20");
        let erc721_url = temp_sqlite_url("erc721");
        let service = EcsService::new(&db_path, Some(1), Some(&erc20_url), Some(&erc721_url), None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        sqlx::query(
            r#"CREATE TABLE "NUMS-LeaderboardScore" (
                entity_id TEXT PRIMARY KEY,
                leaderboard_id TEXT,
                game_id TEXT,
                player TEXT,
                score TEXT,
                timestamp TEXT
            )"#,
        )
        .execute(&service.state.pool)
        .await
        .expect("create leaderboard table");
        sqlx::query(
            r#"CREATE TABLE "NUMS-Claimed" (
                entity_id TEXT PRIMARY KEY,
                player_id TEXT,
                game_id TEXT,
                reward TEXT,
                time TEXT
            )"#,
        )
        .execute(&service.state.pool)
        .await
        .expect("create claimed table");
        sqlx::query(
            r#"CREATE TABLE "NUMS-BundleIssuance" (
                entity_id TEXT PRIMARY KEY,
                bundle_id INTEGER,
                recipient TEXT,
                issued_at TEXT
            )"#,
        )
        .execute(&service.state.pool)
        .await
        .expect("create bundle issuance table");

        let player = "0x008b95a26e1392ed9e817607bfae2dd93efb9c66ee7db0b018091a11d9037006";
        sqlx::query(r#"INSERT INTO "NUMS-LeaderboardScore" (entity_id, leaderboard_id, game_id, player, score, timestamp)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#)
            .bind("score-1")
            .bind("0x1")
            .bind("0x2")
            .bind(player)
            .bind("0xb")
            .bind("0x69c2703d")
            .execute(&service.state.pool)
            .await
            .expect("insert leaderboard row");
        sqlx::query(
            r#"INSERT INTO "NUMS-Claimed" (entity_id, player_id, game_id, reward, time)
                       VALUES (?1, ?2, ?3, ?4, ?5)"#,
        )
        .bind("claim-1")
        .bind(player)
        .bind("0x2")
        .bind("0x8c")
        .bind("0x69c2703d")
        .execute(&service.state.pool)
        .await
        .expect("insert claimed row");
        sqlx::query(
            r#"INSERT INTO "NUMS-BundleIssuance" (entity_id, bundle_id, recipient, issued_at)
                       VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind("issuance-1")
        .bind(1_i64)
        .bind(player)
        .bind("0x69c2dbf7")
        .execute(&service.state.pool)
        .await
        .expect("insert issuance row");

        let world =
            Felt::from_hex("0x048d9413e93af3644407a952ba99596310cb285575819aed9251fe9f45883be2")
                .expect("world");
        let table =
            Felt::from_hex("0x027fb20c50c1bc8220c8d7643d495f921c67c7c69ffe3cb6b5d5a81dd1564fd7")
                .expect("table");
        let entity = Felt::from(12345_u64);
        service
            .record_table_kind(world, table, TableKind::EventMessage)
            .await
            .expect("record table kind");
        service
            .upsert_entity_meta(
                TableKind::EventMessage,
                world,
                table,
                entity,
                1_774_351_127,
                false,
            )
            .await
            .expect("upsert event meta");
        let row_json = serde_json::json!({
            "amount": "0x1e3660",
            "bundle_id": 1,
            "payment_token": "0x0037d4aef94ac5e9b2d49eb763bfa2db76f3f82e31506a78ef8458dc458e820b",
            "recipient": player,
            "referrer": { "Some": player }
        })
        .to_string();
        sqlx::query(
            "INSERT INTO torii_ecs_entity_models (
                kind, world_address, table_id, entity_id, row_json, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(TableKind::EventMessage.as_str())
        .bind(felt_hex(world))
        .bind(felt_hex(table))
        .bind(felt_hex(entity))
        .bind(row_json)
        .bind(1_774_351_127_i64)
        .execute(&service.state.pool)
        .await
        .expect("insert event model");

        sqlx::query(
            "CREATE TABLE erc20.balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL,
                last_tx_hash BLOB NOT NULL
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create erc20 balances");
        sqlx::query(
            "INSERT INTO erc20.balances (token, wallet, balance, last_block, last_tx_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(Vec::<u8>::from(Felt::from(0x99_u64)))
        .bind(Vec::<u8>::from(Felt::from(0x55_u64)))
        .bind(vec![1_u8])
        .bind("1")
        .bind(vec![0_u8; 32])
        .execute(&service.state.pool)
        .await
        .expect("insert erc20 balance");

        sqlx::query(
            "CREATE TABLE erc721.nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL,
                block_number TEXT NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp TEXT NOT NULL
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create erc721 ownership");
        sqlx::query(
            "INSERT INTO erc721.nft_ownership (token, token_id, owner, block_number, tx_hash, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(Vec::<u8>::from(Felt::from(0x1137_u64)))
        .bind(vec![0x04_u8])
        .bind(Vec::<u8>::from(Felt::from(0x25145_u64)))
        .bind("1")
        .bind(vec![1_u8; 32])
        .bind("1774350232")
        .execute(&service.state.pool)
        .await
        .expect("insert erc721 ownership");

        sqlx::query(
            "CREATE TABLE controllers (
                id TEXT PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL,
                deployed_at TEXT NOT NULL,
                updated_at BIGINT NOT NULL
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create controllers backing table");
        sqlx::query(
            "INSERT INTO controllers (id, address, username, deployed_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(player)
        .bind(player)
        .bind("cartridge_user")
        .bind("2024-03-20T12:00:00Z")
        .bind(1_710_936_000_i64)
        .execute(&service.state.pool)
        .await
        .expect("insert controller backing row");

        let controllers = service
            .execute_sql(Request::new(types::SqlQueryRequest {
                query: "SELECT address, username FROM controllers LIMIT 5".to_string(),
            }))
            .await
            .expect("controllers query")
            .into_inner();
        let controller_rows = sql_rows_to_maps(&controllers.rows);
        assert!(!controller_rows.is_empty());
        assert_eq!(controller_rows[0].get("address"), Some(&player.to_string()));
        assert_eq!(
            controller_rows[0].get("username"),
            Some(&"cartridge_user".to_string())
        );

        let historical = service
            .execute_sql(Request::new(types::SqlQueryRequest {
                query: "SELECT model_id, data FROM event_messages_historical LIMIT 5".to_string(),
            }))
            .await
            .expect("historical query")
            .into_inner();
        let historical_rows = sql_rows_to_maps(&historical.rows);
        assert!(!historical_rows.is_empty());
        assert_eq!(
            historical_rows[0].get("model_id"),
            Some(&format!("{}:{}", felt_hex(world), felt_hex(table)))
        );
        assert!(historical_rows[0]
            .get("data")
            .expect("data column")
            .contains("\"recipient\""));

        let token_balances = service
            .execute_sql(Request::new(types::SqlQueryRequest {
                query: "SELECT account_address, contract_address, token_id, balance FROM token_balances ORDER BY account_address, contract_address LIMIT 10".to_string(),
            }))
            .await
            .expect("token balances query")
            .into_inner();
        let token_rows = sql_rows_to_maps(&token_balances.rows);
        assert!(token_rows.len() >= 2);
        assert!(token_rows
            .iter()
            .any(|row| row.get("balance").is_some_and(|v| v.ends_with('1'))));
    }

    #[tokio::test]
    async fn retrieve_token_balances_uses_initialized_second_sqlite_connection() {
        let db_path = test_db_path("token-balances-second-conn");
        let erc20_url = temp_sqlite_url("erc20-second-conn");
        let erc721_url = temp_sqlite_url("erc721-second-conn");
        let service = EcsService::new(&db_path, Some(2), Some(&erc20_url), Some(&erc721_url), None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        let mut setup_conn = service
            .acquire_initialized_connection()
            .await
            .expect("setup connection");
        sqlx::query(
            "CREATE TABLE erc20.balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL,
                last_tx_hash BLOB NOT NULL
            )",
        )
        .execute(&mut *setup_conn)
        .await
        .expect("create erc20 balances");
        sqlx::query(
            "INSERT INTO erc20.balances (token, wallet, balance, last_block, last_tx_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(Felt::from(0x99_u64).as_be_bytes_slice())
        .bind(Felt::from(0x55_u64).as_be_bytes_slice())
        .bind(vec![1_u8])
        .bind("1")
        .bind(vec![0_u8; 32])
        .execute(&mut *setup_conn)
        .await
        .expect("insert erc20 balance");
        drop(setup_conn);

        let _held_conn = service
            .acquire_initialized_connection()
            .await
            .expect("held connection");

        let response = service
            .retrieve_token_balances(Request::new(RetrieveTokenBalancesRequest {
                query: Some(types::TokenBalanceQuery {
                    account_addresses: vec![],
                    contract_addresses: vec![],
                    token_ids: vec![],
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                }),
            }))
            .await
            .expect("retrieve token balances")
            .into_inner();

        assert_eq!(response.balances.len(), 1);
        assert_eq!(
            response.balances[0].contract_address,
            Felt::from(0x99_u64).to_be_bytes_vec()
        );
        assert_eq!(
            response.balances[0].account_address,
            Felt::from(0x55_u64).to_be_bytes_vec()
        );
        assert_eq!(response.balances[0].balance, u256_bytes_from_u64(1));
        assert_eq!(response.balances[0].token_id, None);
    }

    #[tokio::test]
    async fn retrieve_token_balances_erc721_query_plan_uses_ordered_index() {
        let db_path = test_db_path("token-balances-erc721-plan");
        let erc721_url = temp_sqlite_url("erc721-token-balances-plan");
        let service = EcsService::new(&db_path, Some(1), None, Some(&erc721_url), None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        let mut conn = service
            .acquire_initialized_connection()
            .await
            .expect("setup connection");
        sqlx::query(
            "CREATE TABLE erc721.nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create ownership");
        sqlx::query(
            "CREATE INDEX erc721.idx_nft_ownership_owner_token_token_id_ord
             ON nft_ownership(owner, token, length(token_id), token_id)",
        )
        .execute(&mut *conn)
        .await
        .expect("create ordered ownership index");

        let details = explain_query_plan_details(
            &mut conn,
            "SELECT owner, token, token_id
             FROM erc721.nft_ownership
             WHERE owner IN (x'01') AND token IN (x'02')
             ORDER BY token ASC, owner ASC, length(token_id) ASC, token_id ASC
             LIMIT 10",
        )
        .await;
        let combined = details.join(" | ");

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_nft_ownership_owner_token_token_id_ord")),
            "expected ordered ownership index in plan, got: {combined}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("USE TEMP B-TREE")),
            "expected planner to avoid temp sort, got: {combined}"
        );
    }

    #[tokio::test]
    async fn retrieve_token_balances_erc1155_query_plan_uses_ordered_index() {
        let db_path = test_db_path("token-balances-erc1155-plan");
        let erc1155_url = temp_sqlite_url("erc1155-token-balances-plan");
        let service = EcsService::new(&db_path, Some(1), None, None, Some(&erc1155_url))
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        let mut conn = service
            .acquire_initialized_connection()
            .await
            .expect("setup connection");
        sqlx::query(
            "CREATE TABLE erc1155.erc1155_balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract BLOB NOT NULL,
                wallet BLOB NOT NULL,
                token_id BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create erc1155 balances");
        sqlx::query(
            "CREATE INDEX erc1155.idx_erc1155_balances_wallet_contract_token_id_ord
             ON erc1155_balances(wallet, contract, length(token_id), token_id)",
        )
        .execute(&mut *conn)
        .await
        .expect("create ordered erc1155 index");

        let details = explain_query_plan_details(
            &mut conn,
            "SELECT wallet, contract, token_id, balance
             FROM erc1155.erc1155_balances
             WHERE wallet IN (x'01') AND contract IN (x'02')
             ORDER BY contract ASC, wallet ASC, length(token_id) ASC, token_id ASC
             LIMIT 10",
        )
        .await;
        let combined = details.join(" | ");

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_erc1155_balances_wallet_contract_token_id_ord")),
            "expected ordered erc1155 index in plan, got: {combined}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("USE TEMP B-TREE")),
            "expected planner to avoid temp sort, got: {combined}"
        );
    }

    #[tokio::test]
    async fn retrieve_token_contracts_normalizes_total_supply_to_u256_width() {
        let db_path = test_db_path("token-contracts-encoding");
        let erc20_url = temp_sqlite_url("erc20-token-contracts-encoding");
        let service = EcsService::new(&db_path, Some(1), Some(&erc20_url), None, None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        let mut conn = service
            .acquire_initialized_connection()
            .await
            .expect("setup connection");
        sqlx::query(
            "CREATE TABLE erc20.token_metadata (
                token BLOB PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                decimals TEXT,
                total_supply BLOB
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create token metadata");
        sqlx::query(
            "INSERT INTO erc20.token_metadata (token, name, symbol, decimals, total_supply)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(Felt::from(0x99_u64).to_be_bytes_vec())
        .bind("Nums")
        .bind("NUMS")
        .bind("18")
        .bind(vec![0x12_u8, 0x34_u8, 0x56_u8])
        .execute(&mut *conn)
        .await
        .expect("insert token metadata");
        drop(conn);

        let response = service
            .retrieve_token_contracts(Request::new(RetrieveTokenContractsRequest {
                query: Some(types::TokenContractQuery {
                    contract_addresses: vec![],
                    contract_types: vec![ContractType::Erc20 as i32],
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                }),
            }))
            .await
            .expect("retrieve token contracts")
            .into_inner();

        assert_eq!(response.token_contracts.len(), 1);
        let contract = &response.token_contracts[0];
        let expected_total_supply =
            canonical_u256_bytes_from_db(&[0x12, 0x34, 0x56]).expect("canonical");
        assert_eq!(
            contract.contract_address,
            Felt::from(0x99_u64).to_be_bytes_vec()
        );
        assert_eq!(contract.total_supply.as_ref(), Some(&expected_total_supply));
        assert_eq!(contract.metadata, Vec::<u8>::new());
        assert_eq!(contract.token_metadata, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn retrieve_token_balances_normalizes_erc721_token_id_to_u256_width() {
        let db_path = test_db_path("token-balances-encoding");
        let erc721_url = temp_sqlite_url("erc721-token-balances-encoding");
        let service = EcsService::new(&db_path, Some(1), None, Some(&erc721_url), None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        let mut conn = service
            .acquire_initialized_connection()
            .await
            .expect("setup connection");
        sqlx::query(
            "CREATE TABLE erc721.nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create ownership");
        sqlx::query(
            "INSERT INTO erc721.nft_ownership (token, token_id, owner)
             VALUES (?1, ?2, ?3)",
        )
        .bind(Felt::from(0x1137_u64).to_be_bytes_vec())
        .bind(vec![0x04_u8])
        .bind(Felt::from(0x25145_u64).to_be_bytes_vec())
        .execute(&mut *conn)
        .await
        .expect("insert ownership");
        drop(conn);

        let response = service
            .retrieve_token_balances(Request::new(RetrieveTokenBalancesRequest {
                query: Some(types::TokenBalanceQuery {
                    account_addresses: vec![],
                    contract_addresses: vec![],
                    token_ids: vec![],
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                }),
            }))
            .await
            .expect("retrieve token balances")
            .into_inner();

        assert_eq!(response.balances.len(), 1);
        let balance = &response.balances[0];
        let expected_token_id = u256_bytes_from_u64(4);
        assert_eq!(
            balance.contract_address,
            Felt::from(0x1137_u64).to_be_bytes_vec()
        );
        assert_eq!(
            balance.account_address,
            Felt::from(0x25145_u64).to_be_bytes_vec()
        );
        assert_eq!(balance.balance, u256_bytes_from_u64(1));
        assert_eq!(balance.token_id.as_ref(), Some(&expected_token_id));
    }

    #[tokio::test]
    async fn retrieve_token_balances_limits_merged_results_in_sorted_order() {
        let db_path = test_db_path("token-balances-limit-order");
        let erc20_url = temp_sqlite_url("erc20-token-balances-limit-order");
        let erc721_url = temp_sqlite_url("erc721-token-balances-limit-order");
        let erc1155_url = temp_sqlite_url("erc1155-token-balances-limit-order");
        let service = EcsService::new(
            &db_path,
            Some(1),
            Some(&erc20_url),
            Some(&erc721_url),
            Some(&erc1155_url),
        )
        .await
        .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        let mut conn = service
            .acquire_initialized_connection()
            .await
            .expect("setup connection");
        sqlx::query(
            "CREATE TABLE erc20.balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL,
                last_tx_hash BLOB NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create erc20 balances");
        sqlx::query(
            "CREATE TABLE erc721.nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create erc721 ownership");
        sqlx::query(
            "CREATE TABLE erc1155.erc1155_balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract BLOB NOT NULL,
                wallet BLOB NOT NULL,
                token_id BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block TEXT NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create erc1155 balances");

        sqlx::query(
            "INSERT INTO erc20.balances (token, wallet, balance, last_block, last_tx_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(Vec::<u8>::from(Felt::from(0x30_u64)))
        .bind(Vec::<u8>::from(Felt::from(0x10_u64)))
        .bind(vec![9_u8])
        .bind("1")
        .bind(vec![0_u8; 32])
        .execute(&mut *conn)
        .await
        .expect("insert erc20 balance");
        sqlx::query(
            "INSERT INTO erc721.nft_ownership (token, token_id, owner)
             VALUES (?1, ?2, ?3)",
        )
        .bind(Felt::from(0x10_u64).to_be_bytes_vec())
        .bind(vec![0x02_u8])
        .bind(Felt::from(0x20_u64).to_be_bytes_vec())
        .execute(&mut *conn)
        .await
        .expect("insert erc721 ownership");
        sqlx::query(
            "INSERT INTO erc1155.erc1155_balances (contract, wallet, token_id, balance, last_block)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(Felt::from(0x20_u64).to_be_bytes_vec())
        .bind(Felt::from(0x05_u64).to_be_bytes_vec())
        .bind(vec![0x01_u8])
        .bind(vec![7_u8])
        .bind("1")
        .execute(&mut *conn)
        .await
        .expect("insert erc1155 balance");
        drop(conn);

        let response = service
            .retrieve_token_balances(Request::new(RetrieveTokenBalancesRequest {
                query: Some(types::TokenBalanceQuery {
                    account_addresses: vec![],
                    contract_addresses: vec![],
                    token_ids: vec![],
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 2,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                }),
            }))
            .await
            .expect("retrieve token balances")
            .into_inner();

        assert_eq!(response.balances.len(), 2);
        assert_eq!(
            response.balances[0].contract_address,
            Felt::from(0x10_u64).to_be_bytes_vec()
        );
        assert_eq!(
            response.balances[0].account_address,
            Felt::from(0x20_u64).to_be_bytes_vec()
        );
        assert_eq!(
            response.balances[0].token_id.as_ref(),
            Some(&u256_bytes_from_u64(2))
        );

        assert_eq!(
            response.balances[1].contract_address,
            Felt::from(0x20_u64).to_be_bytes_vec()
        );
        assert_eq!(
            response.balances[1].account_address,
            Felt::from(0x05_u64).to_be_bytes_vec()
        );
        assert_eq!(
            response.balances[1].token_id.as_ref(),
            Some(&u256_bytes_from_u64(1))
        );
    }

    #[tokio::test]
    async fn retrieve_token_balances_matches_canonical_token_id_filters() {
        let db_path = test_db_path("token-balances-token-filter");
        let erc721_url = temp_sqlite_url("erc721-token-balances-token-filter");
        let service = EcsService::new(&db_path, Some(1), None, Some(&erc721_url), None)
            .await
            .expect("service init");
        service
            .attach_erc_databases()
            .await
            .expect("attach erc dbs");

        let mut conn = service
            .acquire_initialized_connection()
            .await
            .expect("setup connection");
        sqlx::query(
            "CREATE TABLE erc721.nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await
        .expect("create ownership");
        sqlx::query(
            "INSERT INTO erc721.nft_ownership (token, token_id, owner)
             VALUES (?1, ?2, ?3)",
        )
        .bind(Felt::from(0x99_u64).to_be_bytes_vec())
        .bind(vec![0x04_u8])
        .bind(Felt::from(0x77_u64).to_be_bytes_vec())
        .execute(&mut *conn)
        .await
        .expect("insert ownership");
        drop(conn);

        let response = service
            .retrieve_token_balances(Request::new(RetrieveTokenBalancesRequest {
                query: Some(types::TokenBalanceQuery {
                    account_addresses: vec![],
                    contract_addresses: vec![],
                    token_ids: vec![u256_bytes_from_u64(4)],
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: vec![],
                    }),
                }),
            }))
            .await
            .expect("retrieve token balances")
            .into_inner();

        assert_eq!(response.balances.len(), 1);
        assert_eq!(
            response.balances[0].contract_address,
            Felt::from(0x99_u64).to_be_bytes_vec()
        );
        assert_eq!(
            response.balances[0].account_address,
            Felt::from(0x77_u64).to_be_bytes_vec()
        );
        assert_eq!(
            response.balances[0].token_id.as_ref(),
            Some(&u256_bytes_from_u64(4))
        );
    }

    #[tokio::test]
    async fn init_skips_missing_sqlite_token_databases() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!("torii-ecs-main-{nonce}.db"));
        std::fs::File::create(&db_path).expect("create main db file");
        let db_path = db_path.display().to_string();
        let missing_erc20 = temp_sqlite_url("missing-token-db-erc20");

        let service = EcsService::new(&db_path, Some(1), Some(&missing_erc20), None, None)
            .await
            .expect("service init");

        service
            .attach_erc_databases()
            .await
            .expect("missing token db should be skipped");

        sqlx::query("CREATE TABLE erc20.balances (id INTEGER PRIMARY KEY)")
            .execute(&service.state.pool)
            .await
            .expect("missing token db should be attached and writable");

        assert!(service.state.has_erc20);
    }

    #[tokio::test]
    async fn retrieve_controllers_reads_backing_table() {
        let db_path = test_db_path("retrieve-controllers");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");
        let controller = felt_hex(Felt::from(0x123_u64));

        sqlx::query(
            "CREATE TABLE controllers (
                id TEXT PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL,
                deployed_at TEXT NOT NULL,
                updated_at BIGINT NOT NULL
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create controllers table");
        sqlx::query(
            "INSERT INTO controllers (id, address, username, deployed_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&controller)
        .bind(&controller)
        .bind("alice")
        .bind("2024-03-20T12:00:00Z")
        .bind(1_710_936_000_i64)
        .execute(&service.state.pool)
        .await
        .expect("insert controller");

        let response = service
            .retrieve_controllers(Request::new(RetrieveControllersRequest {
                query: Some(types::ControllerQuery {
                    contract_addresses: vec![Felt::from(0x123_u64).into()],
                    usernames: Vec::new(),
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 10,
                        direction: PaginationDirection::Forward as i32,
                        order_by: Vec::new(),
                    }),
                }),
            }))
            .await
            .expect("retrieve controllers")
            .into_inner();

        assert_eq!(response.controllers.len(), 1);
        assert!(response.next_cursor.is_empty());
        assert_eq!(
            response.controllers[0].address,
            Vec::<u8>::from(Felt::from(0x123_u64))
        );
        assert_eq!(response.controllers[0].username, "alice");
        assert_eq!(response.controllers[0].deployed_at_timestamp, 1_710_936_000);
    }

    #[tokio::test]
    async fn retrieve_controllers_respects_requested_limit_above_shared_cap() {
        let db_path = test_db_path("retrieve-controllers-limit");
        let service = EcsService::new(&db_path, Some(1), None, None, None)
            .await
            .expect("service init");

        sqlx::query(
            "CREATE TABLE controllers (
                id TEXT PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL,
                deployed_at TEXT NOT NULL,
                updated_at BIGINT NOT NULL
            )",
        )
        .execute(&service.state.pool)
        .await
        .expect("create controllers table");

        let mut tx = service
            .state
            .pool
            .begin()
            .await
            .expect("begin controller insert tx");
        for i in 0..1_500_u64 {
            let controller = felt_hex(Felt::from(i + 1));
            sqlx::query(
                "INSERT INTO controllers (id, address, username, deployed_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(&controller)
            .bind(&controller)
            .bind(format!("user-{i}"))
            .bind("2024-03-20T12:00:00Z")
            .bind(i as i64)
            .execute(&mut *tx)
            .await
            .expect("insert controller");
        }
        tx.commit().await.expect("commit controller inserts");

        let response = service
            .retrieve_controllers(Request::new(RetrieveControllersRequest {
                query: Some(types::ControllerQuery {
                    contract_addresses: Vec::new(),
                    usernames: Vec::new(),
                    pagination: Some(types::Pagination {
                        cursor: String::new(),
                        limit: 1_200,
                        direction: PaginationDirection::Forward as i32,
                        order_by: Vec::new(),
                    }),
                }),
            }))
            .await
            .expect("retrieve controllers")
            .into_inner();

        assert_eq!(response.controllers.len(), 1_200);
        assert!(!response.next_cursor.is_empty());
        assert_eq!(
            response
                .controllers
                .first()
                .map(|controller| controller.address.clone()),
            Some(Vec::<u8>::from(Felt::from(1_u64)))
        );
        assert_eq!(
            response
                .controllers
                .last()
                .map(|controller| controller.address.clone()),
            Some(Vec::<u8>::from(Felt::from(1_200_u64)))
        );
    }
}
