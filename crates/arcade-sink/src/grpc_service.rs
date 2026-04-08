use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde_json::Value;
use sqlx::{
    any::AnyPoolOptions, sqlite::SqliteConnectOptions, Any, ConnectOptions, Pool, QueryBuilder, Row,
};
use starknet::core::types::{Felt, U256};
use tonic::{Request, Response, Status};
use torii_common::{blob_to_u256, u256_to_blob};
use torii_erc721::{storage::OwnershipCursor, Erc721Storage};
use torii_runtime_common::database::DEFAULT_SQLITE_MAX_CONNECTIONS;
use torii_sql::DbPool;

use crate::proto::arcade::{
    arcade_server::Arcade, Collection, Edition, Game, GetPlayerInventoryRequest,
    GetPlayerInventoryResponse, InventoryItem, ListCollectionsRequest, ListCollectionsResponse,
    ListEditionsRequest, ListEditionsResponse, ListGamesRequest, ListGamesResponse,
    ListListingsRequest, ListListingsResponse, ListSalesRequest, ListSalesResponse,
    MarketplaceListing, MarketplaceSale,
};

const GAME_TABLE: &str = "ARCADE-Game";
const EDITION_TABLE: &str = "ARCADE-Edition";
const COLLECTION_TABLE: &str = "ARCADE-Collection";
const COLLECTION_EDITION_TABLE: &str = "ARCADE-CollectionEdition";
const ORDER_TABLE: &str = "ARCADE-Order";
const LISTING_TABLE: &str = "ARCADE-Listing";
const SALE_TABLE: &str = "ARCADE-Sale";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbBackend {
    Sqlite,
    Postgres,
}

impl DbBackend {
    fn detect(database_url: &str) -> Self {
        if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
            Self::Postgres
        } else {
            Self::Sqlite
        }
    }
}

#[derive(Clone)]
pub struct ArcadeService {
    state: Arc<ArcadeState>,
}

struct ArcadeState {
    pool: Pool<Any>,
    backend: DbBackend,
    erc721: Arc<Erc721Storage>,
}

#[derive(Clone)]
struct CollectionRecord {
    collection_id: String,
    uuid: Option<String>,
    contract_address: String,
}

struct ListingRecord {
    order_id: u64,
    entity_id: String,
    collection_id: String,
    token_id: String,
    currency: String,
    owner: String,
    price: String,
    quantity: String,
    expiration: u64,
    status: u32,
    category: u32,
    royalties: bool,
    time: u64,
}

struct SaleRecord {
    order_id: u64,
    entity_id: String,
    collection_id: String,
    token_id: String,
    currency: String,
    seller: String,
    buyer: String,
    price: String,
    quantity: String,
    expiration: u64,
    status: u32,
    category: u32,
    royalties: bool,
    time: u64,
}

impl ArcadeService {
    pub async fn new(
        database_url: &str,
        erc721_database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
        sqlx::any::install_default_drivers();

        let backend = DbBackend::detect(database_url);
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

        let erc721_pool: DbPool = Erc721Storage::connect_pool(erc721_database_url).await?;
        let erc721 = Arc::new(Erc721Storage::from_pool(erc721_database_url, erc721_pool).await?);

        let service = Self {
            state: Arc::new(ArcadeState {
                pool,
                backend,
                erc721,
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

        for statement in projection_schema_sql() {
            sqlx::query(statement).execute(&self.state.pool).await?;
        }

        self.bootstrap_from_source().await?;
        Ok(())
    }

    pub async fn bootstrap_from_source(&self) -> Result<()> {
        self.clear_projection_tables().await?;
        self.bootstrap_games().await?;
        self.bootstrap_editions().await?;
        self.bootstrap_collections().await?;
        self.bootstrap_listings().await?;
        self.bootstrap_sales().await?;
        Ok(())
    }

    pub async fn refresh_game(&self, entity_id: Felt) -> Result<()> {
        if !self.source_table_exists(GAME_TABLE).await? {
            return Ok(());
        }
        if let Some(row) = self
            .fetch_source_row_by_entity_id(GAME_TABLE, entity_id)
            .await?
        {
            self.upsert_game_row(&row).await?;
        }
        Ok(())
    }

    pub async fn refresh_edition(&self, entity_id: Felt) -> Result<()> {
        if !self.source_table_exists(EDITION_TABLE).await? {
            return Ok(());
        }
        if let Some(row) = self
            .fetch_source_row_by_entity_id(EDITION_TABLE, entity_id)
            .await?
        {
            self.upsert_edition_row(&row).await?;
        }
        Ok(())
    }

    pub async fn refresh_collection(&self, entity_id: Felt) -> Result<()> {
        let collection_table_exists = self.source_table_exists(COLLECTION_TABLE).await?;
        let relation_table_exists = self.source_table_exists(COLLECTION_EDITION_TABLE).await?;
        if !collection_table_exists && !relation_table_exists {
            return Ok(());
        }

        if collection_table_exists {
            if let Some(row) = self
                .fetch_source_row_by_entity_id(COLLECTION_TABLE, entity_id)
                .await?
            {
                self.refresh_collection_from_row(&row).await?;
                return Ok(());
            }
        }

        if relation_table_exists {
            if let Some(row) = self
                .fetch_source_row_by_entity_id(COLLECTION_EDITION_TABLE, entity_id)
                .await?
            {
                self.refresh_collection_from_row(&row).await?;
                return Ok(());
            }
        }

        Ok(())
    }

    pub async fn refresh_listing(&self, entity_id: Felt) -> Result<()> {
        let Some(listing_table) = self.listing_source_table().await? else {
            return Ok(());
        };
        if let Some(row) = self
            .fetch_source_row_by_entity_id(listing_table, entity_id)
            .await?
        {
            self.upsert_listing_row(&row).await?;
        }
        Ok(())
    }

    pub async fn refresh_sale(&self, entity_id: Felt) -> Result<()> {
        if !self.source_table_exists(SALE_TABLE).await? {
            return Ok(());
        }
        if let Some(row) = self
            .fetch_source_row_by_entity_id(SALE_TABLE, entity_id)
            .await?
        {
            self.upsert_sale_row(&row).await?;
        }
        Ok(())
    }

    pub async fn delete_game(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_games", entity_id).await
    }

    pub async fn delete_edition(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_editions", entity_id)
            .await
    }

    pub async fn delete_collection(&self, entity_id: Felt) -> Result<()> {
        self.delete_collection_rows_for_entity(entity_id).await
    }

    pub async fn delete_listing(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_listings", entity_id)
            .await
    }

    pub async fn delete_sale(&self, entity_id: Felt) -> Result<()> {
        self.delete_by_entity("torii_arcade_sales", entity_id).await
    }

    async fn delete_by_entity(&self, table: &str, entity_id: Felt) -> Result<()> {
        let sql = select_by_entity_match_sql(self.state.backend, table, "DELETE FROM");
        sqlx::query(&sql)
            .bind(felt_hex(entity_id))
            .bind(felt_hex_padded(entity_id))
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn clear_projection_tables(&self) -> Result<()> {
        for table in [
            "torii_arcade_games",
            "torii_arcade_editions",
            "torii_arcade_collections",
            "torii_arcade_listings",
            "torii_arcade_sales",
        ] {
            self.clear_projection_table(table).await?;
        }
        Ok(())
    }

    async fn clear_projection_table(&self, table: &str) -> Result<()> {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn fetch_source_row_by_entity_id(
        &self,
        table: &str,
        entity_id: Felt,
    ) -> Result<Option<sqlx::any::AnyRow>> {
        let sql = select_by_entity_match_sql(self.state.backend, table, "SELECT * FROM");
        let entity_id_hex = felt_hex(entity_id);
        let entity_id_padded = felt_hex_padded(entity_id);
        let row = sqlx::query(&sql)
            .bind(entity_id_hex.clone())
            .bind(entity_id_padded.clone())
            .fetch_optional(&self.state.pool)
            .await?;
        Ok(row)
    }

    async fn fetch_source_row_by_field(
        &self,
        table: &str,
        field: &str,
        value: &str,
    ) -> Result<Option<sqlx::any::AnyRow>> {
        let padded = felt_from_hex(value).map_or_else(|_| value.to_string(), felt_hex_padded);
        let sql = select_by_field_match_sql(self.state.backend, table, field, "SELECT * FROM");
        let row = sqlx::query(&sql)
            .bind(value)
            .bind(padded.clone())
            .fetch_optional(&self.state.pool)
            .await?;
        Ok(row)
    }

    async fn fetch_source_rows_by_field(
        &self,
        table: &str,
        field: &str,
        value: &str,
    ) -> Result<Vec<sqlx::any::AnyRow>> {
        let padded = felt_from_hex(value).map_or_else(|_| value.to_string(), felt_hex_padded);
        let sql = select_by_field_match_sql(self.state.backend, table, field, "SELECT * FROM");
        let rows = sqlx::query(&sql)
            .bind(value)
            .bind(padded.clone())
            .fetch_all(&self.state.pool)
            .await?;
        Ok(rows)
    }

    async fn delete_collection_rows_for_entity(&self, entity_id: Felt) -> Result<()> {
        let entity_id_hex = felt_hex(entity_id);
        let entity_id_padded = felt_hex_padded(entity_id);
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "DELETE FROM torii_arcade_collections
                 WHERE entity_id = ?1 OR entity_id = ?2
                    OR collection_entity_id = ?1 OR collection_entity_id = ?2"
            }
            DbBackend::Postgres => {
                "DELETE FROM torii_arcade_collections
                 WHERE entity_id = $1 OR entity_id = $2
                    OR collection_entity_id = $1 OR collection_entity_id = $2"
            }
        };
        sqlx::query(sql)
            .bind(entity_id_hex.clone())
            .bind(entity_id_padded.clone())
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn refresh_collection_from_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        if row.try_get_raw("collection").is_ok() && row.try_get_raw("edition").is_ok() {
            return self.upsert_collection_relation_row(row).await;
        }

        let collection_id = row_felt_hex(row, "id")?;
        let collection_entity_id = row_felt_hex(row, "entity_id")?;

        if self.source_table_exists(COLLECTION_EDITION_TABLE).await? {
            self.delete_collection_rows_for_entity(felt_from_hex(&collection_entity_id)?)
                .await?;
            let relation_rows = self
                .fetch_source_rows_by_field(COLLECTION_EDITION_TABLE, "collection", &collection_id)
                .await?;
            for relation_row in relation_rows {
                self.upsert_collection_relation_row(&relation_row).await?;
            }
            return Ok(());
        }

        self.upsert_collection_row(row).await
    }

    async fn bootstrap_games(&self) -> Result<()> {
        if !self.source_table_exists(GAME_TABLE).await? {
            return Ok(());
        }
        let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Game""#)
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_game_row(&row).await?;
        }
        Ok(())
    }

    async fn bootstrap_editions(&self) -> Result<()> {
        if !self.source_table_exists(EDITION_TABLE).await? {
            return Ok(());
        }
        let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Edition""#)
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_edition_row(&row).await?;
        }
        Ok(())
    }

    async fn bootstrap_collections(&self) -> Result<()> {
        let collection_table_exists = self.source_table_exists(COLLECTION_TABLE).await?;
        let relation_table_exists = self.source_table_exists(COLLECTION_EDITION_TABLE).await?;
        if !collection_table_exists && !relation_table_exists {
            return Ok(());
        }
        if relation_table_exists {
            let rows = sqlx::query(r#"SELECT * FROM "ARCADE-CollectionEdition""#)
                .fetch_all(&self.state.pool)
                .await?;
            for row in rows {
                self.upsert_collection_relation_row(&row).await?;
            }
        } else if collection_table_exists {
            let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Collection""#)
                .fetch_all(&self.state.pool)
                .await?;
            for row in rows {
                self.upsert_collection_row(&row).await?;
            }
        }
        Ok(())
    }

    async fn bootstrap_listings(&self) -> Result<()> {
        let Some(listing_table) = self.listing_source_table().await? else {
            return Ok(());
        };
        let rows = sqlx::query(&format!(r#"SELECT * FROM "{listing_table}""#))
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_listing_row(&row).await?;
        }
        Ok(())
    }

    async fn bootstrap_sales(&self) -> Result<()> {
        if !self.source_table_exists(SALE_TABLE).await? {
            return Ok(());
        }
        let rows = sqlx::query(r#"SELECT * FROM "ARCADE-Sale""#)
            .fetch_all(&self.state.pool)
            .await?;
        for row in rows {
            self.upsert_sale_row(&row).await?;
        }
        Ok(())
    }

    async fn source_table_exists(&self, table_name: &str) -> Result<bool> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1"
            }
            DbBackend::Postgres => {
                "SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = $1"
            }
        };
        let count: i64 = sqlx::query_scalar(sql)
            .bind(table_name)
            .fetch_one(&self.state.pool)
            .await?;
        Ok(count > 0)
    }

    pub async fn load_tracked_table_names_by_id(&self) -> Result<HashMap<String, String>> {
        if !self.source_table_exists("dojo_tables").await? {
            return Ok(HashMap::new());
        }

        let mut builder: QueryBuilder<Any> =
            QueryBuilder::new("SELECT id, name FROM dojo_tables WHERE name IN (");
        let mut separated = builder.separated(", ");
        for table_name in [
            GAME_TABLE,
            EDITION_TABLE,
            COLLECTION_TABLE,
            COLLECTION_EDITION_TABLE,
            ORDER_TABLE,
            LISTING_TABLE,
            SALE_TABLE,
        ] {
            separated.push_bind(table_name);
        }
        separated.push_unseparated(")");

        let rows = builder.build().fetch_all(&self.state.pool).await?;
        let mut names_by_id = HashMap::with_capacity(rows.len());
        for row in rows {
            names_by_id.insert(row_felt_hex(&row, "id")?, row_string(&row, "name")?);
        }

        Ok(names_by_id)
    }

    async fn listing_source_table(&self) -> Result<Option<&'static str>> {
        for table_name in [ORDER_TABLE, LISTING_TABLE] {
            if self.source_table_exists(table_name).await? {
                return Ok(Some(table_name));
            }
        }

        Ok(None)
    }

    async fn upsert_game_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let game_id = row_felt_hex(row, "id")?;
        let entity_id = row_felt_hex(row, "entity_id")?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_games (
                    game_id, entity_id, name, description, published, whitelisted, color, image,
                    external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, strftime('%s','now'))
                 ON CONFLICT(game_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    name = excluded.name,
                    description = excluded.description,
                    published = excluded.published,
                    whitelisted = excluded.whitelisted,
                    color = excluded.color,
                    image = excluded.image,
                    external_url = excluded.external_url,
                    animation_url = excluded.animation_url,
                    youtube_url = excluded.youtube_url,
                    attributes_json = excluded.attributes_json,
                    properties_json = excluded.properties_json,
                    socials_json = excluded.socials_json,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_games (
                    game_id, entity_id, name, description, published, whitelisted, color, image,
                    external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(game_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    published = EXCLUDED.published,
                    whitelisted = EXCLUDED.whitelisted,
                    color = EXCLUDED.color,
                    image = EXCLUDED.image,
                    external_url = EXCLUDED.external_url,
                    animation_url = EXCLUDED.animation_url,
                    youtube_url = EXCLUDED.youtube_url,
                    attributes_json = EXCLUDED.attributes_json,
                    properties_json = EXCLUDED.properties_json,
                    socials_json = EXCLUDED.socials_json,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(game_id.clone())
            .bind(entity_id.clone())
            .bind(row_opt_string(row, "name")?)
            .bind(row_opt_string(row, "description")?)
            .bind(row_bool_i64(row, "published")?)
            .bind(row_bool_i64(row, "whitelisted")?)
            .bind(row_opt_string(row, "color")?)
            .bind(row_opt_string(row, "image")?)
            .bind(row_opt_string(row, "external_url")?)
            .bind(row_opt_string(row, "animation_url")?)
            .bind(row_opt_string(row, "youtube_url")?)
            .bind(row_opt_string(row, "attributes")?)
            .bind(row_opt_string(row, "properties")?)
            .bind(row_opt_string(row, "socials")?)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_edition_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let edition_id = row_felt_hex(row, "id")?;
        let entity_id = row_felt_hex(row, "entity_id")?;
        let game_id = row_felt_hex(row, "game_id")?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_editions (
                    edition_id, entity_id, game_id, world_address, namespace, name, description,
                    published, whitelisted, priority, config_json, color, image, external_url,
                    animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, strftime('%s','now'))
                 ON CONFLICT(edition_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    game_id = excluded.game_id,
                    world_address = excluded.world_address,
                    namespace = excluded.namespace,
                    name = excluded.name,
                    description = excluded.description,
                    published = excluded.published,
                    whitelisted = excluded.whitelisted,
                    priority = excluded.priority,
                    config_json = excluded.config_json,
                    color = excluded.color,
                    image = excluded.image,
                    external_url = excluded.external_url,
                    animation_url = excluded.animation_url,
                    youtube_url = excluded.youtube_url,
                    attributes_json = excluded.attributes_json,
                    properties_json = excluded.properties_json,
                    socials_json = excluded.socials_json,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_editions (
                    edition_id, entity_id, game_id, world_address, namespace, name, description,
                    published, whitelisted, priority, config_json, color, image, external_url,
                    animation_url, youtube_url, attributes_json, properties_json, socials_json, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(edition_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    game_id = EXCLUDED.game_id,
                    world_address = EXCLUDED.world_address,
                    namespace = EXCLUDED.namespace,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    published = EXCLUDED.published,
                    whitelisted = EXCLUDED.whitelisted,
                    priority = EXCLUDED.priority,
                    config_json = EXCLUDED.config_json,
                    color = EXCLUDED.color,
                    image = EXCLUDED.image,
                    external_url = EXCLUDED.external_url,
                    animation_url = EXCLUDED.animation_url,
                    youtube_url = EXCLUDED.youtube_url,
                    attributes_json = EXCLUDED.attributes_json,
                    properties_json = EXCLUDED.properties_json,
                    socials_json = EXCLUDED.socials_json,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(edition_id.clone())
            .bind(entity_id.clone())
            .bind(game_id.clone())
            .bind(row_opt_felt_hex(row, "world_address")?)
            .bind(row_opt_felt_hex(row, "namespace")?)
            .bind(row_opt_string(row, "name")?)
            .bind(row_opt_string(row, "description")?)
            .bind(row_bool_i64(row, "published")?)
            .bind(row_bool_i64(row, "whitelisted")?)
            .bind(row_u64(row, "priority")? as i64)
            .bind(row_opt_string(row, "config")?)
            .bind(row_opt_string(row, "color")?)
            .bind(row_opt_string(row, "image")?)
            .bind(row_opt_string(row, "external_url")?)
            .bind(row_opt_string(row, "animation_url")?)
            .bind(row_opt_string(row, "youtube_url")?)
            .bind(row_opt_string(row, "attributes")?)
            .bind(row_opt_string(row, "properties")?)
            .bind(row_opt_string(row, "socials")?)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_collection_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let collection_id = row_felt_hex(row, "id")?;
        let entity_id = row_felt_hex(row, "entity_id")?;
        let contract_address = row_felt_hex(row, "contract_address")?;
        self.upsert_collection_projection(
            entity_id.clone(),
            entity_id,
            collection_id,
            None,
            row_opt_felt_hex(row, "uuid")?,
            contract_address,
        )
        .await
    }

    async fn upsert_collection_relation_row(&self, relation_row: &sqlx::any::AnyRow) -> Result<()> {
        let relation_entity_id = row_felt_hex(relation_row, "entity_id")?;
        let collection_id = row_felt_hex(relation_row, "collection")?;
        let edition_id = row_felt_hex(relation_row, "edition")?;
        let active = row_bool_i64(relation_row, "active")? != 0;

        if !active {
            self.delete_by_entity(
                "torii_arcade_collections",
                felt_from_hex(&relation_entity_id)?,
            )
            .await?;
            return Ok(());
        }

        let (collection_entity_id, uuid, contract_address) =
            if self.source_table_exists(COLLECTION_TABLE).await? {
                if let Some(collection_row) = self
                    .fetch_source_row_by_field(COLLECTION_TABLE, "id", &collection_id)
                    .await?
                {
                    (
                        row_felt_hex(&collection_row, "entity_id")?,
                        row_opt_felt_hex(&collection_row, "uuid")?,
                        row_felt_hex(&collection_row, "contract_address")?,
                    )
                } else {
                    // In Arcade, CollectionEdition.collection is already the collection address.
                    (relation_entity_id.clone(), None, collection_id.clone())
                }
            } else {
                // In Arcade, CollectionEdition.collection is already the collection address.
                (relation_entity_id.clone(), None, collection_id.clone())
            };

        self.upsert_collection_projection(
            relation_entity_id,
            collection_entity_id,
            collection_id,
            Some(edition_id),
            uuid,
            contract_address,
        )
        .await
    }

    async fn upsert_collection_projection(
        &self,
        entity_id: String,
        collection_entity_id: String,
        collection_id: String,
        edition_id: Option<String>,
        uuid: Option<String>,
        contract_address: String,
    ) -> Result<()> {
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_collections (
                    entity_id, collection_entity_id, collection_id, edition_id, uuid,
                    contract_address, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%s','now'))
                 ON CONFLICT(entity_id) DO UPDATE SET
                    collection_entity_id = excluded.collection_entity_id,
                    collection_id = excluded.collection_id,
                    edition_id = excluded.edition_id,
                    uuid = excluded.uuid,
                    contract_address = excluded.contract_address,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_collections (
                    entity_id, collection_entity_id, collection_id, edition_id, uuid,
                    contract_address, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(entity_id) DO UPDATE SET
                    collection_entity_id = EXCLUDED.collection_entity_id,
                    collection_id = EXCLUDED.collection_id,
                    edition_id = EXCLUDED.edition_id,
                    uuid = EXCLUDED.uuid,
                    contract_address = EXCLUDED.contract_address,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(entity_id)
            .bind(collection_entity_id)
            .bind(collection_id)
            .bind(edition_id)
            .bind(uuid)
            .bind(contract_address)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_listing_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let parsed = parse_listing_row(row)?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_listings (
                    order_id, entity_id, collection_id, token_id, currency, owner, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, strftime('%s','now'))
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    collection_id = excluded.collection_id,
                    token_id = excluded.token_id,
                    currency = excluded.currency,
                    owner = excluded.owner,
                    price = excluded.price,
                    quantity = excluded.quantity,
                    expiration = excluded.expiration,
                    status = excluded.status,
                    category = excluded.category,
                    royalties = excluded.royalties,
                    time = excluded.time,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_listings (
                    order_id, entity_id, collection_id, token_id, currency, owner, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    collection_id = EXCLUDED.collection_id,
                    token_id = EXCLUDED.token_id,
                    currency = EXCLUDED.currency,
                    owner = EXCLUDED.owner,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    expiration = EXCLUDED.expiration,
                    status = EXCLUDED.status,
                    category = EXCLUDED.category,
                    royalties = EXCLUDED.royalties,
                    time = EXCLUDED.time,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(parsed.order_id as i64)
            .bind(parsed.entity_id)
            .bind(parsed.collection_id)
            .bind(parsed.token_id)
            .bind(parsed.currency)
            .bind(parsed.owner)
            .bind(parsed.price)
            .bind(parsed.quantity)
            .bind(parsed.expiration as i64)
            .bind(parsed.status as i64)
            .bind(parsed.category as i64)
            .bind(i64::from(parsed.royalties))
            .bind(parsed.time as i64)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn upsert_sale_row(&self, row: &sqlx::any::AnyRow) -> Result<()> {
        let parsed = parse_sale_row(row)?;
        let sql = match self.state.backend {
            DbBackend::Sqlite => {
                "INSERT INTO torii_arcade_sales (
                    order_id, entity_id, collection_id, token_id, currency, seller, buyer, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, strftime('%s','now'))
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = excluded.entity_id,
                    collection_id = excluded.collection_id,
                    token_id = excluded.token_id,
                    currency = excluded.currency,
                    seller = excluded.seller,
                    buyer = excluded.buyer,
                    price = excluded.price,
                    quantity = excluded.quantity,
                    expiration = excluded.expiration,
                    status = excluded.status,
                    category = excluded.category,
                    royalties = excluded.royalties,
                    time = excluded.time,
                    updated_at = excluded.updated_at"
            }
            DbBackend::Postgres => {
                "INSERT INTO torii_arcade_sales (
                    order_id, entity_id, collection_id, token_id, currency, seller, buyer, price, quantity,
                    expiration, status, category, royalties, time, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(order_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    collection_id = EXCLUDED.collection_id,
                    token_id = EXCLUDED.token_id,
                    currency = EXCLUDED.currency,
                    seller = EXCLUDED.seller,
                    buyer = EXCLUDED.buyer,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    expiration = EXCLUDED.expiration,
                    status = EXCLUDED.status,
                    category = EXCLUDED.category,
                    royalties = EXCLUDED.royalties,
                    time = EXCLUDED.time,
                    updated_at = EXCLUDED.updated_at"
            }
        };

        sqlx::query(sql)
            .bind(parsed.order_id as i64)
            .bind(parsed.entity_id)
            .bind(parsed.collection_id)
            .bind(parsed.token_id)
            .bind(parsed.currency)
            .bind(parsed.seller)
            .bind(parsed.buyer)
            .bind(parsed.price)
            .bind(parsed.quantity)
            .bind(parsed.expiration as i64)
            .bind(parsed.status as i64)
            .bind(parsed.category as i64)
            .bind(i64::from(parsed.royalties))
            .bind(parsed.time as i64)
            .execute(&self.state.pool)
            .await?;
        Ok(())
    }

    async fn load_collection_records(&self) -> Result<Vec<CollectionRecord>> {
        let rows = sqlx::query(
            "SELECT DISTINCT collection_id, edition_id, uuid, contract_address
             FROM torii_arcade_collections
             ORDER BY collection_id ASC",
        )
        .fetch_all(&self.state.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(CollectionRecord {
                    collection_id: row.try_get("collection_id")?,
                    uuid: row.try_get("uuid").ok(),
                    contract_address: row.try_get("contract_address")?,
                })
            })
            .collect()
    }

    async fn load_collections_by_contract(&self) -> Result<HashMap<String, CollectionRecord>> {
        Ok(self
            .load_collection_records()
            .await?
            .into_iter()
            .map(|record| (record.contract_address.clone(), record))
            .collect())
    }

    async fn load_collections_by_id(&self) -> Result<HashMap<String, CollectionRecord>> {
        Ok(self
            .load_collection_records()
            .await?
            .into_iter()
            .map(|record| (record.collection_id.clone(), record))
            .collect())
    }
}

#[tonic::async_trait]
impl Arcade for ArcadeService {
    async fn list_games(
        &self,
        request: Request<ListGamesRequest>,
    ) -> Result<Response<ListGamesResponse>, Status> {
        let req = request.into_inner();
        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT game_id, entity_id, name, description, published, whitelisted, color, image, external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json FROM torii_arcade_games WHERE 1=1",
        );
        if !req.include_unpublished {
            builder.push(" AND published = ").push_bind(1_i64);
        }
        if !req.include_unwhitelisted {
            builder.push(" AND whitelisted = ").push_bind(1_i64);
        }
        builder.push(" ORDER BY name ASC");

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;
        let games = rows
            .into_iter()
            .map(|row| game_from_row(&row))
            .collect::<Result<Vec<_>>>()
            .map_err(internal_status)?;

        Ok(Response::new(ListGamesResponse { games }))
    }

    async fn list_editions(
        &self,
        request: Request<ListEditionsRequest>,
    ) -> Result<Response<ListEditionsResponse>, Status> {
        let req = request.into_inner();
        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT edition_id, entity_id, game_id, world_address, namespace, name, description, published, whitelisted, priority, config_json, color, image, external_url, animation_url, youtube_url, attributes_json, properties_json, socials_json FROM torii_arcade_editions WHERE 1=1",
        );
        if !req.game_id.is_empty() {
            let game_id = felt_hex(felt_from_bytes(&req.game_id).map_err(internal_status)?);
            builder.push(" AND game_id = ").push_bind(game_id);
        }
        if !req.collection_id.is_empty() {
            let collection_id =
                felt_hex(felt_from_bytes(&req.collection_id).map_err(internal_status)?);
            builder.push(
                " AND EXISTS (
                    SELECT 1
                    FROM torii_arcade_collections collections
                    WHERE collections.edition_id = torii_arcade_editions.edition_id
                      AND collections.collection_id = ",
            );
            builder.push_bind(collection_id).push(")");
        }
        if !req.include_unpublished {
            builder.push(" AND published = ").push_bind(1_i64);
        }
        if !req.include_unwhitelisted {
            builder.push(" AND whitelisted = ").push_bind(1_i64);
        }
        builder.push(" ORDER BY priority DESC, edition_id ASC");
        builder
            .push(" LIMIT ")
            .push_bind(i64::from(limit_or_default(req.limit, 100)));

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;
        let editions = rows
            .into_iter()
            .map(|row| edition_from_row(&row))
            .collect::<Result<Vec<_>>>()
            .map_err(internal_status)?;

        Ok(Response::new(ListEditionsResponse { editions }))
    }

    async fn list_collections(
        &self,
        request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let req = request.into_inner();
        let limit = limit_or_default(req.limit, 500);
        let rows = QueryBuilder::<Any>::new(
            "SELECT collection_id, uuid, contract_address
             FROM (
                SELECT DISTINCT collection_id, uuid, contract_address
                FROM torii_arcade_collections
             )
             ORDER BY collection_id ASC LIMIT ",
        )
        .push_bind(i64::from(limit))
        .build()
        .fetch_all(&self.state.pool)
        .await
        .map_err(internal_status)?;

        let mut collections = Vec::with_capacity(rows.len());
        for row in rows {
            let contract_address = row_string(&row, "contract_address").map_err(internal_status)?;
            let token_felt = felt_from_hex(&contract_address).map_err(internal_status)?;
            let metadata = self
                .state
                .erc721
                .get_token_metadata(token_felt)
                .await
                .map_err(internal_status)?;
            let (name, symbol, _) = metadata.unwrap_or((None, None, None));
            collections.push(Collection {
                collection_id: felt_from_hex(
                    &row_string(&row, "collection_id").map_err(internal_status)?,
                )
                .map_err(internal_status)?
                .to_bytes_be()
                .to_vec(),
                uuid: row
                    .try_get::<String, _>("uuid")
                    .ok()
                    .and_then(|value| felt_from_hex(&value).ok())
                    .map(|felt| felt.to_bytes_be().to_vec())
                    .unwrap_or_default(),
                contract_address: token_felt.to_bytes_be().to_vec(),
                name: name.unwrap_or_default(),
                symbol: symbol.unwrap_or_default(),
            });
        }

        Ok(Response::new(ListCollectionsResponse { collections }))
    }

    async fn list_listings(
        &self,
        request: Request<ListListingsRequest>,
    ) -> Result<Response<ListListingsResponse>, Status> {
        let req = request.into_inner();
        let collections_by_id = self
            .load_collections_by_id()
            .await
            .map_err(internal_status)?;
        let collections_by_contract = self
            .load_collections_by_contract()
            .await
            .map_err(internal_status)?;
        let collection_id_filter = if req.collection_id.is_empty() {
            None
        } else {
            Some(felt_hex(
                felt_from_bytes(&req.collection_id).map_err(internal_status)?,
            ))
        };
        let collection_contract_filter = if req.collection_contract.is_empty() {
            None
        } else {
            let felt = felt_from_bytes(&req.collection_contract).map_err(internal_status)?;
            let hex = felt_hex(felt);
            collections_by_contract
                .get(&hex)
                .map(|record| record.collection_id.clone())
        };

        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT order_id, entity_id, collection_id, token_id, currency, owner, price, quantity, expiration, status, category, royalties, time FROM torii_arcade_listings WHERE 1=1",
        );
        if let Some(collection_id) = collection_id_filter.or(collection_contract_filter) {
            builder
                .push(" AND collection_id = ")
                .push_bind(collection_id);
        }
        if !req.owner.is_empty() {
            builder.push(" AND owner = ").push_bind(felt_hex(
                felt_from_bytes(&req.owner).map_err(internal_status)?,
            ));
        }
        if !req.currency.is_empty() {
            builder.push(" AND currency = ").push_bind(felt_hex(
                felt_from_bytes(&req.currency).map_err(internal_status)?,
            ));
        }
        if !req.include_inactive {
            builder.push(" AND status = ").push_bind(1_i64);
        }
        if req.cursor_order_id > 0 {
            builder
                .push(" AND order_id < ")
                .push_bind(req.cursor_order_id as i64);
        }
        builder.push(" ORDER BY order_id DESC");
        let limit = limit_or_default(req.limit, 100);
        builder.push(" LIMIT ").push_bind((limit + 1) as i64);

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;

        let mut listings = Vec::new();
        let mut next_cursor_order_id = 0_u64;
        for (index, row) in rows.into_iter().enumerate() {
            if index == limit as usize {
                next_cursor_order_id = row.try_get::<i64, _>("order_id").unwrap_or_default() as u64;
                break;
            }
            listings.push(
                listing_from_projection_row(&row, &collections_by_id, &self.state.erc721)
                    .await
                    .map_err(internal_status)?,
            );
        }

        Ok(Response::new(ListListingsResponse {
            listings,
            next_cursor_order_id,
        }))
    }

    async fn list_sales(
        &self,
        request: Request<ListSalesRequest>,
    ) -> Result<Response<ListSalesResponse>, Status> {
        let req = request.into_inner();
        let collections_by_id = self
            .load_collections_by_id()
            .await
            .map_err(internal_status)?;
        let collections_by_contract = self
            .load_collections_by_contract()
            .await
            .map_err(internal_status)?;
        let collection_id_filter = if req.collection_id.is_empty() {
            None
        } else {
            Some(felt_hex(
                felt_from_bytes(&req.collection_id).map_err(internal_status)?,
            ))
        };
        let collection_contract_filter = if req.collection_contract.is_empty() {
            None
        } else {
            let felt = felt_from_bytes(&req.collection_contract).map_err(internal_status)?;
            let hex = felt_hex(felt);
            collections_by_contract
                .get(&hex)
                .map(|record| record.collection_id.clone())
        };

        let mut builder: QueryBuilder<Any> = QueryBuilder::new(
            "SELECT order_id, entity_id, collection_id, token_id, currency, seller, buyer, price, quantity, expiration, status, category, royalties, time FROM torii_arcade_sales WHERE 1=1",
        );
        if let Some(collection_id) = collection_id_filter.or(collection_contract_filter) {
            builder
                .push(" AND collection_id = ")
                .push_bind(collection_id);
        }
        if !req.account.is_empty() {
            let account = felt_hex(felt_from_bytes(&req.account).map_err(internal_status)?);
            builder
                .push(" AND (seller = ")
                .push_bind(account.clone())
                .push(" OR buyer = ")
                .push_bind(account)
                .push(")");
        }
        if req.cursor_order_id > 0 {
            builder
                .push(" AND order_id < ")
                .push_bind(req.cursor_order_id as i64);
        }
        builder.push(" ORDER BY order_id DESC");
        let limit = limit_or_default(req.limit, 100);
        builder.push(" LIMIT ").push_bind((limit + 1) as i64);

        let rows = builder
            .build()
            .fetch_all(&self.state.pool)
            .await
            .map_err(internal_status)?;

        let mut sales = Vec::new();
        let mut next_cursor_order_id = 0_u64;
        for (index, row) in rows.into_iter().enumerate() {
            if index == limit as usize {
                next_cursor_order_id = row.try_get::<i64, _>("order_id").unwrap_or_default() as u64;
                break;
            }
            sales.push(
                sale_from_projection_row(&row, &collections_by_id, &self.state.erc721)
                    .await
                    .map_err(internal_status)?,
            );
        }

        Ok(Response::new(ListSalesResponse {
            sales,
            next_cursor_order_id,
        }))
    }

    async fn get_player_inventory(
        &self,
        request: Request<GetPlayerInventoryRequest>,
    ) -> Result<Response<GetPlayerInventoryResponse>, Status> {
        let req = request.into_inner();
        if req.owner.is_empty() {
            return Err(Status::invalid_argument("owner is required"));
        }
        let owner = felt_from_bytes(&req.owner).map_err(internal_status)?;
        let collections_by_contract = self
            .load_collections_by_contract()
            .await
            .map_err(internal_status)?;
        let token_filter = if req.collection_contract.is_empty() {
            collections_by_contract
                .keys()
                .map(|value| felt_from_hex(value))
                .collect::<Result<Vec<_>>>()
                .map_err(internal_status)?
        } else {
            vec![felt_from_bytes(&req.collection_contract).map_err(internal_status)?]
        };
        let cursor = if req.cursor_block_number > 0 || req.cursor_id > 0 {
            Some(OwnershipCursor {
                block_number: req.cursor_block_number,
                id: req.cursor_id,
            })
        } else {
            None
        };
        let limit = limit_or_default(req.limit, 100);
        let (ownership, next_cursor) = self
            .state
            .erc721
            .get_ownership_by_owner(owner, &token_filter, cursor, limit)
            .await
            .map_err(internal_status)?;

        let mut items = Vec::with_capacity(ownership.len());
        for item in ownership {
            let collection = collections_by_contract
                .get(&felt_hex(item.token))
                .cloned()
                .unwrap_or(CollectionRecord {
                    collection_id: felt_hex(item.token),
                    uuid: None,
                    contract_address: felt_hex(item.token),
                });
            let metadata = self
                .state
                .erc721
                .get_token_metadata(item.token)
                .await
                .map_err(internal_status)?;
            let (name, symbol, _) = metadata.unwrap_or((None, None, None));
            items.push(InventoryItem {
                collection: Some(Collection {
                    collection_id: felt_from_hex(&collection.collection_id)
                        .map_err(internal_status)?
                        .to_bytes_be()
                        .to_vec(),
                    uuid: collection
                        .uuid
                        .as_deref()
                        .and_then(|value| felt_from_hex(value).ok())
                        .map(|felt| felt.to_bytes_be().to_vec())
                        .unwrap_or_default(),
                    contract_address: felt_from_hex(&collection.contract_address)
                        .map_err(internal_status)?
                        .to_bytes_be()
                        .to_vec(),
                    name: name.unwrap_or_default(),
                    symbol: symbol.unwrap_or_default(),
                }),
                token_id: u256_to_bytes(item.token_id),
                owner: item.owner.to_bytes_be().to_vec(),
                last_block: item.block_number,
            });
        }

        Ok(Response::new(GetPlayerInventoryResponse {
            items,
            next_cursor_block_number: next_cursor.map_or(0, |cursor| cursor.block_number),
            next_cursor_id: next_cursor.map_or(0, |cursor| cursor.id),
        }))
    }
}

fn projection_schema_sql() -> &'static [&'static str] {
    &[
        "CREATE TABLE IF NOT EXISTS torii_arcade_games (
            game_id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            name TEXT,
            description TEXT,
            published BIGINT NOT NULL DEFAULT 0,
            whitelisted BIGINT NOT NULL DEFAULT 0,
            color TEXT,
            image TEXT,
            external_url TEXT,
            animation_url TEXT,
            youtube_url TEXT,
            attributes_json TEXT,
            properties_json TEXT,
            socials_json TEXT,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_games_visibility_idx
            ON torii_arcade_games(published, whitelisted)",
        "CREATE TABLE IF NOT EXISTS torii_arcade_editions (
            edition_id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            game_id TEXT NOT NULL,
            world_address TEXT,
            namespace TEXT,
            name TEXT,
            description TEXT,
            published BIGINT NOT NULL DEFAULT 0,
            whitelisted BIGINT NOT NULL DEFAULT 0,
            priority BIGINT NOT NULL DEFAULT 0,
            config_json TEXT,
            color TEXT,
            image TEXT,
            external_url TEXT,
            animation_url TEXT,
            youtube_url TEXT,
            attributes_json TEXT,
            properties_json TEXT,
            socials_json TEXT,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_editions_game_idx
            ON torii_arcade_editions(game_id, published, whitelisted, priority DESC)",
        "DROP TABLE IF EXISTS torii_arcade_collections",
        "CREATE TABLE IF NOT EXISTS torii_arcade_collections (
            entity_id TEXT PRIMARY KEY,
            collection_entity_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            edition_id TEXT,
            uuid TEXT,
            contract_address TEXT NOT NULL,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_collections_contract_idx
            ON torii_arcade_collections(contract_address)",
        "CREATE INDEX IF NOT EXISTS torii_arcade_collections_collection_idx
            ON torii_arcade_collections(collection_id)",
        "CREATE INDEX IF NOT EXISTS torii_arcade_collections_edition_idx
            ON torii_arcade_collections(edition_id)",
        "CREATE TABLE IF NOT EXISTS torii_arcade_listings (
            order_id BIGINT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            owner TEXT NOT NULL,
            price TEXT NOT NULL,
            quantity TEXT NOT NULL,
            expiration BIGINT NOT NULL,
            status BIGINT NOT NULL,
            category BIGINT NOT NULL,
            royalties BIGINT NOT NULL,
            time BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_listings_market_idx
            ON torii_arcade_listings(collection_id, status, order_id DESC)",
        "CREATE INDEX IF NOT EXISTS torii_arcade_listings_owner_idx
            ON torii_arcade_listings(owner, order_id DESC)",
        "CREATE TABLE IF NOT EXISTS torii_arcade_sales (
            order_id BIGINT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            seller TEXT NOT NULL,
            buyer TEXT NOT NULL,
            price TEXT NOT NULL,
            quantity TEXT NOT NULL,
            expiration BIGINT NOT NULL,
            status BIGINT NOT NULL,
            category BIGINT NOT NULL,
            royalties BIGINT NOT NULL,
            time BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS torii_arcade_sales_market_idx
            ON torii_arcade_sales(collection_id, order_id DESC)",
        "CREATE INDEX IF NOT EXISTS torii_arcade_sales_account_idx
            ON torii_arcade_sales(seller, buyer, order_id DESC)",
    ]
}

async fn listing_from_projection_row(
    row: &sqlx::any::AnyRow,
    collections_by_id: &HashMap<String, CollectionRecord>,
    erc721: &Arc<Erc721Storage>,
) -> Result<MarketplaceListing> {
    let collection_id = row_string(row, "collection_id")?;
    let collection = collection_proto(
        collections_by_id.get(&collection_id).cloned(),
        erc721,
        &collection_id,
    )
    .await?;
    Ok(MarketplaceListing {
        order_id: row.try_get::<i64, _>("order_id")? as u64,
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        collection: Some(collection),
        token_id: decimal_to_bytes(&row_string(row, "token_id")?)?,
        currency: felt_from_hex(&row_string(row, "currency")?)?
            .to_bytes_be()
            .to_vec(),
        owner: felt_from_hex(&row_string(row, "owner")?)?
            .to_bytes_be()
            .to_vec(),
        price: row_string(row, "price")?,
        quantity: row_string(row, "quantity")?,
        expiration: row.try_get::<i64, _>("expiration")? as u64,
        status: row.try_get::<i64, _>("status")? as u32,
        category: row.try_get::<i64, _>("category")? as u32,
        royalties: row.try_get::<i64, _>("royalties")? != 0,
        time: row.try_get::<i64, _>("time")? as u64,
    })
}

async fn sale_from_projection_row(
    row: &sqlx::any::AnyRow,
    collections_by_id: &HashMap<String, CollectionRecord>,
    erc721: &Arc<Erc721Storage>,
) -> Result<MarketplaceSale> {
    let collection_id = row_string(row, "collection_id")?;
    let collection = collection_proto(
        collections_by_id.get(&collection_id).cloned(),
        erc721,
        &collection_id,
    )
    .await?;
    Ok(MarketplaceSale {
        order_id: row.try_get::<i64, _>("order_id")? as u64,
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        collection: Some(collection),
        token_id: decimal_to_bytes(&row_string(row, "token_id")?)?,
        currency: felt_from_hex(&row_string(row, "currency")?)?
            .to_bytes_be()
            .to_vec(),
        seller: felt_from_hex(&row_string(row, "seller")?)?
            .to_bytes_be()
            .to_vec(),
        buyer: felt_from_hex(&row_string(row, "buyer")?)?
            .to_bytes_be()
            .to_vec(),
        price: row_string(row, "price")?,
        quantity: row_string(row, "quantity")?,
        expiration: row.try_get::<i64, _>("expiration")? as u64,
        status: row.try_get::<i64, _>("status")? as u32,
        category: row.try_get::<i64, _>("category")? as u32,
        royalties: row.try_get::<i64, _>("royalties")? != 0,
        time: row.try_get::<i64, _>("time")? as u64,
    })
}

async fn collection_proto(
    record: Option<CollectionRecord>,
    erc721: &Arc<Erc721Storage>,
    fallback_collection_id: &str,
) -> Result<Collection> {
    let record = record.unwrap_or(CollectionRecord {
        collection_id: fallback_collection_id.to_string(),
        uuid: None,
        contract_address: fallback_collection_id.to_string(),
    });
    let token = felt_from_hex(&record.contract_address)?;
    let metadata = erc721.get_token_metadata(token).await?;
    let (name, symbol, _) = metadata.unwrap_or((None, None, None));
    Ok(Collection {
        collection_id: felt_from_hex(&record.collection_id)?.to_bytes_be().to_vec(),
        uuid: record
            .uuid
            .as_deref()
            .and_then(|value| felt_from_hex(value).ok())
            .map(|felt| felt.to_bytes_be().to_vec())
            .unwrap_or_default(),
        contract_address: token.to_bytes_be().to_vec(),
        name: name.unwrap_or_default(),
        symbol: symbol.unwrap_or_default(),
    })
}

fn game_from_row(row: &sqlx::any::AnyRow) -> Result<Game> {
    Ok(Game {
        game_id: felt_from_hex(&row_string(row, "game_id")?)?
            .to_bytes_be()
            .to_vec(),
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        name: row
            .try_get::<Option<String>, _>("name")?
            .unwrap_or_default(),
        description: row
            .try_get::<Option<String>, _>("description")?
            .unwrap_or_default(),
        published: row.try_get::<i64, _>("published")? != 0,
        whitelisted: row.try_get::<i64, _>("whitelisted")? != 0,
        color: row
            .try_get::<Option<String>, _>("color")?
            .unwrap_or_default(),
        image: row
            .try_get::<Option<String>, _>("image")?
            .unwrap_or_default(),
        external_url: row
            .try_get::<Option<String>, _>("external_url")?
            .unwrap_or_default(),
        animation_url: row
            .try_get::<Option<String>, _>("animation_url")?
            .unwrap_or_default(),
        youtube_url: row
            .try_get::<Option<String>, _>("youtube_url")?
            .unwrap_or_default(),
        attributes_json: row
            .try_get::<Option<String>, _>("attributes_json")?
            .unwrap_or_default(),
        properties_json: row
            .try_get::<Option<String>, _>("properties_json")?
            .unwrap_or_default(),
        socials_json: row
            .try_get::<Option<String>, _>("socials_json")?
            .unwrap_or_default(),
    })
}

fn edition_from_row(row: &sqlx::any::AnyRow) -> Result<Edition> {
    Ok(Edition {
        edition_id: felt_from_hex(&row_string(row, "edition_id")?)?
            .to_bytes_be()
            .to_vec(),
        entity_id: felt_from_hex(&row_string(row, "entity_id")?)?
            .to_bytes_be()
            .to_vec(),
        game_id: felt_from_hex(&row_string(row, "game_id")?)?
            .to_bytes_be()
            .to_vec(),
        world_address: row
            .try_get::<Option<String>, _>("world_address")?
            .as_deref()
            .and_then(|value| felt_from_hex(value).ok())
            .map(|felt| felt.to_bytes_be().to_vec())
            .unwrap_or_default(),
        namespace: row
            .try_get::<Option<String>, _>("namespace")?
            .as_deref()
            .and_then(|value| felt_from_hex(value).ok())
            .map(|felt| felt.to_bytes_be().to_vec())
            .unwrap_or_default(),
        name: row
            .try_get::<Option<String>, _>("name")?
            .unwrap_or_default(),
        description: row
            .try_get::<Option<String>, _>("description")?
            .unwrap_or_default(),
        published: row.try_get::<i64, _>("published")? != 0,
        whitelisted: row.try_get::<i64, _>("whitelisted")? != 0,
        priority: row.try_get::<i64, _>("priority")? as u32,
        config_json: row
            .try_get::<Option<String>, _>("config_json")?
            .unwrap_or_default(),
        color: row
            .try_get::<Option<String>, _>("color")?
            .unwrap_or_default(),
        image: row
            .try_get::<Option<String>, _>("image")?
            .unwrap_or_default(),
        external_url: row
            .try_get::<Option<String>, _>("external_url")?
            .unwrap_or_default(),
        animation_url: row
            .try_get::<Option<String>, _>("animation_url")?
            .unwrap_or_default(),
        youtube_url: row
            .try_get::<Option<String>, _>("youtube_url")?
            .unwrap_or_default(),
        attributes_json: row
            .try_get::<Option<String>, _>("attributes_json")?
            .unwrap_or_default(),
        properties_json: row
            .try_get::<Option<String>, _>("properties_json")?
            .unwrap_or_default(),
        socials_json: row
            .try_get::<Option<String>, _>("socials_json")?
            .unwrap_or_default(),
    })
}

fn parse_listing_row(row: &sqlx::any::AnyRow) -> Result<ListingRecord> {
    if row.try_get_raw("order").is_ok() {
        let entity_id = row_felt_hex(row, "entity_id")?;
        let order_value = serde_json::from_str::<Value>(&row_string(row, "order")?)?;
        return Ok(ListingRecord {
            order_id: row_u64(row, "order_id")?,
            entity_id,
            collection_id: json_felt_string(&order_value, "collection")?,
            token_id: json_string(&order_value, "token_id")?,
            currency: json_felt_string(&order_value, "currency")?,
            owner: json_felt_string(&order_value, "owner")?,
            price: json_number_string(&order_value, "price")?,
            quantity: json_number_string(&order_value, "quantity")?,
            expiration: json_u64(&order_value, "expiration")?,
            status: json_u64(&order_value, "status")? as u32,
            category: json_u64(&order_value, "category")? as u32,
            royalties: json_bool(&order_value, "royalties")?,
            time: row_opt_u64(row, "time")?.unwrap_or_default(),
        });
    }

    Ok(ListingRecord {
        order_id: row_u64(row, "id")?,
        entity_id: row_felt_hex(row, "entity_id")?,
        collection_id: row_felt_hex(row, "collection")?,
        token_id: row_string(row, "token_id")?,
        currency: row_felt_hex(row, "currency")?,
        owner: row_felt_hex(row, "owner")?,
        price: row_string(row, "price")?,
        quantity: row_string(row, "quantity")?,
        expiration: row_u64(row, "expiration")?,
        status: row_u64(row, "status")? as u32,
        category: row_u64(row, "category")? as u32,
        royalties: row_bool_i64(row, "royalties")? != 0,
        time: row_opt_u64(row, "time")?.unwrap_or_default(),
    })
}

fn parse_sale_row(row: &sqlx::any::AnyRow) -> Result<SaleRecord> {
    let entity_id = row_felt_hex(row, "entity_id")?;
    let order_value = serde_json::from_str::<Value>(&row_string(row, "order")?)?;
    Ok(SaleRecord {
        order_id: row_u64(row, "order_id")?,
        entity_id,
        collection_id: json_felt_string(&order_value, "collection")?,
        token_id: json_string(&order_value, "token_id")?,
        currency: json_felt_string(&order_value, "currency")?,
        seller: row_felt_hex(row, "from")?,
        buyer: row_felt_hex(row, "to")?,
        price: json_number_string(&order_value, "price")?,
        quantity: json_number_string(&order_value, "quantity")?,
        expiration: json_u64(&order_value, "expiration")?,
        status: json_u64(&order_value, "status")? as u32,
        category: json_u64(&order_value, "category")? as u32,
        royalties: json_bool(&order_value, "royalties")?,
        time: row_u64(row, "time")?,
    })
}

fn select_by_entity_match_sql(backend: DbBackend, table: &str, statement: &str) -> String {
    match backend {
        DbBackend::Sqlite => {
            format!(r#"{statement} "{table}" WHERE entity_id = ?1 OR entity_id = ?2"#)
        }
        DbBackend::Postgres => {
            format!(r#"{statement} "{table}" WHERE entity_id = $1 OR entity_id = $2"#)
        }
    }
}

fn select_by_field_match_sql(
    backend: DbBackend,
    table: &str,
    field: &str,
    statement: &str,
) -> String {
    match backend {
        DbBackend::Sqlite => {
            format!(r#"{statement} "{table}" WHERE "{field}" = ?1 OR "{field}" = ?2"#)
        }
        DbBackend::Postgres => {
            format!(r#"{statement} "{table}" WHERE "{field}" = $1 OR "{field}" = $2"#)
        }
    }
}

fn limit_or_default(limit: u32, default: u32) -> u32 {
    if limit == 0 {
        default
    } else {
        limit.min(500)
    }
}

fn row_string(row: &sqlx::any::AnyRow, column: &str) -> Result<String> {
    row.try_get::<String, _>(column)
        .map_err(|err| anyhow!("failed to read {column}: {err}"))
}

fn row_felt_hex(row: &sqlx::any::AnyRow, column: &str) -> Result<String> {
    if let Ok(bytes) = row.try_get::<Vec<u8>, _>(column) {
        return Ok(felt_hex(felt_from_bytes(&bytes)?));
    }

    let value = row
        .try_get::<String, _>(column)
        .map_err(|err| anyhow!("failed to read {column} as felt bytes or string: {err}"))?;
    let felt = felt_from_hex(&value)
        .or_else(|_| Felt::from_hex(&value))
        .map_err(|err| anyhow!("failed to parse {column} as felt: {err}"))?;
    Ok(felt_hex(felt))
}

fn row_opt_string(row: &sqlx::any::AnyRow, column: &str) -> Result<Option<String>> {
    row.try_get::<Option<String>, _>(column)
        .map_err(|err| anyhow!("failed to read {column}: {err}"))
}

fn row_opt_felt_hex(row: &sqlx::any::AnyRow, column: &str) -> Result<Option<String>> {
    if row.try_get_raw(column).is_err() {
        return Ok(None);
    }
    let value = row_opt_string(row, column)?;
    value
        .map(|raw| {
            let felt = felt_from_hex(&raw)
                .or_else(|_| Felt::from_hex(&raw))
                .map_err(|err| anyhow!("failed to parse {column} as felt: {err}"))?;
            Ok(felt_hex(felt))
        })
        .transpose()
}

fn row_u64(row: &sqlx::any::AnyRow, column: &str) -> Result<u64> {
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Ok(value as u64);
    }
    if let Ok(value) = row.try_get::<String, _>(column) {
        return value
            .parse::<u64>()
            .map_err(|err| anyhow!("failed to parse {column}: {err}"));
    }
    Err(anyhow!("failed to read {column} as u64"))
}

fn row_opt_u64(row: &sqlx::any::AnyRow, column: &str) -> Result<Option<u64>> {
    if row.try_get_raw(column).is_err() {
        return Ok(None);
    }
    if let Ok(value) = row.try_get::<Option<i64>, _>(column) {
        return Ok(value.map(|value| value as u64));
    }
    if let Ok(value) = row.try_get::<Option<String>, _>(column) {
        return value
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|err| anyhow!("failed to parse {column}: {err}"))
            })
            .transpose();
    }
    Err(anyhow!("failed to read {column} as optional u64"))
}

fn row_bool_i64(row: &sqlx::any::AnyRow, column: &str) -> Result<i64> {
    if let Ok(value) = row.try_get::<bool, _>(column) {
        return Ok(i64::from(value));
    }
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<String, _>(column) {
        let normalized = value.trim().to_ascii_lowercase();
        return Ok(match normalized.as_str() {
            "true" | "t" | "1" | "yes" | "y" => 1,
            _ => 0,
        });
    }
    Err(anyhow!("failed to read {column} as bool"))
}

fn json_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("missing string field {key}"))
}

fn json_felt_string(value: &Value, key: &str) -> Result<String> {
    let raw = json_string(value, key)?;
    let felt = felt_from_hex(&raw)
        .or_else(|_| Felt::from_hex(&raw))
        .map_err(|err| anyhow!("invalid felt field {key}: {err}"))?;
    Ok(felt_hex(felt))
}

fn json_number_string(value: &Value, key: &str) -> Result<String> {
    if let Some(raw) = value.get(key) {
        if let Some(string) = raw.as_str() {
            return Ok(string.to_string());
        }
        return Ok(raw.to_string());
    }
    Err(anyhow!("missing numeric field {key}"))
}

fn json_u64(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing u64 field {key}"))
}

fn json_bool(value: &Value, key: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("missing bool field {key}"))
}

fn felt_hex(value: Felt) -> String {
    format!("{value:#x}")
}

fn felt_hex_padded(value: Felt) -> String {
    format!("{value:#066x}")
}

fn felt_from_hex(value: &str) -> Result<Felt> {
    Felt::from_str(value).map_err(|err| anyhow!("invalid felt {value}: {err}"))
}

fn felt_from_bytes(value: &[u8]) -> Result<Felt> {
    if value.len() > 32 {
        return Err(anyhow!("felt bytes exceed 32 bytes"));
    }

    let mut padded = [0u8; 32];
    padded[32 - value.len()..].copy_from_slice(value);
    Ok(Felt::from_bytes_be(&padded))
}

fn decimal_to_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("cannot parse empty decimal value"));
    }

    let mut bytes = [0_u8; 32];
    for digit in value.bytes() {
        if !digit.is_ascii_digit() {
            return Err(anyhow!("invalid decimal value {value}"));
        }

        let mut carry = u16::from(digit - b'0');
        for byte in bytes.iter_mut().rev() {
            let acc = u16::from(*byte) * 10 + carry;
            *byte = (acc & 0xff) as u8;
            carry = acc >> 8;
        }

        if carry != 0 {
            return Err(anyhow!("decimal value overflows 256 bits: {value}"));
        }
    }

    let value: U256 = blob_to_u256(&bytes);
    Ok(u256_to_blob(value))
}

fn u256_to_bytes(value: U256) -> Vec<u8> {
    u256_to_blob(value)
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

fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;
    use tempfile::tempdir;
    use tonic::Request;

    #[tokio::test]
    async fn bootstrap_from_current_arcade_sqlite_schema_populates_projections() {
        let temp_dir = tempdir().expect("tempdir");
        let source_db = temp_dir.path().join("source.db");
        let erc721_db = temp_dir.path().join("erc721.db");
        let source_url = source_db.to_string_lossy().to_string();
        let source_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&format!("sqlite://{source_url}"))
                    .expect("sqlite connect options")
                    .create_if_missing(true),
            )
            .await
            .expect("connect source");

        sqlx::query(
            r#"CREATE TABLE "ARCADE-Game" (
                "entity_id" TEXT PRIMARY KEY,
                "id" TEXT,
                "published" TEXT,
                "whitelisted" TEXT,
                "color" TEXT,
                "image" TEXT,
                "image_data" TEXT,
                "external_url" TEXT,
                "description" TEXT,
                "name" TEXT,
                "animation_url" TEXT,
                "youtube_url" TEXT,
                "attributes" TEXT,
                "properties" TEXT,
                "socials" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await
        .expect("create game table");

        sqlx::query(
            r#"CREATE TABLE "ARCADE-Edition" (
                "entity_id" TEXT PRIMARY KEY,
                "id" TEXT,
                "game_id" TEXT,
                "world_address" TEXT,
                "namespace" TEXT,
                "name" TEXT,
                "description" TEXT,
                "published" TEXT,
                "whitelisted" TEXT,
                "priority" TEXT,
                "config" TEXT,
                "color" TEXT,
                "image" TEXT,
                "external_url" TEXT,
                "animation_url" TEXT,
                "youtube_url" TEXT,
                "attributes" TEXT,
                "properties" TEXT,
                "socials" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await
        .expect("create edition table");

        sqlx::query(
            r#"CREATE TABLE "ARCADE-Collection" (
                "entity_id" TEXT PRIMARY KEY,
                "id" TEXT,
                "uuid" TEXT,
                "contract_address" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await
        .expect("create collection table");

        sqlx::query(
            r#"CREATE TABLE "ARCADE-CollectionEdition" (
                "entity_id" TEXT PRIMARY KEY,
                "collection" TEXT,
                "edition" TEXT,
                "active" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await
        .expect("create collection edition table");

        sqlx::query(
            r#"CREATE TABLE "ARCADE-Order" (
                "entity_id" TEXT PRIMARY KEY,
                "id" TEXT,
                "collection" TEXT,
                "token_id" TEXT,
                "royalties" TEXT,
                "category" TEXT,
                "status" TEXT,
                "expiration" TEXT,
                "quantity" TEXT,
                "price" TEXT,
                "currency" TEXT,
                "owner" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await
        .expect("create order table");

        sqlx::query(
            r#"INSERT INTO "ARCADE-Game" (
                entity_id, id, published, whitelisted, color, image, external_url, description,
                name, animation_url, youtube_url, attributes, properties, socials
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"#,
        )
        .bind("0x004533cf8322e4e8109cf9479dfb8d5425a5b33b2e8ea09b780760b6ebefaf1c")
        .bind("0x0000000000000000000000000000000000000000000000000000000000000001")
        .bind("true")
        .bind("true")
        .bind("#ffffff")
        .bind("https://example.com/game.png")
        .bind("https://example.com/game")
        .bind("Example game")
        .bind("Loot Survivor")
        .bind("https://example.com/game.mp4")
        .bind("https://example.com/game.mp4")
        .bind("[]")
        .bind("{}")
        .bind("{}")
        .execute(&source_pool)
        .await
        .expect("insert game");

        sqlx::query(
            r#"INSERT INTO "ARCADE-Edition" (
                entity_id, id, game_id, world_address, namespace, name, description, published,
                whitelisted, priority, config, color, image, external_url, animation_url,
                youtube_url, attributes, properties, socials
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)"#,
        )
        .bind("0x0011111111111111111111111111111111111111111111111111111111111111")
        .bind("0x0000000000000000000000000000000000000000000000000000000000000002")
        .bind("0x0000000000000000000000000000000000000000000000000000000000000001")
        .bind("0x0123")
        .bind("0x0456")
        .bind("Edition One")
        .bind("Example edition")
        .bind("true")
        .bind("true")
        .bind("10")
        .bind("{}")
        .bind("#000000")
        .bind("https://example.com/edition.png")
        .bind("https://example.com/edition")
        .bind("")
        .bind("")
        .bind("[]")
        .bind("{}")
        .bind("{}")
        .execute(&source_pool)
        .await
        .expect("insert edition");

        sqlx::query(
            r#"INSERT INTO "ARCADE-Collection" (entity_id, id, uuid, contract_address)
               VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind("0x0022222222222222222222222222222222222222222222222222222222222222")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .bind("0x0999")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .execute(&source_pool)
        .await
        .expect("insert collection");

        sqlx::query(
            r#"INSERT INTO "ARCADE-CollectionEdition" (entity_id, collection, edition, active)
               VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind("0x0033333333333333333333333333333333333333333333333333333333333333")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .bind("0x0000000000000000000000000000000000000000000000000000000000000002")
        .bind("TRUE")
        .execute(&source_pool)
        .await
        .expect("insert collection edition");

        sqlx::query(
            r#"INSERT INTO "ARCADE-Order" (
                entity_id, id, collection, token_id, royalties, category, status, expiration,
                quantity, price, currency, owner
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"#,
        )
        .bind("0x007aebc55e8392be3f8b5140abb8debbc5430b2499f16fa7a4095e6d0e42c40c")
        .bind("1")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .bind("16345572918946916709028395658032763347530533577384345871581184")
        .bind("false")
        .bind("2")
        .bind("1")
        .bind("0")
        .bind("1")
        .bind("1000000000000000000")
        .bind("0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d")
        .bind("0x06b0fda70d073743a3b0f6b02cbc1ab5c1c85f2a36d7c3c17ca5dbf8ea0883a0")
        .execute(&source_pool)
        .await
        .expect("insert order");

        drop(source_pool);

        let service = ArcadeService::new(&source_url, &erc721_db.to_string_lossy(), Some(1))
            .await
            .expect("create arcade service");

        let game_count: i64 = sqlx::query_scalar("SELECT count(*) FROM torii_arcade_games")
            .fetch_one(&service.state.pool)
            .await
            .expect("count games");
        let edition_count: i64 = sqlx::query_scalar("SELECT count(*) FROM torii_arcade_editions")
            .fetch_one(&service.state.pool)
            .await
            .expect("count editions");
        let collection_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM torii_arcade_collections")
                .fetch_one(&service.state.pool)
                .await
                .expect("count collections");
        let listing_count: i64 = sqlx::query_scalar("SELECT count(*) FROM torii_arcade_listings")
            .fetch_one(&service.state.pool)
            .await
            .expect("count listings");

        assert_eq!(game_count, 1);
        assert_eq!(edition_count, 1);
        assert_eq!(collection_count, 1);
        assert_eq!(listing_count, 1);

        let editions = service
            .list_editions(Request::new(ListEditionsRequest {
                game_id: Vec::new(),
                include_unpublished: true,
                include_unwhitelisted: true,
                limit: 10,
                collection_id: felt_from_hex(
                    "0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4",
                )
                .expect("collection id")
                .to_bytes_be()
                .to_vec(),
            }))
            .await
            .expect("list editions")
            .into_inner()
            .editions;
        assert_eq!(editions.len(), 1);
        assert_eq!(
            felt_hex(felt_from_bytes(&editions[0].edition_id).expect("edition id bytes"),),
            "0x2"
        );
    }

    #[tokio::test]
    async fn bootstrap_collection_relations_without_collection_table_populates_projection() {
        let temp_dir = tempdir().expect("tempdir");
        let source_db = temp_dir.path().join("source.db");
        let erc721_db = temp_dir.path().join("erc721.db");
        let source_url = source_db.to_string_lossy().to_string();
        let source_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&format!("sqlite://{source_url}"))
                    .expect("sqlite connect options")
                    .create_if_missing(true),
            )
            .await
            .expect("connect source");

        sqlx::query(
            r#"CREATE TABLE "ARCADE-CollectionEdition" (
                "entity_id" TEXT PRIMARY KEY,
                "collection" TEXT,
                "edition" TEXT,
                "active" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await
        .expect("create collection edition table");

        sqlx::query(
            r#"INSERT INTO "ARCADE-CollectionEdition" (entity_id, collection, edition, active)
               VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind("0x0033333333333333333333333333333333333333333333333333333333333333")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .bind("0x0000000000000000000000000000000000000000000000000000000000000002")
        .bind("TRUE")
        .execute(&source_pool)
        .await
        .expect("insert collection edition");

        drop(source_pool);

        let service = ArcadeService::new(&source_url, &erc721_db.to_string_lossy(), Some(1))
            .await
            .expect("create arcade service");

        let row = sqlx::query(
            "SELECT entity_id, collection_entity_id, collection_id, edition_id, uuid, contract_address
             FROM torii_arcade_collections",
        )
        .fetch_one(&service.state.pool)
        .await
        .expect("projected collection row");

        let entity_id: String = row.try_get("entity_id").expect("entity_id");
        let collection_entity_id: String = row
            .try_get("collection_entity_id")
            .expect("collection_entity_id");
        let collection_id: String = row.try_get("collection_id").expect("collection_id");
        let edition_id: String = row.try_get("edition_id").expect("edition_id");
        let uuid: Option<String> = row.try_get("uuid").expect("uuid");
        let contract_address: String = row.try_get("contract_address").expect("contract_address");

        assert!(entity_id.starts_with("0x3"));
        assert_eq!(collection_entity_id, entity_id);
        assert_eq!(
            collection_id,
            "0x46da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4"
        );
        assert_eq!(edition_id, "0x2");
        assert_eq!(uuid, None);
        assert_eq!(contract_address, collection_id);
    }
}
