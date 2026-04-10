use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::Felt;
use torii::axum::Router;
use torii::etl::envelope::{Envelope, TypeId};
use torii::etl::extractor::ExtractionBatch;
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};

use crate::grpc_service::ArcadeService;

pub struct ArcadeSink {
    service: Arc<ArcadeService>,
    tracked_tables: RwLock<HashMap<String, TrackedTable>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectionOp {
    Refresh(TrackedTable, Felt),
    Delete(TrackedTable, Felt),
}

#[derive(Debug, Default)]
struct ProjectionBatchPlan {
    tracked_tables: HashMap<String, TrackedTable>,
    projection_ops: Vec<ProjectionOp>,
    reload_tracked_tables: bool,
    requires_full_rebuild: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrackedTable {
    Game,
    Edition,
    Collection,
    Listing,
    Sale,
}

impl TrackedTable {
    fn known_table_ids() -> &'static [(&'static str, Self)] {
        &[
            (
                "0x6143bc86ed1a08df992c568392c454a92ef7e7b5ba08e9bf75643cf5cfc8b14",
                Self::Game,
            ),
            (
                "0x76002f6d86762c47f4c4004d8ca9d8c2cb82c3929b5247a19e4551c47fd0a2c",
                Self::Edition,
            ),
            (
                "0x61ac61b99b1ae4cb2f06c53e9f5cb7c9ef7638a67d970280f138993d2ddbaa",
                Self::Collection,
            ),
            (
                "0x3f3039bdaaf02188f4b9d7e611fcecf93dde076f0e998c32738f30d6b812166",
                Self::Collection,
            ),
            (
                "0x2b375547a6102a593675b82dc24b7ee906423985424792436866a3a3d3b194",
                Self::Listing,
            ),
            (
                "0x49a3252a2f65bab4482e38d0667850e31e0baa69416eab3d825c4a27cf31b2a",
                Self::Listing,
            ),
            (
                "0x2641babd55028e5173f25aec79c7481d2625d36a6856060fa02b0818ca8a199",
                Self::Sale,
            ),
        ]
    }

    fn from_table_name(table_name: &str) -> Option<Self> {
        match table_name {
            "ARCADE-Game" => Some(Self::Game),
            "ARCADE-Edition" => Some(Self::Edition),
            "ARCADE-Collection" | "ARCADE-CollectionEdition" => Some(Self::Collection),
            "ARCADE-Order" | "ARCADE-Listing" => Some(Self::Listing),
            "ARCADE-Sale" => Some(Self::Sale),
            _ => None,
        }
    }

    async fn refresh(self, service: &ArcadeService, entity_id: Felt) -> Result<()> {
        match self {
            Self::Game => service.refresh_game(entity_id).await,
            Self::Edition => service.refresh_edition(entity_id).await,
            Self::Collection => service.refresh_collection(entity_id).await,
            Self::Listing => service.refresh_listing(entity_id).await,
            Self::Sale => service.refresh_sale(entity_id).await,
        }
    }

    async fn delete(self, service: &ArcadeService, entity_id: Felt) -> Result<()> {
        match self {
            Self::Game => service.delete_game(entity_id).await,
            Self::Edition => service.delete_edition(entity_id).await,
            Self::Collection => service.delete_collection(entity_id).await,
            Self::Listing => service.delete_listing(entity_id).await,
            Self::Sale => service.delete_sale(entity_id).await,
        }
    }
}

impl ArcadeSink {
    pub async fn new(
        database_url: &str,
        erc721_database_url: &str,
        max_connections: Option<u32>,
    ) -> Result<Self> {
        let service =
            Arc::new(ArcadeService::new(database_url, erc721_database_url, max_connections).await?);
        let tracked_tables = Self::load_tracked_tables(service.as_ref()).await?;

        Ok(Self {
            service,
            tracked_tables: RwLock::new(tracked_tables),
        })
    }

    pub fn get_grpc_service_impl(&self) -> Arc<ArcadeService> {
        self.service.clone()
    }

    async fn load_tracked_tables(service: &ArcadeService) -> Result<HashMap<String, TrackedTable>> {
        let mut tracked_tables = TrackedTable::known_table_ids()
            .iter()
            .copied()
            .map(|(table_id, tracked_table)| (table_id.to_string(), tracked_table))
            .collect::<HashMap<_, _>>();
        tracked_tables.extend(
            service
                .load_tracked_table_names_by_id()
                .await?
                .into_iter()
                .filter_map(|(table_id, table_name)| {
                    TrackedTable::from_table_name(&table_name).map(|tracked| (table_id, tracked))
                }),
        );
        Ok(tracked_tables)
    }

    fn tracked_tables_snapshot(&self) -> HashMap<String, TrackedTable> {
        self.tracked_tables
            .read()
            .expect("tracked table map lock poisoned")
            .clone()
    }

    fn replace_tracked_tables(&self, tracked_tables: HashMap<String, TrackedTable>) {
        *self
            .tracked_tables
            .write()
            .expect("tracked table map lock poisoned") = tracked_tables;
    }

    fn update_tracked_table(
        tracked_tables: &mut HashMap<String, TrackedTable>,
        table_id: Felt,
        table_name: &str,
    ) {
        let table_id = format!("{table_id:#x}");
        if let Some(tracked_table) = TrackedTable::from_table_name(table_name) {
            tracked_tables.insert(table_id, tracked_table);
        } else {
            tracked_tables.remove(&table_id);
        }
    }

    fn tracked_table_for_id(
        tracked_tables: &HashMap<String, TrackedTable>,
        table_id: Felt,
    ) -> Option<TrackedTable> {
        tracked_tables.get(&format!("{table_id:#x}")).copied()
    }

    fn is_schema_rebuild_msg(
        tracked_tables: &HashMap<String, TrackedTable>,
        msg: &IntrospectMsg,
    ) -> bool {
        match msg {
            IntrospectMsg::CreateTable(table) => {
                TrackedTable::from_table_name(&table.name).is_some()
                    || Self::tracked_table_for_id(tracked_tables, table.id).is_some()
            }
            IntrospectMsg::UpdateTable(table) => {
                TrackedTable::from_table_name(&table.name).is_some()
                    || Self::tracked_table_for_id(tracked_tables, table.id).is_some()
            }
            IntrospectMsg::RenameTable(table) => {
                TrackedTable::from_table_name(&table.name).is_some()
                    || Self::tracked_table_for_id(tracked_tables, table.id).is_some()
            }
            IntrospectMsg::DropTable(table) => {
                Self::tracked_table_for_id(tracked_tables, table.id).is_some()
            }
            IntrospectMsg::AddColumns(event) => {
                Self::tracked_table_for_id(tracked_tables, event.table).is_some()
            }
            IntrospectMsg::DropColumns(event) => {
                Self::tracked_table_for_id(tracked_tables, event.table).is_some()
            }
            IntrospectMsg::RetypeColumns(event) => {
                Self::tracked_table_for_id(tracked_tables, event.table).is_some()
            }
            IntrospectMsg::RetypePrimary(event) => {
                Self::tracked_table_for_id(tracked_tables, event.table).is_some()
            }
            IntrospectMsg::RenameColumns(event) => {
                Self::tracked_table_for_id(tracked_tables, event.table).is_some()
            }
            IntrospectMsg::RenamePrimary(event) => {
                Self::tracked_table_for_id(tracked_tables, event.table).is_some()
            }
            _ => false,
        }
    }

    fn build_batch_plan(&self, envelopes: &[Envelope]) -> ProjectionBatchPlan {
        Self::build_batch_plan_from_tables(self.tracked_tables_snapshot(), envelopes)
    }

    fn build_batch_plan_from_tables(
        tracked_tables: HashMap<String, TrackedTable>,
        envelopes: &[Envelope],
    ) -> ProjectionBatchPlan {
        let mut plan = ProjectionBatchPlan {
            tracked_tables,
            ..ProjectionBatchPlan::default()
        };

        for envelope in envelopes {
            if envelope.type_id != TypeId::new("introspect") {
                continue;
            }

            let Some(body) = envelope.downcast_ref::<IntrospectBody>() else {
                continue;
            };

            if Self::is_schema_rebuild_msg(&plan.tracked_tables, &body.msg) {
                plan.requires_full_rebuild = true;
            }

            match &body.msg {
                IntrospectMsg::CreateTable(table) => {
                    Self::update_tracked_table(&mut plan.tracked_tables, table.id, &table.name);
                    plan.reload_tracked_tables = true;
                }
                IntrospectMsg::UpdateTable(table) => {
                    Self::update_tracked_table(&mut plan.tracked_tables, table.id, &table.name);
                    plan.reload_tracked_tables = true;
                }
                IntrospectMsg::RenameTable(table) => {
                    Self::update_tracked_table(&mut plan.tracked_tables, table.id, &table.name);
                    plan.reload_tracked_tables = true;
                }
                IntrospectMsg::DropTable(table) => {
                    plan.tracked_tables.remove(&format!("{:#x}", table.id));
                    plan.reload_tracked_tables = true;
                }
                IntrospectMsg::InsertsFields(insert) => {
                    if let Some(tracked_table) =
                        Self::tracked_table_for_id(&plan.tracked_tables, insert.table)
                    {
                        for record in &insert.records {
                            let entity_id = Felt::from_bytes_be_slice(&record.id);
                            plan.projection_ops
                                .push(ProjectionOp::Refresh(tracked_table, entity_id));
                        }
                    }
                }
                IntrospectMsg::DeletesFields(delete) => {
                    if let Some(tracked_table) =
                        Self::tracked_table_for_id(&plan.tracked_tables, delete.table)
                    {
                        for row in &delete.rows {
                            let entity_id = row.to_felt();
                            plan.projection_ops
                                .push(ProjectionOp::Refresh(tracked_table, entity_id));
                        }
                    }
                }
                IntrospectMsg::DeleteRecords(delete) => {
                    if let Some(tracked_table) =
                        Self::tracked_table_for_id(&plan.tracked_tables, delete.table)
                    {
                        for row in &delete.rows {
                            let entity_id = row.to_felt();
                            plan.projection_ops
                                .push(ProjectionOp::Delete(tracked_table, entity_id));
                        }
                    }
                }
                _ => {}
            }
        }

        plan
    }
}

#[async_trait]
impl Sink for ArcadeSink {
    fn name(&self) -> &'static str {
        "arcade"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("introspect")]
    }

    async fn process(&self, envelopes: &[Envelope], _batch: &ExtractionBatch) -> Result<()> {
        let mut plan = self.build_batch_plan(envelopes);

        if plan.reload_tracked_tables {
            plan.tracked_tables = Self::load_tracked_tables(self.service.as_ref()).await?;
        }

        self.replace_tracked_tables(plan.tracked_tables);

        if plan.requires_full_rebuild {
            self.service.bootstrap_from_source().await?;
            return Ok(());
        }

        for op in plan.projection_ops {
            match op {
                ProjectionOp::Refresh(tracked_table, entity_id) => {
                    tracked_table
                        .refresh(self.service.as_ref(), entity_id)
                        .await?;
                }
                ProjectionOp::Delete(tracked_table, entity_id) => {
                    tracked_table
                        .delete(self.service.as_ref(), entity_id)
                        .await?;
                }
            }
        }

        Ok(())
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use anyhow::Result;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::Row;
    use starknet::core::types::Felt;
    use tempfile::tempdir;
    use torii::etl::extractor::ExtractionBatch;
    use torii::etl::{Envelope, MetaData};
    use torii_introspect::events::{
        InsertsFields, IntrospectBody, IntrospectMsg, Record, RenamePrimary,
    };

    use super::*;

    const GAME_TABLE_ID: &str = "0x6143bc86ed1a08df992c568392c454a92ef7e7b5ba08e9bf75643cf5cfc8b14";
    const COLLECTION_EDITION_TABLE_ID: &str =
        "0x8c6f6a3efadfd0f4a8408d7ad1b4e37f4f733fe10b8a5ef9a76fd4d8be3b5b";

    fn introspect_envelope(msg: IntrospectMsg) -> Envelope {
        Envelope::from(IntrospectBody {
            context: MetaData {
                block_number: Some(1),
                transaction_hash: Felt::ZERO,
                from_address: Felt::ZERO,
            },
            msg,
        })
    }

    fn tracked_tables() -> HashMap<String, TrackedTable> {
        TrackedTable::known_table_ids()
            .iter()
            .copied()
            .map(|(table_id, tracked_table)| (table_id.to_string(), tracked_table))
            .collect()
    }

    #[tokio::test]
    async fn builds_refresh_ops_for_live_insert_envelopes() -> Result<()> {
        let entity = Felt::from_hex_unchecked("0x201");
        let envelope = introspect_envelope(IntrospectMsg::InsertsFields(InsertsFields {
            table: Felt::from_hex_unchecked(GAME_TABLE_ID),
            columns: vec![],
            records: vec![Record {
                id: entity.to_bytes_be(),
                values: Vec::new(),
            }],
        }));
        let plan = ArcadeSink::build_batch_plan_from_tables(tracked_tables(), &[envelope]);

        assert_eq!(
            plan.projection_ops,
            vec![ProjectionOp::Refresh(
                TrackedTable::Game,
                Felt::from_hex_unchecked("0x201")
            )]
        );
        assert!(!plan.requires_full_rebuild);
        Ok(())
    }

    #[tokio::test]
    async fn marks_tracked_schema_changes_for_full_rebuild() -> Result<()> {
        let plan = ArcadeSink::build_batch_plan_from_tables(
            tracked_tables(),
            &[introspect_envelope(IntrospectMsg::RenamePrimary(
                RenamePrimary {
                    table: Felt::from_hex_unchecked(GAME_TABLE_ID),
                    name: "entity_id".to_string(),
                },
            ))],
        );

        assert!(plan.requires_full_rebuild);
        Ok(())
    }

    #[tokio::test]
    async fn collection_edition_insert_refreshes_collection_projection() -> Result<()> {
        let temp_dir = tempdir()?;
        let source_db = temp_dir.path().join("source.db");
        let erc721_db = temp_dir.path().join("erc721.db");
        let source_url = source_db.to_string_lossy().to_string();
        let source_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&format!("sqlite://{source_url}"))?
                    .create_if_missing(true),
            )
            .await?;

        sqlx::query(
            r#"CREATE TABLE "ARCADE-Collection" (
                "entity_id" TEXT PRIMARY KEY,
                "id" TEXT,
                "uuid" TEXT,
                "contract_address" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await?;

        sqlx::query(
            r#"CREATE TABLE "ARCADE-CollectionEdition" (
                "entity_id" TEXT PRIMARY KEY,
                "collection" TEXT,
                "edition" TEXT,
                "active" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await?;

        sqlx::query(
            r#"INSERT INTO "ARCADE-Collection" (entity_id, id, uuid, contract_address)
               VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind("0x0022222222222222222222222222222222222222222222222222222222222222")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .bind("0x0999")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .execute(&source_pool)
        .await?;

        let sink = ArcadeSink::new(&source_url, &erc721_db.to_string_lossy(), Some(1)).await?;

        {
            let mut tracked_tables = sink
                .tracked_tables
                .write()
                .expect("tracked table map lock poisoned");
            tracked_tables.insert(
                COLLECTION_EDITION_TABLE_ID.to_string(),
                TrackedTable::Collection,
            );
        }

        let initial_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM torii_arcade_collections")
                .fetch_one(&source_pool)
                .await?;
        assert_eq!(initial_count, 0);

        sqlx::query(
            r#"INSERT INTO "ARCADE-CollectionEdition" (entity_id, collection, edition, active)
               VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind("0x0033333333333333333333333333333333333333333333333333333333333333")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .bind("0x0000000000000000000000000000000000000000000000000000000000000002")
        .bind("TRUE")
        .execute(&source_pool)
        .await?;

        let envelope = introspect_envelope(IntrospectMsg::InsertsFields(InsertsFields {
            table: Felt::from_hex_unchecked(COLLECTION_EDITION_TABLE_ID),
            columns: vec![],
            records: vec![Record {
                id: Felt::from_hex_unchecked(
                    "0x0033333333333333333333333333333333333333333333333333333333333333",
                )
                .to_bytes_be(),
                values: Vec::new(),
            }],
        }));

        sink.process(&[envelope], &ExtractionBatch::empty()).await?;

        let projected_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM torii_arcade_collections")
                .fetch_one(&source_pool)
                .await?;
        let projected_edition_id: Option<String> =
            sqlx::query_scalar("SELECT edition_id FROM torii_arcade_collections LIMIT 1")
                .fetch_optional(&source_pool)
                .await?;

        assert_eq!(projected_count, 1);
        assert_eq!(projected_edition_id.as_deref(), Some("0x2"));
        Ok(())
    }

    #[tokio::test]
    async fn collection_edition_insert_without_collection_table_refreshes_projection() -> Result<()>
    {
        let temp_dir = tempdir()?;
        let source_db = temp_dir.path().join("source.db");
        let erc721_db = temp_dir.path().join("erc721.db");
        let source_url = source_db.to_string_lossy().to_string();
        let source_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&format!("sqlite://{source_url}"))?
                    .create_if_missing(true),
            )
            .await?;

        sqlx::query(
            r#"CREATE TABLE "ARCADE-CollectionEdition" (
                "entity_id" TEXT PRIMARY KEY,
                "collection" TEXT,
                "edition" TEXT,
                "active" TEXT
            )"#,
        )
        .execute(&source_pool)
        .await?;

        let sink = ArcadeSink::new(&source_url, &erc721_db.to_string_lossy(), Some(1)).await?;

        {
            let mut tracked_tables = sink
                .tracked_tables
                .write()
                .expect("tracked table map lock poisoned");
            tracked_tables.insert(
                COLLECTION_EDITION_TABLE_ID.to_string(),
                TrackedTable::Collection,
            );
        }

        sqlx::query(
            r#"INSERT INTO "ARCADE-CollectionEdition" (entity_id, collection, edition, active)
               VALUES (?1, ?2, ?3, ?4)"#,
        )
        .bind("0x0033333333333333333333333333333333333333333333333333333333333333")
        .bind("0x046da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4")
        .bind("0x0000000000000000000000000000000000000000000000000000000000000002")
        .bind("TRUE")
        .execute(&source_pool)
        .await?;

        let envelope = introspect_envelope(IntrospectMsg::InsertsFields(InsertsFields {
            table: Felt::from_hex_unchecked(COLLECTION_EDITION_TABLE_ID),
            columns: vec![],
            records: vec![Record {
                id: Felt::from_hex_unchecked(
                    "0x0033333333333333333333333333333333333333333333333333333333333333",
                )
                .to_bytes_be(),
                values: Vec::new(),
            }],
        }));

        sink.process(&[envelope], &ExtractionBatch::empty()).await?;

        let projected = sqlx::query(
            "SELECT collection_id, edition_id, uuid, contract_address
             FROM torii_arcade_collections
             LIMIT 1",
        )
        .fetch_one(&source_pool)
        .await?;

        let collection_id: String = projected.try_get("collection_id")?;
        let edition_id: String = projected.try_get("edition_id")?;
        let uuid: Option<String> = projected.try_get("uuid")?;
        let contract_address: String = projected.try_get("contract_address")?;

        assert_eq!(
            collection_id,
            "0x46da8955829adf2bda310099a0063451923f02e648cf25a1203aac6335cf0e4"
        );
        assert_eq!(edition_id, "0x2");
        assert_eq!(uuid, None);
        assert_eq!(contract_address, collection_id);
        Ok(())
    }
}
