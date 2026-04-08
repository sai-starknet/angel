use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_trait::async_trait;
use starknet_types_raw::Felt;
use torii::axum::Router;
use torii::command::CommandBusSender;
use torii::etl::decoder::DecoderId;
use torii::etl::envelope::{Envelope, TypeId};
use torii::etl::extractor::ExtractionBatch;
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii_dojo::external_contract::{
    resolve_external_contract, ExternalContractRegisteredBody, RegisterExternalContractCommand,
    RegisteredContractType, SharedContractTypeRegistry,
};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};

use crate::grpc_service::{EcsService, TableKind};

pub struct EcsSink {
    service: Arc<EcsService>,
    contract_types: SharedContractTypeRegistry,
    external_contract_from_block: u64,
    external_contract_indexing: bool,
    installed_external_decoders: HashSet<DecoderId>,
    command_bus: RwLock<Option<CommandBusSender>>,
}

impl EcsSink {
    pub async fn new(
        database_url: &str,
        max_connections: Option<u32>,
        erc20_url: Option<&str>,
        erc721_url: Option<&str>,
        erc1155_url: Option<&str>,
        contract_types: SharedContractTypeRegistry,
        external_contract_from_block: u64,
        external_contract_indexing: bool,
        installed_external_decoders: HashSet<DecoderId>,
    ) -> Result<Self> {
        Ok(Self {
            service: Arc::new(
                EcsService::new(
                    database_url,
                    max_connections,
                    erc20_url,
                    erc721_url,
                    erc1155_url,
                )
                .await?,
            ),
            contract_types,
            external_contract_from_block,
            external_contract_indexing,
            installed_external_decoders,
            command_bus: RwLock::new(None),
        })
    }

    pub fn get_grpc_service_impl(&self) -> Arc<EcsService> {
        self.service.clone()
    }

    async fn contract_type_for(&self, contract_address: Felt) -> crate::proto::types::ContractType {
        let contract_type = self
            .contract_types
            .read()
            .await
            .get(&contract_address.into())
            .copied()
            .unwrap_or(RegisteredContractType::World);

        match contract_type {
            RegisteredContractType::World => crate::proto::types::ContractType::World,
            RegisteredContractType::Erc20 => crate::proto::types::ContractType::Erc20,
            RegisteredContractType::Erc721 => crate::proto::types::ContractType::Erc721,
            RegisteredContractType::Erc1155 => crate::proto::types::ContractType::Erc1155,
            RegisteredContractType::Other => crate::proto::types::ContractType::Other,
        }
    }

    async fn handle_external_contract_registered(&self, body: &ExternalContractRegisteredBody) {
        if !self.external_contract_indexing {
            tracing::debug!(
                target: "torii::ecs_sink",
                contract = format!("{:#x}", body.msg.contract_address),
                "External contract registration ignored because runtime indexing is disabled"
            );
            return;
        }

        let Some(resolved) = resolve_external_contract(&body.msg.contract_name) else {
            tracing::warn!(
                target: "torii::ecs_sink",
                world = format!("{:#x}", body.context.from_address),
                contract = format!("{:#x}", body.msg.contract_address),
                contract_name = %body.msg.contract_name,
                "Unsupported external contract registration; skipping runtime indexing"
            );
            return;
        };

        let missing_decoders: Vec<_> = resolved
            .decoder_ids
            .iter()
            .copied()
            .filter(|decoder_id| !self.installed_external_decoders.contains(decoder_id))
            .collect();
        if !missing_decoders.is_empty() {
            tracing::warn!(
                target: "torii::ecs_sink",
                world = format!("{:#x}", body.context.from_address),
                contract = format!("{:#x}", body.msg.contract_address),
                contract_name = %body.msg.contract_name,
                missing_decoders = ?missing_decoders,
                "External contract registration skipped because matching decoders are not installed"
            );
            return;
        }

        let command_bus = self
            .command_bus
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned());
        let Some(command_bus) = command_bus else {
            tracing::warn!(
                target: "torii::ecs_sink",
                contract = format!("{:#x}", body.msg.contract_address),
                "External contract registration dropped because the command bus is unavailable"
            );
            return;
        };

        if let Err(error) = command_bus.dispatch(RegisterExternalContractCommand {
            world_address: body.context.from_address.into(),
            contract_address: body.msg.contract_address,
            contract_name: body.msg.contract_name.clone(),
            namespace: body.msg.namespace.clone(),
            instance_name: body.msg.instance_name.clone(),
            from_block: self.external_contract_from_block,
            registration_block: body.msg.registration_block,
            contract_type: resolved.contract_type,
            decoder_ids: resolved.decoder_ids,
        }) {
            tracing::warn!(
                target: "torii::ecs_sink",
                world = format!("{:#x}", body.context.from_address),
                contract = format!("{:#x}", body.msg.contract_address),
                error = %error,
                "Failed to enqueue external contract registration command"
            );
        }
    }
}

#[async_trait]
impl Sink for EcsSink {
    fn name(&self) -> &'static str {
        "ecs"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![
            TypeId::new("introspect"),
            TypeId::new("dojo.external_contract_registered"),
        ]
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        for (ordinal, event) in batch.events.iter().enumerate() {
            let block_number = batch
                .transactions
                .get(&event.transaction_hash)
                .map_or(event.block_number, |tx| tx.block_number);
            let timestamp = batch
                .blocks
                .get(&block_number)
                .map_or(0, |block| block.timestamp);
            self.service
                .store_event(
                    event.from_address,
                    event.transaction_hash,
                    block_number,
                    timestamp,
                    &event.keys,
                    &event.data,
                    ordinal,
                )
                .await?;

            self.service
                .record_contract_progress(
                    event.from_address,
                    self.contract_type_for(event.from_address).await,
                    block_number,
                    timestamp,
                    None,
                )
                .await?;

            let Some(selector) = event.keys.first() else {
                continue;
            };
            let selector_raw = *selector;
            if matches!(
                selector_raw,
                s if s == Felt::selector("StoreSetRecord")
                    || s == Felt::selector("StoreUpdateRecord")
                    || s == Felt::selector("StoreUpdateMember")
                    || s == Felt::selector("StoreDelRecord")
            ) {
                if event.keys.len() >= 3 {
                    self.service
                        .record_table_kind(event.from_address, event.keys[1], TableKind::Entity)
                        .await?;
                    self.service
                        .upsert_entity_meta(
                            TableKind::Entity,
                            event.from_address,
                            event.keys[1],
                            event.keys[2],
                            timestamp,
                            selector_raw == Felt::selector("StoreDelRecord"),
                        )
                        .await?;
                }
            } else if selector_raw == Felt::selector("EventEmitted") && event.keys.len() >= 2 {
                self.service
                    .record_table_kind(event.from_address, event.keys[1], TableKind::EventMessage)
                    .await?;
            }
        }

        for envelope in envelopes {
            if envelope.type_id == TypeId::new("dojo.external_contract_registered") {
                if let Some(body) = envelope.downcast_ref::<ExternalContractRegisteredBody>() {
                    self.handle_external_contract_registered(body).await;
                }
                continue;
            }

            if envelope.type_id != TypeId::new("introspect") {
                continue;
            }

            let Some(body) = envelope.downcast_ref::<IntrospectBody>() else {
                continue;
            };
            let timestamp = batch
                .transactions
                .get(&body.context.transaction_hash)
                .and_then(|tx| batch.blocks.get(&tx.block_number))
                .map_or(0, |block| block.timestamp);

            match &body.msg {
                IntrospectMsg::CreateTable(table) => {
                    self.service
                        .cache_created_table(body.context.from_address, table)
                        .await;
                    self.service
                        .record_table_kind(
                            body.context.from_address,
                            table.id.into(),
                            TableKind::Entity,
                        )
                        .await
                        .ok();
                }
                IntrospectMsg::UpdateTable(table) => {
                    self.service
                        .cache_updated_table(body.context.from_address, table)
                        .await;
                    self.service
                        .record_table_kind(
                            body.context.from_address,
                            table.id.into(),
                            TableKind::Entity,
                        )
                        .await
                        .ok();
                }
                IntrospectMsg::InsertsFields(insert) => {
                    let kind = if batch.events.iter().any(|event| {
                        event.keys.first().is_some_and(|selector| {
                            *selector == Felt::selector("EventEmitted")
                                && event.keys.get(1).copied() == Some(insert.table.into())
                        })
                    }) {
                        TableKind::EventMessage
                    } else {
                        TableKind::Entity
                    };
                    self.service
                        .record_table_kind(body.context.from_address, insert.table.into(), kind)
                        .await?;
                    let raw_columns: Vec<Felt> =
                        insert.columns.iter().copied().map(Into::into).collect();
                    for record in &insert.records {
                        let entity_id = Felt::from_be_bytes_slice(&record.id)?;
                        self.service
                            .upsert_entity_meta(
                                kind,
                                body.context.from_address,
                                insert.table.into(),
                                entity_id,
                                timestamp,
                                false,
                            )
                            .await?;
                        self.service
                            .upsert_entity_model(
                                kind,
                                body.context.from_address,
                                insert.table.into(),
                                &raw_columns,
                                record,
                                timestamp,
                            )
                            .await?;
                        self.service
                            .publish_entity_update(kind, body.context.from_address, entity_id)
                            .await?;
                    }
                }
                IntrospectMsg::DeleteRecords(delete) => {
                    let kind = self.service.table_kind(delete.table.into()).await?;
                    for row in &delete.rows {
                        let entity_id = row.to_felt();
                        self.service
                            .upsert_entity_meta(
                                kind,
                                body.context.from_address,
                                delete.table.into(),
                                entity_id.into(),
                                timestamp,
                                true,
                            )
                            .await?;
                        self.service
                            .delete_entity_model(
                                kind,
                                body.context.from_address,
                                delete.table.into(),
                                entity_id.into(),
                            )
                            .await?;
                        self.service
                            .publish_entity_update(
                                kind,
                                body.context.from_address,
                                entity_id.into(),
                            )
                            .await?;
                    }
                }
                _ => {}
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

    async fn initialize(&mut self, _event_bus: Arc<EventBus>, context: &SinkContext) -> Result<()> {
        if let Ok(mut command_bus) = self.command_bus.write() {
            *command_bus = Some(context.command_bus.clone());
        }
        self.service.attach_erc_databases().await?;
        Ok(())
    }
}
