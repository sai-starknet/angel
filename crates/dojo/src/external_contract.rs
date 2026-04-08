use anyhow::{Context, Result};
use async_trait::async_trait;
use introspect_types::{
    cairo_event_name_and_selector, CairoDeserialize, CairoDeserializer, CairoEvent, CairoSerde,
    DecodeResult, FeltSource,
};
use serde::{Deserialize, Serialize};
use starknet_types_core::felt::Felt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use torii::command::{Command, CommandHandler};
use torii::etl::decoder::DecoderId;
use torii::etl::engine_db::EngineDb;
use torii::etl::{EventBody, EventMsg, TypeId};

const EVENT_EXTRACTOR_TYPE: &str = "event";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisteredContractType {
    World,
    Erc20,
    Erc721,
    Erc1155,
    Other,
}

pub type SharedContractTypeRegistry = Arc<RwLock<HashMap<Felt, RegisteredContractType>>>;
pub type SharedDecoderRegistry = Arc<RwLock<HashMap<Felt, Vec<DecoderId>>>>;

#[derive(Debug)]
pub struct ExternalContractRegisteredEvent {
    pub namespace: String,
    pub contract_name: String,
    pub instance_name: String,
    pub contract_selector: Felt,
    pub class_hash: Felt,
    pub contract_address: Felt,
    pub block_number: u64,
}

impl<D: FeltSource> CairoEvent<CairoSerde<D>> for ExternalContractRegisteredEvent {
    fn deserialize_event<K: FeltSource>(
        keys: &mut K,
        data: &mut CairoSerde<D>,
    ) -> DecodeResult<Self> {
        let mut keys: CairoSerde<_> = keys.into();
        let namespace = keys.next_string()?;
        let contract_name = keys.next_string()?;
        let instance_name = keys.next_string()?;
        let contract_selector = keys.next()?;
        let class_hash = data.next()?;
        let contract_address = data.next()?;
        let block_number = <u64 as CairoDeserialize<_>>::deserialize(data)?;
        Ok(Self {
            namespace,
            contract_name,
            instance_name,
            contract_selector,
            class_hash,
            contract_address,
            block_number,
        })
    }
}

cairo_event_name_and_selector!(
    ExternalContractRegisteredEvent,
    "ExternalContractRegistered"
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalContractRegistered {
    pub namespace: String,
    pub contract_name: String,
    pub instance_name: String,
    pub contract_selector: Felt,
    pub class_hash: Felt,
    pub contract_address: Felt,
    pub registration_block: u64,
}

impl From<ExternalContractRegisteredEvent> for ExternalContractRegistered {
    fn from(value: ExternalContractRegisteredEvent) -> Self {
        Self {
            namespace: value.namespace,
            contract_name: value.contract_name,
            instance_name: value.instance_name,
            contract_selector: value.contract_selector,
            class_hash: value.class_hash,
            contract_address: value.contract_address,
            registration_block: value.block_number,
        }
    }
}

pub type ExternalContractRegisteredBody = EventBody<ExternalContractRegistered>;

impl EventMsg for ExternalContractRegistered {
    fn event_id(&self) -> String {
        format!(
            "dojo.external_contract_registered.{:064x}.{}",
            self.contract_address, self.registration_block
        )
    }

    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("dojo.external_contract_registered")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedExternalContract {
    pub contract_type: RegisteredContractType,
    pub decoder_ids: Vec<DecoderId>,
}

pub fn resolve_external_contract(contract_name: &str) -> Option<ResolvedExternalContract> {
    let normalized = contract_name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_uppercase();

    if normalized.is_empty() {
        return None;
    }

    match normalized.as_str() {
        "ERC20" => Some(ResolvedExternalContract {
            contract_type: RegisteredContractType::Erc20,
            decoder_ids: vec![DecoderId::new("erc20")],
        }),
        "ERC721" => Some(ResolvedExternalContract {
            contract_type: RegisteredContractType::Erc721,
            decoder_ids: vec![DecoderId::new("erc721")],
        }),
        "ERC1155" => Some(ResolvedExternalContract {
            contract_type: RegisteredContractType::Erc1155,
            decoder_ids: vec![DecoderId::new("erc1155")],
        }),
        _ => Some(ResolvedExternalContract {
            contract_type: RegisteredContractType::World,
            decoder_ids: vec![DecoderId::new("dojo-introspect")],
        }),
    }
}

pub fn contract_type_from_decoder_ids(decoder_ids: &[DecoderId]) -> Option<RegisteredContractType> {
    let ids: HashSet<_> = decoder_ids.iter().copied().collect();
    if ids.len() != 1 {
        return None;
    }

    if ids.contains(&DecoderId::new("erc20")) {
        Some(RegisteredContractType::Erc20)
    } else if ids.contains(&DecoderId::new("erc721")) {
        Some(RegisteredContractType::Erc721)
    } else if ids.contains(&DecoderId::new("erc1155")) {
        Some(RegisteredContractType::Erc1155)
    } else {
        None
    }
}

#[derive(Debug, Clone)]
pub struct RegisterExternalContractCommand {
    pub world_address: Felt,
    pub contract_address: Felt,
    pub contract_name: String,
    pub namespace: String,
    pub instance_name: String,
    pub from_block: u64,
    pub registration_block: u64,
    pub contract_type: RegisteredContractType,
    pub decoder_ids: Vec<DecoderId>,
}

pub struct RegisterExternalContractCommandHandler {
    engine_db: Arc<EngineDb>,
    decoder_registry: SharedDecoderRegistry,
    contract_type_registry: SharedContractTypeRegistry,
}

impl RegisterExternalContractCommandHandler {
    pub fn new(
        engine_db: Arc<EngineDb>,
        decoder_registry: SharedDecoderRegistry,
        contract_type_registry: SharedContractTypeRegistry,
    ) -> Self {
        Self {
            engine_db,
            decoder_registry,
            contract_type_registry,
        }
    }
}

#[async_trait]
impl CommandHandler for RegisterExternalContractCommandHandler {
    fn supports(&self, command: &dyn Command) -> bool {
        command.as_any().is::<RegisterExternalContractCommand>()
    }

    async fn handle_command(&self, command: Box<dyn Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<RegisterExternalContractCommand>()
            .map_err(|_| anyhow::anyhow!("invalid external contract registration command"))?;
        let command = *command;

        self.engine_db
            .set_contract_decoders(command.contract_address.into(), &command.decoder_ids)
            .await
            .with_context(|| {
                format!(
                    "failed to persist decoder mapping for {:#x}",
                    command.contract_address
                )
            })?;

        let state_key = format!("{:#x}", command.contract_address);
        let existing_state = self
            .engine_db
            .get_extractor_state(EVENT_EXTRACTOR_TYPE, &state_key)
            .await
            .with_context(|| {
                format!(
                    "failed to load extractor state for {:#x}",
                    command.contract_address
                )
            })?;

        if existing_state.is_none() {
            self.engine_db
                .set_extractor_state(
                    EVENT_EXTRACTOR_TYPE,
                    &state_key,
                    &format!("block:{}", command.from_block),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to persist extractor state for {:#x}",
                        command.contract_address
                    )
                })?;
        } else {
            tracing::debug!(
                target: "torii::dojo::external_contract",
                world = format!("{:#x}", command.world_address),
                contract = format!("{:#x}", command.contract_address),
                "External contract already has extractor state; keeping current progress"
            );
        }

        self.decoder_registry
            .write()
            .await
            .insert(command.contract_address, command.decoder_ids);
        self.contract_type_registry
            .write()
            .await
            .insert(command.contract_address, command.contract_type);

        tracing::info!(
            target: "torii::dojo::external_contract",
            world = format!("{:#x}", command.world_address),
            contract = format!("{:#x}", command.contract_address),
            namespace = command.namespace,
            contract_name = command.contract_name,
            instance_name = command.instance_name,
            from_block = command.from_block,
            registration_block = command.registration_block,
            "Registered external contract for runtime indexing"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use torii::etl::engine_db::EngineDbConfig;

    #[test]
    fn resolves_supported_contract_names() {
        let erc20 = resolve_external_contract("ERC20").unwrap();
        assert_eq!(erc20.contract_type, RegisteredContractType::Erc20);
        assert_eq!(erc20.decoder_ids, vec![DecoderId::new("erc20")]);

        let erc721 = resolve_external_contract("erc_721").unwrap();
        assert_eq!(erc721.contract_type, RegisteredContractType::Erc721);

        let erc1155 = resolve_external_contract("Erc-1155").unwrap();
        assert_eq!(erc1155.contract_type, RegisteredContractType::Erc1155);

        let dojo = resolve_external_contract("Actions").unwrap();
        assert_eq!(dojo.contract_type, RegisteredContractType::World);
        assert_eq!(dojo.decoder_ids, vec![DecoderId::new("dojo-introspect")]);

        assert!(resolve_external_contract("---").is_none());
    }

    #[tokio::test]
    async fn registration_handler_is_idempotent_for_existing_progress() {
        let engine_db = Arc::new(
            EngineDb::new(EngineDbConfig {
                path: "sqlite::memory:".to_string(),
            })
            .await
            .unwrap(),
        );
        let decoder_registry = Arc::new(RwLock::new(HashMap::new()));
        let contract_type_registry = Arc::new(RwLock::new(HashMap::new()));
        let handler = RegisterExternalContractCommandHandler::new(
            engine_db.clone(),
            decoder_registry.clone(),
            contract_type_registry.clone(),
        );
        let contract_address = Felt::from_hex("0x1234").unwrap();

        handler
            .handle_command(Box::new(RegisterExternalContractCommand {
                world_address: Felt::ONE,
                contract_address,
                contract_name: "ERC20".to_string(),
                namespace: "tokens".to_string(),
                instance_name: "eth".to_string(),
                from_block: 12,
                registration_block: 77,
                contract_type: RegisteredContractType::Erc20,
                decoder_ids: vec![DecoderId::new("erc20")],
            }))
            .await
            .unwrap();

        assert_eq!(
            engine_db
                .get_extractor_state(EVENT_EXTRACTOR_TYPE, &format!("{contract_address:#x}"))
                .await
                .unwrap()
                .as_deref(),
            Some("block:12")
        );

        engine_db
            .set_extractor_state(
                EVENT_EXTRACTOR_TYPE,
                &format!("{contract_address:#x}"),
                "block:91",
            )
            .await
            .unwrap();

        handler
            .handle_command(Box::new(RegisterExternalContractCommand {
                world_address: Felt::ONE,
                contract_address,
                contract_name: "ERC20".to_string(),
                namespace: "tokens".to_string(),
                instance_name: "eth".to_string(),
                from_block: 12,
                registration_block: 77,
                contract_type: RegisteredContractType::Erc20,
                decoder_ids: vec![DecoderId::new("erc20")],
            }))
            .await
            .unwrap();

        assert_eq!(
            engine_db
                .get_extractor_state(EVENT_EXTRACTOR_TYPE, &format!("{contract_address:#x}"))
                .await
                .unwrap()
                .as_deref(),
            Some("block:91")
        );
        assert_eq!(
            engine_db
                .get_contract_decoders(contract_address.into())
                .await
                .unwrap()
                .unwrap(),
            vec![DecoderId::new("erc20")]
        );
        assert_eq!(
            decoder_registry
                .read()
                .await
                .get(&contract_address)
                .cloned()
                .unwrap(),
            vec![DecoderId::new("erc20")]
        );
        assert_eq!(
            contract_type_registry
                .read()
                .await
                .get(&contract_address)
                .copied(),
            Some(RegisteredContractType::Erc20)
        );
    }
}
