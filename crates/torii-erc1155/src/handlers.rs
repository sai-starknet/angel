use anyhow::Result;
use async_trait::async_trait;
use primitive_types::U256;
use prost::Message;
use prost_types::Any;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet_types_raw::Felt;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use torii::command::CommandHandler;
use torii::etl::sink::EventBus;
use torii::UpdateType;
use torii_common::{process_token_uri_request, u256_to_bytes, MetadataFetcher, TokenUriRequest};

use crate::proto;
use crate::storage::Erc1155Storage;

#[derive(Debug, Clone)]
pub struct FetchErc1155MetadataCommand {
    pub token: Felt,
}

#[derive(Debug, Clone)]
pub struct RefreshErc1155TokenUriCommand {
    pub contract: Felt,
    pub token_id: U256,
}

pub struct Erc1155MetadataCommandHandler {
    fetcher: Arc<MetadataFetcher>,
    storage: Arc<Erc1155Storage>,
    event_bus: Mutex<Option<Arc<EventBus>>>,
    in_flight: Mutex<HashSet<Felt>>,
}

impl Erc1155MetadataCommandHandler {
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>, storage: Arc<Erc1155Storage>) -> Self {
        Self {
            fetcher: Arc::new(MetadataFetcher::new(provider)),
            storage,
            event_bus: Mutex::new(None),
            in_flight: Mutex::new(HashSet::new()),
        }
    }

    fn matches_metadata_filters(
        metadata: &proto::TokenMetadataEntry,
        filters: &std::collections::HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&metadata.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl CommandHandler for Erc1155MetadataCommandHandler {
    fn supports(&self, command: &dyn torii::command::Command) -> bool {
        command.as_any().is::<FetchErc1155MetadataCommand>()
    }

    fn attach_event_bus(&self, event_bus: Arc<EventBus>) {
        *self.event_bus.lock().unwrap() = Some(event_bus);
    }

    async fn handle_command(&self, command: Box<dyn torii::command::Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<FetchErc1155MetadataCommand>()
            .map_err(|_| anyhow::anyhow!("erc1155 metadata handler received unexpected command"))?;
        let command = *command;

        {
            let mut in_flight = self.in_flight.lock().unwrap();
            if !in_flight.insert(command.token) {
                tracing::debug!(
                    target: "torii_erc1155::handlers",
                    token = %format!("{:#x}", command.token),
                    "Skipping duplicate in-flight ERC1155 metadata command"
                );
                return Ok(());
            }
        }

        let meta = self.fetcher.fetch_erc1155_metadata(command.token).await;
        let result: Result<()> = async {
            self.storage
                .upsert_token_metadata(
                    command.token,
                    meta.name.as_deref(),
                    meta.symbol.as_deref(),
                    meta.total_supply,
                )
                .await?;

            let event_bus = self.event_bus.lock().unwrap().clone();
            if let Some(event_bus) = event_bus {
                let meta_entry = proto::TokenMetadataEntry {
                    token: command.token.to_be_bytes_vec(),
                    name: meta.name,
                    symbol: meta.symbol,
                    total_supply: meta.total_supply.map(u256_to_bytes),
                };

                let mut buf = Vec::new();
                meta_entry.encode(&mut buf)?;
                let any = Any {
                    type_url: "type.googleapis.com/torii.sinks.erc1155.TokenMetadataEntry"
                        .to_string(),
                    value: buf,
                };

                event_bus.publish_protobuf(
                    "erc1155.metadata",
                    "erc1155.metadata",
                    &any,
                    &meta_entry,
                    UpdateType::Created,
                    Self::matches_metadata_filters,
                );
            }

            Ok(())
        }
        .await;

        self.in_flight.lock().unwrap().remove(&command.token);
        result
    }
}

pub struct Erc1155TokenUriCommandHandler {
    fetcher: Arc<MetadataFetcher>,
    storage: Arc<Erc1155Storage>,
    image_cache_dir: Option<PathBuf>,
    in_flight: Mutex<HashSet<(Felt, U256)>>,
}

impl Erc1155TokenUriCommandHandler {
    pub fn new(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        storage: Arc<Erc1155Storage>,
        image_cache_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            fetcher: Arc::new(MetadataFetcher::new(provider)),
            storage,
            image_cache_dir,
            in_flight: Mutex::new(HashSet::new()),
        }
    }
}

#[async_trait]
impl CommandHandler for Erc1155TokenUriCommandHandler {
    fn supports(&self, command: &dyn torii::command::Command) -> bool {
        command.as_any().is::<RefreshErc1155TokenUriCommand>()
    }

    async fn handle_command(&self, command: Box<dyn torii::command::Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<RefreshErc1155TokenUriCommand>()
            .map_err(|_| {
                anyhow::anyhow!("erc1155 token uri handler received unexpected command")
            })?;
        let command = *command;

        {
            let mut in_flight = self.in_flight.lock().unwrap();
            if !in_flight.insert((command.contract, command.token_id)) {
                tracing::debug!(
                    target: "torii_erc1155::handlers",
                    contract = %format!("{:#x}", command.contract),
                    token_id = %command.token_id,
                    "Skipping duplicate in-flight ERC1155 token URI command"
                );
                return Ok(());
            }
        }

        let result = process_token_uri_request(
            self.fetcher.as_ref(),
            self.storage.as_ref(),
            &TokenUriRequest {
                contract: command.contract,
                token_id: command.token_id,
                standard: torii_common::TokenStandard::Erc1155,
            },
            self.image_cache_dir.as_deref(),
        )
        .await;

        self.in_flight
            .lock()
            .unwrap()
            .remove(&(command.contract, command.token_id));
        result
    }
}
