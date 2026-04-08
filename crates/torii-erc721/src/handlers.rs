use crate::conversions::{
    core_to_primitive_u256, core_to_raw_felt, primitive_to_core_u256, u256_to_bytes,
};
use anyhow::Result;
use async_trait::async_trait;
use prost::Message;
use prost_types::Any;
use starknet::core::types::Felt;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use torii::command::CommandHandler;
use torii::etl::sink::EventBus;
use torii::UpdateType;
use torii_common::{process_token_uri_request, MetadataFetcher, TokenUriRequest};

use crate::proto;
use crate::storage::Erc721Storage;

#[derive(Debug, Clone)]
pub struct FetchErc721MetadataCommand {
    pub token: Felt,
}

#[derive(Debug, Clone)]
pub struct RefreshErc721TokenUriCommand {
    pub contract: Felt,
    pub token_id: starknet::core::types::U256,
}

pub struct Erc721MetadataCommandHandler {
    fetcher: Arc<MetadataFetcher>,
    storage: Arc<Erc721Storage>,
    event_bus: Mutex<Option<Arc<EventBus>>>,
    in_flight: Mutex<HashSet<Felt>>,
    max_retries: u8,
}

impl Erc721MetadataCommandHandler {
    pub fn new(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        storage: Arc<Erc721Storage>,
        max_retries: u8,
    ) -> Self {
        Self {
            fetcher: Arc::new(MetadataFetcher::new(provider)),
            storage,
            event_bus: Mutex::new(None),
            in_flight: Mutex::new(HashSet::new()),
            max_retries: max_retries.max(1),
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

    fn metadata_complete(meta: &torii_common::TokenMetadata) -> bool {
        meta.name
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && meta
                .symbol
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    fn metadata_retry_delay(attempt: u8) -> std::time::Duration {
        let shift = u32::from(attempt.saturating_sub(1).min(8));
        std::time::Duration::from_millis(250 * (1u64 << shift))
    }
}

#[async_trait]
impl CommandHandler for Erc721MetadataCommandHandler {
    fn supports(&self, command: &dyn torii::command::Command) -> bool {
        command.as_any().is::<FetchErc721MetadataCommand>()
    }

    fn attach_event_bus(&self, event_bus: Arc<EventBus>) {
        *self.event_bus.lock().unwrap() = Some(event_bus);
    }

    async fn handle_command(&self, command: Box<dyn torii::command::Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<FetchErc721MetadataCommand>()
            .map_err(|_| anyhow::anyhow!("erc721 metadata handler received unexpected command"))?;
        let command = *command;

        {
            let mut in_flight = self.in_flight.lock().unwrap();
            if !in_flight.insert(command.token) {
                tracing::debug!(
                    target: "torii_erc721::handlers",
                    token = %format!("{:#x}", command.token),
                    "Skipping duplicate in-flight ERC721 metadata command"
                );
                return Ok(());
            }
        }

        let mut attempt = 1;
        let result = loop {
            let meta = self
                .fetcher
                .fetch_erc721_metadata(core_to_raw_felt(command.token))
                .await;
            if !Self::metadata_complete(&meta) && attempt < self.max_retries {
                let next_attempt = attempt + 1;
                let delay = Self::metadata_retry_delay(next_attempt);
                tracing::warn!(
                    target: "torii_erc721::handlers",
                    token = %format!("{:#x}", command.token),
                    attempt = next_attempt,
                    max_retries = self.max_retries,
                    delay_ms = delay.as_millis() as u64,
                    has_name = meta.name.as_deref().is_some_and(|value| !value.trim().is_empty()),
                    has_symbol = meta.symbol.as_deref().is_some_and(|value| !value.trim().is_empty()),
                    has_total_supply = meta.total_supply.is_some(),
                    "ERC721 metadata fetch returned incomplete data, retrying"
                );
                tokio::time::sleep(delay).await;
                attempt = next_attempt;
                continue;
            }

            let outcome: Result<()> = async {
                self.storage
                    .upsert_token_metadata(
                        command.token,
                        meta.name.as_deref(),
                        meta.symbol.as_deref(),
                        meta.total_supply.map(primitive_to_core_u256),
                    )
                    .await?;

                let event_bus = self.event_bus.lock().unwrap().clone();
                if let Some(event_bus) = event_bus {
                    let meta_entry = proto::TokenMetadataEntry {
                        token: command.token.to_bytes_be().to_vec(),
                        name: meta.name,
                        symbol: meta.symbol,
                        total_supply: meta
                            .total_supply
                            .map(primitive_to_core_u256)
                            .map(u256_to_bytes),
                    };

                    let mut buf = Vec::new();
                    meta_entry.encode(&mut buf)?;
                    let any = Any {
                        type_url: "type.googleapis.com/torii.sinks.erc721.TokenMetadataEntry"
                            .to_string(),
                        value: buf,
                    };

                    event_bus.publish_protobuf(
                        "erc721.metadata",
                        "erc721.metadata",
                        &any,
                        &meta_entry,
                        UpdateType::Created,
                        Self::matches_metadata_filters,
                    );
                }

                Ok(())
            }
            .await;

            match outcome {
                Ok(()) => break Ok(()),
                Err(error) if attempt < self.max_retries => {
                    let next_attempt = attempt + 1;
                    let delay = Self::metadata_retry_delay(next_attempt);
                    tracing::warn!(
                        target: "torii_erc721::handlers",
                        token = %format!("{:#x}", command.token),
                        attempt = next_attempt,
                        max_retries = self.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %error,
                        "ERC721 metadata command failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt = next_attempt;
                }
                Err(error) => {
                    break Err(error);
                }
            }
        };

        self.in_flight.lock().unwrap().remove(&command.token);
        result
    }
}

pub struct Erc721TokenUriCommandHandler {
    fetcher: Arc<MetadataFetcher>,
    storage: Arc<Erc721Storage>,
    image_cache_dir: Option<PathBuf>,
    in_flight: Mutex<HashSet<(Felt, starknet::core::types::U256)>>,
}

impl Erc721TokenUriCommandHandler {
    pub fn new(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        storage: Arc<Erc721Storage>,
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
impl CommandHandler for Erc721TokenUriCommandHandler {
    fn supports(&self, command: &dyn torii::command::Command) -> bool {
        command.as_any().is::<RefreshErc721TokenUriCommand>()
    }

    async fn handle_command(&self, command: Box<dyn torii::command::Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<RefreshErc721TokenUriCommand>()
            .map_err(|_| anyhow::anyhow!("erc721 token uri handler received unexpected command"))?;
        let command = *command;

        {
            let mut in_flight = self.in_flight.lock().unwrap();
            if !in_flight.insert((command.contract, command.token_id)) {
                tracing::debug!(
                    target: "torii_erc721::handlers",
                    contract = %format!("{:#x}", command.contract),
                    token_id = %command.token_id,
                    "Skipping duplicate in-flight ERC721 token URI command"
                );
                return Ok(());
            }
        }

        let result = process_token_uri_request(
            self.fetcher.as_ref(),
            self.storage.as_ref(),
            &TokenUriRequest {
                contract: core_to_raw_felt(command.contract),
                token_id: core_to_primitive_u256(command.token_id),
                standard: torii_common::TokenStandard::Erc721,
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
