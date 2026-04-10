use anyhow::Result;
use async_trait::async_trait;
use prost::Message;
use prost_types::Any;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet_types_raw::Felt;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use torii::command::CommandHandler;
use torii::etl::sink::EventBus;
use torii::UpdateType;
use torii_common::MetadataFetcher;

use crate::proto;
use crate::storage::Erc20Storage;

#[derive(Debug, Clone)]
pub struct FetchErc20MetadataCommand {
    pub token: Felt,
}

pub struct Erc20MetadataCommandHandler {
    fetcher: Arc<MetadataFetcher>,
    storage: Arc<Erc20Storage>,
    event_bus: Mutex<Option<Arc<EventBus>>>,
    in_flight: Mutex<HashSet<Felt>>,
    max_retries: u8,
}

impl Erc20MetadataCommandHandler {
    pub fn new(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        storage: Arc<Erc20Storage>,
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

    fn metadata_retry_delay(attempt: u8) -> std::time::Duration {
        let shift = u32::from(attempt.saturating_sub(1).min(8));
        std::time::Duration::from_millis(250 * (1u64 << shift))
    }
}

#[async_trait]
impl CommandHandler for Erc20MetadataCommandHandler {
    fn supports(&self, command: &dyn torii::command::Command) -> bool {
        command.as_any().is::<FetchErc20MetadataCommand>()
    }

    fn attach_event_bus(&self, event_bus: Arc<EventBus>) {
        *self.event_bus.lock().unwrap() = Some(event_bus);
    }

    async fn handle_command(&self, command: Box<dyn torii::command::Command>) -> Result<()> {
        let command = command
            .into_any()
            .downcast::<FetchErc20MetadataCommand>()
            .map_err(|_| anyhow::anyhow!("erc20 metadata handler received unexpected command"))?;
        let command = *command;

        {
            let mut in_flight = self.in_flight.lock().unwrap();
            if !in_flight.insert(command.token) {
                tracing::debug!(
                    target: "torii_erc20::handlers",
                    token = %format!("{:#x}", command.token),
                    "Skipping duplicate in-flight ERC20 metadata command"
                );
                return Ok(());
            }
        }

        let mut attempt = 1;
        let result = loop {
            let meta = self.fetcher.fetch_erc20_metadata(command.token).await;
            let outcome: Result<()> = async {
                self.storage
                    .upsert_token_metadata(
                        command.token,
                        meta.name.as_deref(),
                        meta.symbol.as_deref(),
                        meta.decimals,
                        meta.total_supply,
                    )
                    .await?;

                let event_bus = self.event_bus.lock().unwrap().clone();
                if let Some(event_bus) = event_bus {
                    let meta_entry = proto::TokenMetadataEntry {
                        token: command.token.to_be_bytes_vec(),
                        name: meta.name,
                        symbol: meta.symbol,
                        decimals: meta.decimals.map(|d| d as u32),
                        total_supply: meta.total_supply.map(|s| s.),
                    };

                    let mut buf = Vec::new();
                    meta_entry.encode(&mut buf)?;
                    let any = Any {
                        type_url: "type.googleapis.com/torii.sinks.erc20.TokenMetadataEntry"
                            .to_string(),
                        value: buf,
                    };

                    event_bus.publish_protobuf(
                        "erc20.metadata",
                        "erc20.metadata",
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
                    ::metrics::counter!("torii_erc20_metadata_retries_total").increment(1);
                    tracing::warn!(
                        target: "torii_erc20::handlers",
                        token = %format!("{:#x}", command.token),
                        attempt = next_attempt,
                        max_retries = self.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %error,
                        "ERC20 metadata command failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt = next_attempt;
                }
                Err(error) => {
                    ::metrics::counter!("torii_erc20_metadata_terminal_failures_total")
                        .increment(1);
                    break Err(error);
                }
            }
        };

        self.in_flight.lock().unwrap().remove(&command.token);
        result
    }
}
