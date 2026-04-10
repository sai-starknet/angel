use crate::etl::engine_db::EngineDb;
use crate::etl::extractor::{BlockContext, ExtractionBatch, RetryPolicy, TransactionContext};
use crate::etl::StarknetEvent;
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use starknet::core::types::requests::{GetBlockWithTxHashesRequest, GetTransactionReceiptRequest};
use starknet::core::types::{BlockId, ExecutionResult, MaybePreConfirmedBlockWithTxHashes};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const RECEIPT_LOOKUP_BATCH_SIZE: usize = 200;

pub(crate) fn resolved_rpc_parallelism(configured: usize) -> usize {
    if configured == 0 {
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get().clamp(2, 8))
            .unwrap_or(4)
    } else {
        configured.max(1)
    }
}

pub(crate) async fn fetch_block_timestamps(
    provider: Arc<JsonRpcClient<HttpTransport>>,
    retry_policy: &RetryPolicy,
    rpc_parallelism: usize,
    block_numbers: &[u64],
    engine_db: &EngineDb,
) -> Result<HashMap<u64, u64>> {
    if block_numbers.is_empty() {
        return Ok(HashMap::new());
    }

    let cached = engine_db.get_block_timestamps(block_numbers).await?;
    let uncached: Vec<u64> = block_numbers
        .iter()
        .filter(|n| !cached.contains_key(n))
        .copied()
        .collect();

    if uncached.is_empty() {
        return Ok(cached);
    }

    tracing::debug!(
        target: "torii::etl::event",
        cached = cached.len(),
        uncached = uncached.len(),
        "Fetching block timestamps"
    );

    ::metrics::gauge!("torii_rpc_parallelism").set(rpc_parallelism as f64);

    let timestamp_tasks = uncached
        .chunks(RECEIPT_LOOKUP_BATCH_SIZE)
        .enumerate()
        .map(|(chunk_index, chunk)| {
            let requests: Vec<ProviderRequestData> = chunk
                .iter()
                .map(|&n| {
                    ProviderRequestData::GetBlockWithTxHashes(GetBlockWithTxHashesRequest {
                        block_id: BlockId::Number(n),
                    })
                })
                .collect();
            let chunk_blocks = chunk.to_vec();
            let retry_policy = retry_policy.clone();
            let provider = provider.clone();
            async move {
                let chunk_start = std::time::Instant::now();
                let responses = retry_policy
                    .execute(|| {
                        let provider = provider.clone();
                        let requests_ref = &requests;
                        async move {
                            provider
                                .batch_requests(requests_ref)
                                .await
                                .context("Failed to fetch block headers")
                        }
                    })
                    .await?;
                ::metrics::histogram!(
                    "torii_rpc_chunk_duration_seconds",
                    "extractor" => "event",
                    "method" => "get_block_with_tx_hashes_batch"
                )
                .record(chunk_start.elapsed().as_secs_f64());
                Ok::<_, anyhow::Error>((chunk_index, chunk_blocks, responses))
            }
        })
        .collect::<Vec<_>>();

    let mut new_timestamps = stream::iter(timestamp_tasks)
        .buffer_unordered(rpc_parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    new_timestamps.sort_by_key(|(chunk_index, _, _)| *chunk_index);

    let mut merged_timestamps = HashMap::new();
    for (_, chunk_blocks, responses) in new_timestamps {
        for (block_num, response) in chunk_blocks.iter().zip(responses) {
            if let ProviderResponseData::GetBlockWithTxHashes(block) = response {
                match block {
                    MaybePreConfirmedBlockWithTxHashes::Block(b) => {
                        merged_timestamps.insert(*block_num, b.timestamp);
                    }
                    MaybePreConfirmedBlockWithTxHashes::PreConfirmedBlock(_) => {
                        tracing::warn!(
                            target: "torii::etl::event",
                            block_num = block_num,
                            "Skipping pre-confirmed block"
                        );
                    }
                }
            }
        }
    }

    if !merged_timestamps.is_empty() {
        engine_db
            .insert_block_timestamps(&merged_timestamps)
            .await?;
    }

    let mut result = cached;
    result.extend(merged_timestamps);
    Ok(result)
}

pub(crate) async fn fetch_successful_transaction_hashes(
    provider: Arc<JsonRpcClient<HttpTransport>>,
    retry_policy: &RetryPolicy,
    rpc_parallelism: usize,
    tx_hashes: &[Felt],
    extractor_metric_label: &'static str,
) -> Result<HashSet<Felt>> {
    let mut successful = HashSet::with_capacity(tx_hashes.len());

    ::metrics::gauge!("torii_rpc_parallelism").set(rpc_parallelism as f64);

    let receipt_tasks = tx_hashes
        .chunks(RECEIPT_LOOKUP_BATCH_SIZE)
        .enumerate()
        .map(|(chunk_index, chunk)| {
            let requests: Vec<ProviderRequestData> = chunk
                .iter()
                .map(|tx_hash| {
                    ProviderRequestData::GetTransactionReceipt(GetTransactionReceiptRequest {
                        transaction_hash: tx_hash.into(),
                    })
                })
                .collect();
            let requested_hashes = chunk.to_vec();
            let retry_policy = retry_policy.clone();
            let provider = provider.clone();
            async move {
                let chunk_start = std::time::Instant::now();
                let responses = retry_policy
                    .execute(|| {
                        let provider = provider.clone();
                        let requests_ref = &requests;
                        async move {
                            provider
                                .batch_requests(requests_ref)
                                .await
                                .context("Failed to fetch transaction receipts in batch")
                        }
                    })
                    .await;
                let responses = match responses {
                    Ok(responses) => {
                        ::metrics::counter!(
                            "torii_rpc_requests_total",
                            "method" => "get_transaction_receipt_batch",
                            "status" => "ok"
                        )
                        .increment(1);
                        responses
                    }
                    Err(err) => {
                        ::metrics::counter!(
                            "torii_rpc_requests_total",
                            "method" => "get_transaction_receipt_batch",
                            "status" => "error"
                        )
                        .increment(1);
                        return Err(err);
                    }
                };
                ::metrics::histogram!(
                    "torii_rpc_chunk_duration_seconds",
                    "extractor" => extractor_metric_label,
                    "method" => "get_transaction_receipt_batch"
                )
                .record(chunk_start.elapsed().as_secs_f64());
                Ok::<_, anyhow::Error>((chunk_index, requested_hashes, responses))
            }
        })
        .collect::<Vec<_>>();

    let mut chunk_results = stream::iter(receipt_tasks)
        .buffer_unordered(rpc_parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    chunk_results.sort_by_key(|(chunk_index, _, _)| *chunk_index);

    for (_, requested_hashes, responses) in chunk_results {
        for (requested_tx_hash, response) in requested_hashes.iter().zip(responses) {
            match response {
                ProviderResponseData::GetTransactionReceipt(receipt_with_block_info) => {
                    if matches!(
                        receipt_with_block_info.receipt.execution_result(),
                        ExecutionResult::Succeeded
                    ) {
                        successful.insert(*requested_tx_hash);
                    }
                }
                _ => {
                    anyhow::bail!("Unexpected response type for tx receipt {requested_tx_hash:#x}");
                }
            }
        }
    }

    Ok(successful)
}

pub(crate) fn filter_events_by_tx_hashes(
    events: Vec<StarknetEvent>,
    successful_transaction_hashes: &HashSet<Felt>,
) -> Vec<StarknetEvent> {
    events
        .into_iter()
        .filter(|event| successful_transaction_hashes.contains(&event.transaction_hash))
        .collect()
}

pub(crate) async fn build_batch(
    provider: Arc<JsonRpcClient<HttpTransport>>,
    retry_policy: &RetryPolicy,
    rpc_parallelism: usize,
    events: Vec<StarknetEvent>,
    engine_db: &EngineDb,
) -> Result<ExtractionBatch> {
    let block_numbers: Vec<u64> = events
        .iter()
        .map(|e| e.block_number)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let timestamps = fetch_block_timestamps(
        provider,
        retry_policy,
        rpc_parallelism,
        &block_numbers,
        engine_db,
    )
    .await?;

    let mut blocks = HashMap::new();
    for block_num in block_numbers {
        let timestamp = timestamps.get(&block_num).copied().unwrap_or(0);
        blocks.insert(
            block_num,
            Arc::new(BlockContext {
                number: block_num,
                timestamp,
                hash: Felt::ZERO,
                parent_hash: Felt::ZERO,
            }),
        );
    }

    let mut transactions = HashMap::new();
    for event in &events {
        transactions
            .entry(event.transaction_hash)
            .or_insert_with(|| {
                Arc::new(TransactionContext {
                    hash: event.transaction_hash,
                    block_number: event.block_number,
                    sender_address: None,
                    calldata: Vec::new(),
                })
            });
    }

    Ok(ExtractionBatch {
        events,
        blocks,
        transactions,
        declared_classes: Vec::new(),
        deployed_contracts: Vec::new(),
        cursor: None,
        chain_head: None,
    })
}
