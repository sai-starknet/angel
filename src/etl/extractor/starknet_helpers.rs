use crate::etl::StarknetEvent;

use super::{BlockContext, BlockData, DeclaredClass, DeployedContract, TransactionContext};
use anyhow::{Context, Result};
use starknet::core::types::contract::{AbiEntry, TypedAbiEvent};
use starknet::core::types::requests::{GetBlockWithReceiptsRequest, GetClassAtRequest};
use starknet::core::types::{
    BlockId, ContractClass, DeclareTransactionContent, DeployAccountTransactionContent,
    ExecutionResult, InvokeTransactionContent, LegacyContractAbiEntry,
    MaybePreConfirmedBlockWithReceipts, TransactionContent, TransactionReceipt,
};
use starknet::providers::{Provider, ProviderRequestData, ProviderResponseData};
use starknet_types_raw::Felt;
use std::collections::HashSet;

#[inline]
fn is_execution_succeeded(execution_result: &ExecutionResult) -> bool {
    matches!(execution_result, ExecutionResult::Succeeded)
}

#[inline]
fn is_receipt_succeeded(receipt: &TransactionReceipt) -> bool {
    is_execution_succeeded(receipt.execution_result())
}

/// Builds a batch of `GetBlockWithReceipts` requests for a range of block numbers.
///
/// # Arguments
///
/// * `from_block` - The starting block number
/// * `to_block` - The ending block number
///
/// # Returns
///
/// A vector of `ProviderRequestData` requests.
pub fn block_with_receipts_batch_from_block_range(
    from_block: u64,
    to_block: u64,
) -> Vec<ProviderRequestData> {
    (from_block..=to_block)
        .map(|block_num| {
            ProviderRequestData::GetBlockWithReceipts(GetBlockWithReceiptsRequest {
                block_id: BlockId::Number(block_num),
            })
        })
        .collect()
}

/// Fetches contract classes for a list of class hashes using batch requests.
///
/// This is useful for inspecting ABIs to determine contract types (ERC20, ERC721, etc.)
/// or to build selector → function name mappings.
///
/// # Arguments
///
/// * `provider` - The provider to fetch classes from
/// * `class_hashes` - The class hashes to fetch
///
/// # Returns
///
/// A vector of tuples containing (class_hash, ContractClass).
/// If a class is not found, it will return an error.
///
/// # Example
///
/// ```rust,ignore
/// let declared = vec![class_hash_1, class_hash_2];
/// let classes = fetch_classes_batch(&provider, &declared).await?;
///
/// for (class_hash, class) in classes {
///     // Inspect class.abi to determine contract type
///     // Build selector mappings, etc.
/// }
/// ```
pub async fn fetch_classes_batch<P>(
    provider: &P,
    class_hashes: &[Felt],
) -> Result<Vec<(Felt, ContractClass)>>
where
    P: Provider,
{
    if class_hashes.is_empty() {
        return Ok(Vec::new());
    }

    // Build batch request
    let requests: Vec<ProviderRequestData> = class_hashes
        .iter()
        .map(|&class_hash| {
            ProviderRequestData::GetClassAt(GetClassAtRequest {
                block_id: BlockId::Tag(starknet::core::types::BlockTag::Latest),
                contract_address: class_hash.into(),
            })
        })
        .collect();

    // Execute batch request
    let responses = provider
        .batch_requests(&requests)
        .await
        .context("Failed to fetch classes in batch")?;

    // Extract classes from responses
    let mut classes = Vec::new();
    for (idx, response) in responses.into_iter().enumerate() {
        let class_hash = class_hashes[idx];
        match response {
            ProviderResponseData::GetClass(class) => {
                classes.push((class_hash, class));
            }
            _ => {
                anyhow::bail!("Unexpected response type for class {class_hash}: expected GetClass");
            }
        }
    }

    Ok(classes)
}

/// Converts a block with receipts into structured block data.
///
/// This decouples the RPC types from the internal types and extracts all relevant information:
/// - Block context (number, hash, timestamp, etc.)
/// - Transaction contexts (sender, calldata)
/// - Events emitted by transactions
/// - Declared classes (from Declare transactions)
/// - Deployed contracts (from Deploy and DeployAccount transactions)
///
/// This function doesn't support pre-confirmed blocks, only mined blocks.
/// Consumes the block and its content to avoid copying the data.
///
/// # Arguments
///
/// * `block` - The block with receipts
///
/// # Returns
///
/// A `BlockData` structure containing all extracted information.
pub fn block_into_contexts(block: MaybePreConfirmedBlockWithReceipts) -> Result<BlockData> {
    // Skip pending/pre-confirmed blocks
    let block_with_receipts = match block {
        MaybePreConfirmedBlockWithReceipts::Block(b) => b,
        MaybePreConfirmedBlockWithReceipts::PreConfirmedBlock(_) => {
            tracing::debug!(
                target: "torii::etl::block_range",
                "Skipping pre-confirmed block"
            );
            // TODO: implement custom error type for the extractor module.
            return Err(anyhow::anyhow!(
                "pre-confirmed block not supported by block_into_context"
            ));
        }
    };

    let block_context = BlockContext {
        number: block_with_receipts.block_number,
        hash: block_with_receipts.block_hash.into(),
        parent_hash: block_with_receipts.parent_hash.into(),
        timestamp: block_with_receipts.timestamp,
    };

    let transactions = block_with_receipts.transactions;

    let mut all_events = Vec::new();
    let mut transaction_contexts = Vec::new();
    let mut declared_classes = Vec::new();
    let mut deployed_contracts = Vec::new();
    let mut skipped_reverted = 0usize;
    let mut processed_transactions = 0usize;

    for tx_with_receipt in transactions {
        let tx = tx_with_receipt.transaction;
        let receipt = tx_with_receipt.receipt;
        processed_transactions += 1;

        let tx_hash = match &receipt {
            TransactionReceipt::Invoke(r) => r.transaction_hash,
            TransactionReceipt::L1Handler(r) => r.transaction_hash,
            TransactionReceipt::Declare(r) => r.transaction_hash,
            TransactionReceipt::Deploy(r) => r.transaction_hash,
            TransactionReceipt::DeployAccount(r) => r.transaction_hash,
        }
        .into();

        if !is_receipt_succeeded(&receipt) {
            skipped_reverted += 1;
            tracing::debug!(
                target: "torii::etl::block_range",
                tx_hash = %format!("{:#x}", tx_hash),
                "Skipping reverted transaction"
            );
            continue;
        }

        // Extract transaction data based on type, including metadata for declares and deploys
        let (sender_address, calldata, declare_info, deploy_account_class) = match tx {
            TransactionContent::Invoke(content) => match content {
                InvokeTransactionContent::V0(c) => {
                    // V0 doesn't have explicit sender, use contract_address
                    (Some(c.contract_address), c.calldata, None, None)
                }
                InvokeTransactionContent::V1(c) => (Some(c.sender_address), c.calldata, None, None),
                InvokeTransactionContent::V3(c) => (Some(c.sender_address), c.calldata, None, None),
            },
            TransactionContent::L1Handler(content) => {
                (Some(content.contract_address), content.calldata, None, None)
            }
            TransactionContent::Declare(content) => match content {
                DeclareTransactionContent::V0(c) => (
                    Some(c.sender_address),
                    Vec::new(),
                    Some((c.class_hash, None)),
                    None,
                ),
                DeclareTransactionContent::V1(c) => (
                    Some(c.sender_address),
                    Vec::new(),
                    Some((c.class_hash, None)),
                    None,
                ),
                DeclareTransactionContent::V2(c) => (
                    Some(c.sender_address),
                    Vec::new(),
                    Some((c.class_hash, Some(c.compiled_class_hash))),
                    None,
                ),
                DeclareTransactionContent::V3(c) => (
                    Some(c.sender_address),
                    Vec::new(),
                    Some((c.class_hash, Some(c.compiled_class_hash))),
                    None,
                ),
            },
            TransactionContent::Deploy(content) => {
                // Old deploy transactions don't have a sender
                (None, content.constructor_calldata, None, None)
            }
            TransactionContent::DeployAccount(content) => match content {
                DeployAccountTransactionContent::V1(c) => {
                    (None, c.constructor_calldata, None, Some(c.class_hash))
                }
                DeployAccountTransactionContent::V3(c) => {
                    (None, c.constructor_calldata, None, Some(c.class_hash))
                }
            },
        };
        let sender_address = sender_address.map(Into::into);
        // Build transaction context
        transaction_contexts.push(TransactionContext {
            hash: tx_hash,
            block_number: block_with_receipts.block_number,
            sender_address: sender_address,
            calldata: calldata.into_iter().map(Into::into).collect(),
        });

        // Extract declared classes from Declare transactions
        if let Some((class_hash, compiled_class_hash)) = declare_info {
            declared_classes.push(DeclaredClass {
                class_hash: class_hash.into(),
                compiled_class_hash: compiled_class_hash.map(Into::into),
                transaction_hash: tx_hash,
            });
        }

        // Extract deployed contracts from Deploy and DeployAccount receipts
        match &receipt {
            TransactionReceipt::Deploy(deploy_receipt) => {
                deployed_contracts.push(DeployedContract {
                    contract_address: deploy_receipt.contract_address.into(),
                    class_hash: Felt::ZERO, // Old Deploy doesn't have class_hash accessible easily
                    transaction_hash: tx_hash,
                });
            }
            TransactionReceipt::DeployAccount(deploy_account_receipt) => {
                deployed_contracts.push(DeployedContract {
                    contract_address: deploy_account_receipt.contract_address.into(),
                    class_hash: deploy_account_class.map(Into::into).unwrap_or(Felt::ZERO),
                    transaction_hash: tx_hash,
                });
            }
            _ => {}
        }

        // Extract events from receipt
        let events = match receipt {
            TransactionReceipt::Invoke(r) => r.events,
            TransactionReceipt::L1Handler(r) => r.events,
            TransactionReceipt::Declare(r) => r.events,
            TransactionReceipt::Deploy(r) => r.events,
            TransactionReceipt::DeployAccount(r) => r.events,
        };

        all_events.extend(events.into_iter().map(|e| {
            StarknetEvent::new(
                e.from_address.into(),
                e.keys.into_iter().map(Into::into).collect(),
                e.data.into_iter().map(Into::into).collect(),
                block_with_receipts.block_number,
                tx_hash,
            )
        }))
    }

    tracing::debug!(
        target: "torii::etl::block_range",
        block_number = block_with_receipts.block_number,
        processed_transactions,
        skipped_reverted,
        emitted_events_after_filter = all_events.len(),
        "Processed block receipts with reverted transaction filtering"
    );

    Ok(BlockData {
        block_context,
        transactions: transaction_contexts,
        events: all_events,
        declared_classes,
        deployed_contracts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_success_predicate_handles_succeeded_and_reverted() {
        assert!(is_execution_succeeded(&ExecutionResult::Succeeded));
        assert!(!is_execution_succeeded(&ExecutionResult::Reverted {
            reason: "reverted".to_string(),
        }));
    }
}

/// Parsed contract ABI
///
/// Simplified representation of a contract's ABI for identification purposes.
/// Contains function and event signatures extracted from the contract class.
#[derive(Debug, Clone)]
pub struct ContractAbi {
    pub abi: Option<Vec<AbiEntry>>,
    pub legacy_abi: Option<Vec<LegacyContractAbiEntry>>,
    functions: HashSet<String>,
    events: HashSet<String>,
}

impl ContractAbi {
    fn extract_function_name(name: &str) -> String {
        name.to_string()
    }

    fn extract_event_name(name: &str) -> String {
        name.to_string()
    }

    #[allow(clippy::match_wildcard_for_single_variants)]
    pub fn from_contract_class(class: ContractClass) -> Result<Self> {
        let mut abi: Option<Vec<AbiEntry>> = None;
        let mut legacy_abi: Option<Vec<LegacyContractAbiEntry>> = None;
        let mut functions = HashSet::new();
        let mut events = HashSet::new();

        match class {
            ContractClass::Sierra(sierra) => {
                let parsed_abi: Vec<AbiEntry> = serde_json::from_str(&sierra.abi)?;
                for entry in &parsed_abi {
                    match entry {
                        AbiEntry::Function(func) => {
                            functions.insert(Self::extract_function_name(&func.name));
                        }
                        AbiEntry::Interface(interface) => {
                            for item in &interface.items {
                                if let AbiEntry::Function(func) = item {
                                    functions.insert(Self::extract_function_name(&func.name));
                                }
                            }
                        }
                        AbiEntry::Event(event) => {
                            use starknet::core::types::contract::AbiEvent;
                            match event {
                                AbiEvent::Typed(TypedAbiEvent::Struct(s)) => {
                                    events.insert(Self::extract_event_name(&s.name));
                                }
                                AbiEvent::Typed(TypedAbiEvent::Enum(e)) => {
                                    events.insert(Self::extract_event_name(&e.name));
                                    for variant in &e.variants {
                                        events.insert(Self::extract_event_name(&variant.name));
                                    }
                                }
                                AbiEvent::Untyped(u) => {
                                    events.insert(Self::extract_event_name(&u.name));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                abi = Some(parsed_abi);
            }
            ContractClass::Legacy(legacy) => {
                if let Some(ref legacy_abi_vec) = legacy.abi {
                    for entry in legacy_abi_vec {
                        match entry {
                            LegacyContractAbiEntry::Function(func) => {
                                functions.insert(Self::extract_function_name(&func.name));
                            }
                            LegacyContractAbiEntry::Event(event) => {
                                events.insert(Self::extract_event_name(&event.name));
                            }
                            _ => {}
                        }
                    }
                }
                legacy_abi = legacy.abi;
            }
        }

        Ok(Self {
            abi,
            legacy_abi,
            functions,
            events,
        })
    }

    pub fn has_function(&self, name: &str) -> bool {
        if self.functions.contains(name) {
            return true;
        }
        let suffix = format!("::{name}");
        self.functions.iter().any(|f| f.ends_with(&suffix))
    }

    pub fn has_event(&self, name: &str) -> bool {
        if self.events.contains(name) {
            return true;
        }
        let suffix = format!("::{name}");
        self.events.iter().any(|e| e.ends_with(&suffix))
    }
}
