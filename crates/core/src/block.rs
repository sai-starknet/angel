use sai_conversion::ItemsInto;
use sai_felt::Felt;
use sai_starknet_types::{
    BlockStatus, BlockWithReceipts, DeclareTransactionReceipt, DeployAccountTransactionReceipt,
    DeployTransactionReceipt, Event, ExecutionResources, ExecutionResult, FeePayment, Hash256,
    InvokeTransactionReceipt, L1DataAvailabilityMode, L1HandlerTransactionReceipt, MsgToL1,
    ResourcePrice, TransactionContent, TransactionFinalityStatus, TransactionReceipt,
    TransactionWithReceipt,
};
use serde::{Deserialize, Serialize};

use crate::FeltMap;

/// Block with transactions and receipts.
///
/// The block object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockEventMap {
    /// Status
    pub status: BlockStatus,
    /// Block hash
    pub block_hash: Felt,
    /// The hash of this block's parent
    pub parent_hash: Felt,
    /// The block number (its height)
    pub block_number: u64,
    /// The new global state root
    pub new_root: Felt,
    /// The time in which the block was created, encoded in Unix time
    pub timestamp: u64,
    /// The Starknet identity of the sequencer submitting this block
    pub sequencer_address: Felt,
    /// The price of L1 gas in the block
    pub l1_gas_price: ResourcePrice,
    /// The price of L2 gas in the block
    pub l2_gas_price: ResourcePrice,
    /// The price of L1 data gas in the block
    pub l1_data_gas_price: ResourcePrice,
    /// Specifies whether the data of this block is published via blob data or calldata
    pub l1_da_mode: L1DataAvailabilityMode,
    /// Semver of the current Starknet protocol
    pub starknet_version: String,
    /// The transactions in this block
    pub transactions: Vec<TransactionWithReceipt>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockEvent {
    pub index: u32,
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
}

/// Transaction and receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionWithReceiptMap {
    /// Transaction
    pub transaction: TransactionContent,
    /// Receipt
    pub receipt: TransactionReceiptMap,
}

/// Starknet transaction receipt containing execution results.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum TransactionReceiptMap {
    /// Receipt for an `INVOKE` transaction.
    Invoke(InvokeTransactionReceiptMap),
    /// Receipt for an `L1_HANDLER` transaction.
    L1Handler(L1HandlerTransactionReceiptMap),
    /// Receipt for a `DECLARE` transaction.
    Declare(DeclareTransactionReceiptMap),
    /// Receipt for a `DEPLOY` transaction.
    Deploy(DeployTransactionReceiptMap),
    /// Receipt for a `DEPLOY_ACCOUNT` transaction.
    DeployAccount(DeployAccountTransactionReceiptMap),
}

/// Invoke transaction receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct InvokeTransactionReceiptMap {
    /// The hash identifying the transaction
    pub transaction_hash: Felt,
    /// The fee that was charged by the sequencer
    pub actual_fee: FeePayment,
    /// Finality status of the tx
    pub finality_status: TransactionFinalityStatus,
    /// Messages sent
    pub messages_sent: Vec<MsgToL1>,
    /// The events emitted as part of this transaction
    pub events: FeltMap<Vec<BlockEvent>>,
    /// The resources consumed by the transaction
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
}

/// L1 handler transaction receipt.
///
/// Receipt for L1 handler transaction.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct L1HandlerTransactionReceiptMap {
    /// The message hash as it appears on the L1 core contract
    pub message_hash: Hash256,
    /// The hash identifying the transaction
    pub transaction_hash: Felt,
    /// The fee that was charged by the sequencer
    pub actual_fee: FeePayment,
    /// Finality status of the tx
    pub finality_status: TransactionFinalityStatus,
    /// Messages sent
    pub messages_sent: Vec<MsgToL1>,
    /// The events emitted as part of this transaction
    pub events: FeltMap<Vec<BlockEvent>>,
    /// The resources consumed by the transaction
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
}

/// Declare transaction receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DeclareTransactionReceiptMap {
    /// The hash identifying the transaction
    pub transaction_hash: Felt,
    /// The fee that was charged by the sequencer
    pub actual_fee: FeePayment,
    /// Finality status of the tx
    pub finality_status: TransactionFinalityStatus,
    /// Messages sent
    pub messages_sent: Vec<MsgToL1>,
    /// The events emitted as part of this transaction
    pub events: FeltMap<Vec<BlockEvent>>,
    /// The resources consumed by the transaction
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
}

/// Deploy transaction receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DeployTransactionReceiptMap {
    /// The hash identifying the transaction
    pub transaction_hash: Felt,
    /// The fee that was charged by the sequencer
    pub actual_fee: FeePayment,
    /// Finality status of the tx
    pub finality_status: TransactionFinalityStatus,
    /// Messages sent
    pub messages_sent: Vec<MsgToL1>,
    /// The events emitted as part of this transaction
    pub events: FeltMap<Vec<BlockEvent>>,
    /// The resources consumed by the transaction
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
    /// The address of the deployed contract
    pub contract_address: Felt,
}

/// Deploy account transaction receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DeployAccountTransactionReceiptMap {
    /// The hash identifying the transaction
    pub transaction_hash: Felt,
    /// The fee that was charged by the sequencer
    pub actual_fee: FeePayment,
    /// Finality status of the tx
    pub finality_status: TransactionFinalityStatus,
    /// Messages sent
    pub messages_sent: Vec<MsgToL1>,
    /// The events emitted as part of this transaction
    pub events: FeltMap<Vec<BlockEvent>>,
    /// The resources consumed by the transaction
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
    /// The address of the deployed contract
    pub contract_address: Felt,
}

pub fn map_events(events: Vec<Event>) -> FeltMap<Vec<BlockEvent>> {
    let mut map = FeltMap::default();
    for (index, event) in events.into_iter().enumerate() {
        map.entry(event.from_address)
            .or_insert_with(Vec::new)
            .push(BlockEvent {
                index: index as u32,
                keys: event.keys,
                data: event.data,
            });
    }
    map
}

pub fn vectorize_events(events: FeltMap<Vec<BlockEvent>>) -> Result<Vec<Event>> {
    let total = events.values().map(|v| v.len()).sum();
    let mut vec: Vec<Option<Event>> = vec![None; total];
    for (from_address, block_events) in events.into_iter() {
        for e in block_events {
            match vec.get_mut(e.index as usize) {
                Some(Some(i)) => {
                    return Err(EventVectorizationError::DuplicateIndexes {
                        from_addresses: (from_address, i.from_address),
                        index: e.index,
                    })
                }
                Some(i) => {
                    *i = Some(Event {
                        from_address,
                        keys: e.keys,
                        data: e.data,
                    });
                }
                None => {
                    return Err(EventVectorizationError::IndexOutOfBounds {
                        from_address,
                        index: e.index,
                        bound: total,
                    })
                }
            }
        }
    }
    if vec.iter().any(|e| e.is_none()) {
        Err(EventVectorizationError::MissingIndexes(
            vec.iter()
                .enumerate()
                .filter_map(|(i, e)| e.is_none().then_some(i))
                .collect(),
        ))
    } else {
        Ok(vec.into_iter().map(Option::unwrap).collect())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum EventVectorizationError {
    DuplicateIndexes {
        from_addresses: (Felt, Felt),
        index: u32,
    },
    MissingIndexes(Vec<usize>),
    IndexOutOfBounds {
        from_address: Felt,
        index: u32,
        bound: usize,
    },
}

impl std::fmt::Display for EventVectorizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventVectorizationError::DuplicateIndexes {
                from_addresses: (a1, a2),
                index,
            } => write!(
                f,
                "Duplicate event index {index} for from_addresses {a1} and {a2}"
            ),
            EventVectorizationError::MissingIndexes(indexes) => write!(
                f,
                "Missing event indexes {indexes:?}"
            ),
            EventVectorizationError::IndexOutOfBounds {
                from_address,
                index,
                bound,
            } => write!(
                f,
                "Event index {index} for from_address {from_address} is out of bounds (bound: {bound})"
            ),
        }
    }
}

impl From<InvokeTransactionReceipt> for InvokeTransactionReceiptMap {
    fn from(receipt: InvokeTransactionReceipt) -> Self {
        InvokeTransactionReceiptMap {
            transaction_hash: receipt.transaction_hash,
            actual_fee: receipt.actual_fee,
            finality_status: receipt.finality_status,
            messages_sent: receipt.messages_sent,
            events: map_events(receipt.events),
            execution_resources: receipt.execution_resources,
            execution_result: receipt.execution_result,
        }
    }
}

impl From<BlockWithReceipts> for BlockEventMap {
    fn from(value: BlockWithReceipts) -> Self {
        BlockEventMap {
            status: value.status,
            block_hash: value.block_hash,
            parent_hash: value.parent_hash,
            block_number: value.block_number,
            new_root: value.new_root,
            timestamp: value.timestamp,
            sequencer_address: value.sequencer_address,
            l1_gas_price: value.l1_gas_price,
            l2_gas_price: value.l2_gas_price,
            l1_data_gas_price: value.l1_data_gas_price,
            l1_da_mode: value.l1_da_mode,
            starknet_version: value.starknet_version,
            transactions: value.transactions.into_iter().map(Into::into).collect(),
        }
    }
}
impl From<TransactionWithReceipt> for TransactionWithReceiptMap {
    fn from(value: TransactionWithReceipt) -> Self {
        TransactionWithReceiptMap {
            transaction: value.transaction,
            receipt: value.receipt.into(),
        }
    }
}
impl From<TransactionReceipt> for TransactionReceiptMap {
    fn from(value: TransactionReceipt) -> Self {
        match value {
            TransactionReceipt::Invoke(receipt) => TransactionReceiptMap::Invoke(receipt.into()),
            TransactionReceipt::L1Handler(receipt) => {
                TransactionReceiptMap::L1Handler(receipt.into())
            }
            TransactionReceipt::Declare(receipt) => TransactionReceiptMap::Declare(receipt.into()),
            TransactionReceipt::Deploy(receipt) => TransactionReceiptMap::Deploy(receipt.into()),
            TransactionReceipt::DeployAccount(receipt) => {
                TransactionReceiptMap::DeployAccount(receipt.into())
            }
        }
    }
}
impl From<L1HandlerTransactionReceipt> for L1HandlerTransactionReceiptMap {
    fn from(value: L1HandlerTransactionReceipt) -> Self {
        L1HandlerTransactionReceiptMap {
            message_hash: value.message_hash,
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: map_events(value.events),
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
        }
    }
}
impl From<DeclareTransactionReceipt> for DeclareTransactionReceiptMap {
    fn from(value: DeclareTransactionReceipt) -> Self {
        DeclareTransactionReceiptMap {
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: map_events(value.events),
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
        }
    }
}
impl From<DeployTransactionReceipt> for DeployTransactionReceiptMap {
    fn from(value: DeployTransactionReceipt) -> Self {
        DeployTransactionReceiptMap {
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: map_events(value.events),
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
            contract_address: value.contract_address,
        }
    }
}
impl From<DeployAccountTransactionReceipt> for DeployAccountTransactionReceiptMap {
    fn from(value: DeployAccountTransactionReceipt) -> Self {
        DeployAccountTransactionReceiptMap {
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: map_events(value.events),
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
            contract_address: value.contract_address,
        }
    }
}

impl TryFrom<BlockEventMap> for BlockWithReceipts {
    type Error = EventVectorizationError;
    fn try_from(value: BlockEventMap) -> Result<Self, Self::Error> {
        Ok(BlockWithReceipts {
            status: value.status,
            block_hash: value.block_hash,
            parent_hash: value.parent_hash,
            block_number: value.block_number,
            new_root: value.new_root,
            timestamp: value.timestamp,
            sequencer_address: value.sequencer_address,
            l1_gas_price: value.l1_gas_price,
            l2_gas_price: value.l2_gas_price,
            l1_data_gas_price: value.l1_data_gas_price,
            l1_da_mode: value.l1_da_mode,
            starknet_version: value.starknet_version,
            transactions: value
                .transactions
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<TransactionWithReceiptMap> for TransactionWithReceipt {
    type Error = EventVectorizationError;
    fn try_from(value: TransactionWithReceiptMap) -> Result<Self, Self::Error> {
        Ok(TransactionWithReceipt {
            transaction: value.transaction,
            receipt: value.receipt.try_into()?,
        })
    }
}

impl TryFrom<TransactionReceiptMap> for TransactionReceipt {
    type Error = EventVectorizationError;
    fn try_from(value: TransactionReceiptMap) -> Result<Self, Self::Error> {
        Ok(match value {
            TransactionReceiptMap::Invoke(r) => TransactionReceipt::Invoke(r.try_into()?),
            TransactionReceiptMap::L1Handler(r) => TransactionReceipt::L1Handler(r.try_into()?),
            TransactionReceiptMap::Declare(r) => TransactionReceipt::Declare(r.try_into()?),
            TransactionReceiptMap::Deploy(r) => TransactionReceipt::Deploy(r.try_into()?),
            TransactionReceiptMap::DeployAccount(r) => {
                TransactionReceipt::DeployAccount(r.try_into()?)
            }
        })
    }
}

impl TryFrom<InvokeTransactionReceiptMap> for InvokeTransactionReceipt {
    type Error = EventVectorizationError;
    fn try_from(value: InvokeTransactionReceiptMap) -> Result<Self, Self::Error> {
        Ok(InvokeTransactionReceipt {
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: vectorize_events(value.events)?,
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
        })
    }
}

impl TryFrom<L1HandlerTransactionReceiptMap> for L1HandlerTransactionReceipt {
    type Error = EventVectorizationError;
    fn try_from(value: L1HandlerTransactionReceiptMap) -> Result<Self, Self::Error> {
        Ok(L1HandlerTransactionReceipt {
            message_hash: value.message_hash,
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: vectorize_events(value.events)?,
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
        })
    }
}

impl TryFrom<DeclareTransactionReceiptMap> for DeclareTransactionReceipt {
    type Error = EventVectorizationError;
    fn try_from(value: DeclareTransactionReceiptMap) -> Result<Self, Self::Error> {
        Ok(DeclareTransactionReceipt {
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: vectorize_events(value.events)?,
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
        })
    }
}

impl TryFrom<DeployTransactionReceiptMap> for DeployTransactionReceipt {
    type Error = EventVectorizationError;
    fn try_from(value: DeployTransactionReceiptMap) -> Result<Self, Self::Error> {
        Ok(DeployTransactionReceipt {
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: vectorize_events(value.events)?,
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
            contract_address: value.contract_address,
        })
    }
}

impl TryFrom<DeployAccountTransactionReceiptMap> for DeployAccountTransactionReceipt {
    type Error = EventVectorizationError;
    fn try_from(value: DeployAccountTransactionReceiptMap) -> Result<Self, Self::Error> {
        Ok(DeployAccountTransactionReceipt {
            transaction_hash: value.transaction_hash,
            actual_fee: value.actual_fee,
            finality_status: value.finality_status,
            messages_sent: value.messages_sent,
            events: vectorize_events(value.events)?,
            execution_resources: value.execution_resources,
            execution_result: value.execution_result,
            contract_address: value.contract_address,
        })
    }
}
