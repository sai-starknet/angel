use crate::block::BlockEventMap;

use super::FeltMap;
use sai_starknet_types::Felt;

#[derive(Debug, Clone, Default)]
pub enum ExtractedEvents {
    #[default]
    None,
    Contract(FeltMap<ContractEvents>),
    Block(BlockEventMap),
}

#[derive(Debug, Clone, Default)]
pub struct ContractEvents {
    pub contract_address: Felt,
    pub transactions: Vec<TransactionEvents>,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionEvents {
    pub block_number: u64,
    pub transaction_hash: Felt,
    pub events: Vec<EventData>,
}

#[derive(Debug, Clone, Default)]
pub struct EventData {
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
}

// #[derive(Debug, Clone)]
// pub struct BlockEvents {
//     pub block: BlockWithReceipts,
//     pub events: Vec<BlockTransactionEvents>,
// }

// #[derive(Debug, Clone, Default)]
// pub struct BlockTransactionEvents {
//     pub transaction_hash: Felt,
//     pub contract_events: FeltMap<Vec<BlockEvent>>,
// }

// impl From<BlockWithReceipts> for BlockEvents {
//     fn from(mut block: BlockWithReceipts) -> Self {
//         let mut events = Vec::with_capacity(block.transactions.len());
//         block.extract_block_events(&mut events);
//         BlockEvents { block, events }
//     }
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
// pub struct EventAddressNotFound(pub Felt);

// pub type EventExtractResult<T = ()> = std::result::Result<T, EventAddressNotFound>;

// impl std::fmt::Display for EventAddressNotFound {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "Event address not found: {:#066x}", self.0)
//     }
// }

// impl std::error::Error for EventAddressNotFound {}

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
// pub enum ExtractError {
//     EventAddressNotFound(Felt, Felt),
//     EventsMissing(Felt),
//     TransactionMissMatch(Felt, Felt),
// }

// impl std::fmt::Display for ExtractError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             ExtractError::EventAddressNotFound(tx_hash, addr) => {
//                 write!(
//                     f,
//                     "Transaction {:#066x} event address not found: {:#066x}",
//                     tx_hash, addr
//                 )
//             }
//             ExtractError::EventsMissing(tx_hash) => {
//                 write!(f, "Events missing for transaction {:#066x}", tx_hash)
//             }
//             ExtractError::TransactionMissMatch(existing, new) => write!(
//                 f,
//                 "Expected transaction hash {:#066x} does not match actual hash {:#066x}",
//                 existing, new
//             ),
//         }
//     }
// }

// impl From<Felt> for EventAddressNotFound {
//     fn from(value: Felt) -> Self {
//         EventAddressNotFound(value)
//     }
// }

// pub trait ExtractBlockTransactionEvents {
//     fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>);
// }

// impl ExtractBlockTransactionEvents for BlockWithReceipts {
//     fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>) {
//         for tx in self.transactions.iter_mut() {
//             tx.extract_block_events(contract_txs);
//         }
//     }
// }

// impl ExtractBlockTransactionEvents for TransactionWithReceipt {
//     fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>) {
//         self.receipt.extract_block_events(contract_txs);
//     }
// }

// impl ExtractBlockTransactionEvents for TransactionReceipt {
//     fn extract_block_events(&mut self, contract_txs: &mut Vec<BlockTransactionEvents>) {
//         contract_txs.push(BlockTransactionEvents {
//             transaction_hash: *self.transaction_hash(),
//             contract_events: self.take_event_datas(),
//         })
//     }
// }

// pub trait ExtractEventData<'a> {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>);
//     fn take_event_datas(&'a mut self) -> FeltMap<Vec<BlockEvent>> {
//         let mut events = FeltMap::default();
//         self.extract_event_data(&mut events);
//         events
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult;
// }

// impl<'a> ExtractEventData<'a> for Vec<Event> {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>) {
//         for (index, event) in self.iter_mut().enumerate() {
//             events
//                 .entry(event.from_address)
//                 .or_default()
//                 .push(BlockEvent {
//                     index: index as u32,
//                     keys: mem::take(&mut event.keys),
//                     data: mem::take(&mut event.data),
//                 });
//         }
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult {
//         for event in self.iter_mut() {
//             match events
//                 .get_mut(&event.from_address)
//                 .and_then(|e| e.pop_front())
//             {
//                 Some(data) => {
//                     event.keys = data.keys;
//                     event.data = data.data;
//                 }
//                 None => return Err(EventAddressNotFound(event.from_address)),
//             }
//         }
//         Ok(())
//     }
// }

// impl<'a> ExtractEventData<'a> for InvokeTransactionReceipt {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>) {
//         self.events.extract_event_data(events);
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult {
//         self.events.rebuild_events(events)
//     }
// }

// impl<'a> ExtractEventData<'a> for L1HandlerTransactionReceipt {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>) {
//         self.events.extract_event_data(events);
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult {
//         self.events.rebuild_events(events)
//     }
// }

// impl<'a> ExtractEventData<'a> for DeclareTransactionReceipt {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>) {
//         self.events.extract_event_data(events);
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult {
//         self.events.rebuild_events(events)
//     }
// }

// impl<'a> ExtractEventData<'a> for DeployTransactionReceipt {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>) {
//         self.events.extract_event_data(events);
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult {
//         self.events.rebuild_events(events)
//     }
// }

// impl<'a> ExtractEventData<'a> for DeployAccountTransactionReceipt {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>) {
//         self.events.extract_event_data(events);
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult {
//         self.events.rebuild_events(events)
//     }
// }

// impl<'a> ExtractEventData<'a> for TransactionReceipt {
//     fn extract_event_data(&'a mut self, events: &mut FeltMap<Vec<BlockEvent>>) {
//         match self {
//             TransactionReceipt::Invoke(invoke) => invoke.extract_event_data(events),
//             TransactionReceipt::L1Handler(l1_handler) => l1_handler.extract_event_data(events),
//             TransactionReceipt::Declare(declare) => declare.extract_event_data(events),
//             TransactionReceipt::Deploy(deploy) => deploy.extract_event_data(events),
//             TransactionReceipt::DeployAccount(deploy_account) => {
//                 deploy_account.extract_event_data(events)
//             }
//         }
//     }
//     fn rebuild_events(&mut self, events: &mut FeltMap<VecDeque<BlockEvent>>) -> EventExtractResult {
//         match self {
//             TransactionReceipt::Invoke(invoke) => invoke.rebuild_events(events),
//             TransactionReceipt::L1Handler(l1_handler) => l1_handler.rebuild_events(events),
//             TransactionReceipt::Declare(declare) => declare.rebuild_events(events),
//             TransactionReceipt::Deploy(deploy) => deploy.rebuild_events(events),
//             TransactionReceipt::DeployAccount(deploy_account) => {
//                 deploy_account.rebuild_events(events)
//             }
//         }
//     }
// }

// impl ContractEvents {
//     pub fn new(contract_address: Felt, transactions: Vec<TransactionEvents>) -> Self {
//         ContractEvents {
//             contract_address,
//             transactions,
//         }
//     }
// }

// impl From<BlockTransactionEvents> for (Felt, FeltMap<VecDeque<BlockEvent>>) {
//     fn from(value: BlockTransactionEvents) -> Self {
//         (value.transaction_hash, value.contract_events.values_into())
//     }
// }

// impl TryFrom<BlockEvents> for BlockWithReceipts {
//     type Error = ExtractError;
//     fn try_from(value: BlockEvents) -> Result<Self, Self::Error> {
//         let mut block = value.block;
//         let mut extracted: VecDeque<_> = value.events.into();
//         for tx in block.transactions.iter_mut() {
//             let tx_hash = *tx.receipt.transaction_hash();
//             let events = extracted
//                 .pop_front()
//                 .ok_or(ExtractError::EventsMissing(tx_hash))?;
//             if tx_hash != events.transaction_hash {
//                 return Err(ExtractError::TransactionMissMatch(
//                     tx_hash,
//                     events.transaction_hash,
//                 ));
//             }
//             let mut events = events.contract_events.values_into();
//             tx.receipt
//                 .rebuild_events(&mut events)
//                 .map_err(|e| ExtractError::EventAddressNotFound(tx_hash, e.0))?;
//         }
//         Ok(block)
//     }
// }
