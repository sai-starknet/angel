use starknet_types_raw::Felt;

#[derive(Debug, Clone)]
pub struct StarknetEvent {
    pub from_address: Felt,
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventContext {
    pub from_address: Felt,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MissingBlockNumber;

impl std::fmt::Display for MissingBlockNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Event is missing block number")
    }
}

impl std::error::Error for MissingBlockNumber {}

impl std::fmt::Display for EventContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "from={:#066x} block={} tx={:#066x}",
            self.from_address, self.block_number, self.transaction_hash
        )
    }
}

#[cfg(feature = "field")]
mod field {
    use super::{MissingBlockNumber, StarknetEvent};
    use starknet::core::types::EmittedEvent as SnEmittedEvent;
    impl TryFrom<SnEmittedEvent> for StarknetEvent {
        type Error = MissingBlockNumber;

        fn try_from(event: SnEmittedEvent) -> Result<Self, Self::Error> {
            Ok(Self {
                from_address: event.from_address.into(),
                keys: event.keys.into_iter().map(Into::into).collect(),
                data: event.data.into_iter().map(Into::into).collect(),
                block_number: event.block_number.ok_or(MissingBlockNumber)?,
                transaction_hash: event.transaction_hash.into(),
            })
        }
    }
}

impl StarknetEvent {
    pub fn new(
        from_address: Felt,
        keys: Vec<Felt>,
        data: Vec<Felt>,
        block_number: u64,
        transaction_hash: Felt,
    ) -> Self {
        StarknetEvent {
            from_address,
            keys,
            data,
            block_number,
            transaction_hash,
        }
    }
    pub fn context(&self) -> EventContext {
        EventContext {
            from_address: self.from_address,
            block_number: self.block_number,
            transaction_hash: self.transaction_hash,
        }
    }

    pub fn filter_pending(events: Vec<impl TryInto<StarknetEvent>>) -> Vec<Self> {
        events
            .into_iter()
            .filter_map(|e| e.try_into().ok())
            .collect()
    }
}
