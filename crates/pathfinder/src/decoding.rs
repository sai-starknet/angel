use crate::sqlite::BlockEventsRow;
use crate::{PFError, PFResult};
use serde::{Deserialize, Serialize};
use starknet_types_raw::event::Event;
use starknet_types_raw::Felt;
use std::cell::RefCell;
use std::fmt::{Formatter, Result as FmtResult};
use std::io::Result as IoResult;
use std::sync::LazyLock;
use zstd::bulk::Decompressor;

// Taken from pathfinder-common but with some optimizations

const MAX_EVENTS_UNCOMPRESSED_SIZE: usize = 128usize * 1024 * 1024;

static ZSTD_EVENTS_DECODER_DICTIONARY: LazyLock<zstd::dict::DecoderDictionary<'static>> =
    LazyLock::new(|| zstd::dict::DecoderDictionary::new(include_bytes!("assets/events.zdict")));

thread_local! {
    static EVENT_DECOMPRESSOR: RefCell<Decompressor<'static>> = RefCell::new(
        Decompressor::with_prepared_dictionary(
            &ZSTD_EVENTS_DECODER_DICTIONARY
        ).expect("failed to create decompressor")
    );
}

/// Minimally encoded Felt value.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct MinimalFelt([u8; 32]);

impl serde::Serialize for MinimalFelt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = self.0;
        let zeros = bytes.iter().take_while(|&&x| x == 0).count();
        bytes[zeros..].serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for MinimalFelt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = MinimalFelt;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
                formatter.write_str("a sequence")
            }

            fn visit_seq<B>(self, mut seq: B) -> Result<Self::Value, B::Error>
            where
                B: serde::de::SeqAccess<'de>,
            {
                let len = seq.size_hint().unwrap();
                let mut bytes = [0; 32];
                let num_zeros = bytes.len() - len;
                let mut i = num_zeros;
                while let Some(value) = seq.next_element()? {
                    bytes[i] = value;
                    i += 1;
                }
                Ok(MinimalFelt(bytes))
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}

impl MinimalFelt {
    #[allow(unsafe_code)]
    fn into_felt_vec(v: Vec<Self>) -> Vec<Felt> {
        // Safe: both repr(transparent) over [u8; 32]
        unsafe {
            let mut v = std::mem::ManuallyDrop::new(v);
            Vec::from_raw_parts(v.as_mut_ptr().cast::<Felt>(), v.len(), v.capacity())
        }
    }
}

impl From<MinimalFelt> for Felt {
    fn from(value: MinimalFelt) -> Self {
        value.0.into()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum EventsForBlock {
    V0 { events: Vec<Vec<EncodedEvent>> },
}

impl EventsForBlock {
    pub fn events(self) -> Vec<Vec<Event>> {
        match self {
            EventsForBlock::V0 { events } => events
                .into_iter()
                .map(|evs| evs.into_iter().map(Event::from).collect())
                .collect(),
        }
    }
}

pub(crate) fn decompress_events(input: &[u8]) -> IoResult<Vec<u8>> {
    EVENT_DECOMPRESSOR.with(|d| {
        d.borrow_mut()
            .decompress(input, MAX_EVENTS_UNCOMPRESSED_SIZE)
    })
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EncodedEvent {
    pub data: Vec<MinimalFelt>,
    pub from_address: MinimalFelt,
    pub keys: Vec<MinimalFelt>,
}

impl From<EncodedEvent> for Event {
    fn from(value: EncodedEvent) -> Self {
        Self {
            data: MinimalFelt::into_felt_vec(value.data),
            from_address: value.from_address.into(),
            keys: MinimalFelt::into_felt_vec(value.keys),
        }
    }
}

impl BlockEventsRow {
    pub fn decompress_events(&self) -> PFResult<Vec<Vec<Event>>> {
        match self.events {
            None => Ok(Vec::new()),
            Some(ref events) => {
                let events = decompress_events(events)?;
                let events: EventsForBlock =
                    bincode::serde::decode_from_slice(&events, bincode::config::standard())?.0;
                Ok(events.events())
            }
        }
    }
}

impl TryFrom<BlockEventsRow> for BlockEvents {
    type Error = PFError;

    fn try_from(value: BlockEventsRow) -> Result<Self, Self::Error> {
        Ok(Self {
            block_number: value.block_number,
            transactions: value.decompress_events()?,
        })
    }
}

#[derive(Debug)]
pub struct BlockEvents {
    pub block_number: u64,
    pub transactions: Vec<Vec<Event>>,
}
