use dojo_introspect::DojoIntrospectError;
use introspect_types::transcode::TranscodeError;
use introspect_types::DecodeError;
use starknet::core::utils::NonAsciiNameError;
use starknet_types_core::felt::Felt;
use std::error::Error as StdError;
use std::sync::PoisonError;

#[derive(Debug, thiserror::Error)]
pub enum DojoToriiError {
    #[error("Unknown Dojo Event selector {0:#066x}")]
    UnknownDojoEventSelector(Felt),
    #[error("Missing event selector")]
    MissingEventSelector,
    #[error("Column {1:#066x} not found in table {0}")]
    ColumnNotFound(String, Felt),
    #[error("Failed to parse field {0:#066x} in table {1}")]
    FieldParseError(Felt, String),
    #[error("Too many values provided for field {0:#066x}")]
    TooManyFieldValues(Felt),
    #[error("Failed to parse values for table {0}")]
    ParseValuesError(String),
    #[error("Cannot add {2} table already exists with id {0:#066x} and name {1}")]
    TableAlreadyExists(Felt, String, String),
    #[error("Table not found with id {0:#066x}")]
    TableNotFoundById(Felt),
    #[error("Failed to acquire lock: {0}")]
    LockError(String),
    #[error("Starknet selector error: {0}")]
    StarknetSelectorError(#[from] NonAsciiNameError),
    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),
    #[error(transparent)]
    DecodeError(#[from] DecodeError),
    #[error("Failed to deserialize event data for {0}: {1:?}")]
    EventDeserializationError(&'static str, DecodeError),
    #[error(transparent)]
    DojoIntrospectError(#[from] DojoIntrospectError),
    #[error("Transcode error: {0:?}")]
    TranscodeError(TranscodeError<DecodeError, ()>),
    #[error("Store error: {0}")]
    StoreError(#[source] Box<dyn StdError + Send + Sync>),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

pub type DojoToriiResult<T = ()> = std::result::Result<T, DojoToriiError>;

impl From<TranscodeError<DecodeError, ()>> for DojoToriiError {
    fn from(err: TranscodeError<DecodeError, ()>) -> Self {
        Self::TranscodeError(err)
    }
}

impl DojoToriiError {
    pub fn store_error<T: StdError + Send + Sync + 'static>(err: T) -> Self {
        Self::StoreError(Box::new(err))
    }
}

impl<T> From<PoisonError<T>> for DojoToriiError {
    fn from(err: PoisonError<T>) -> Self {
        Self::LockPoisoned(err.to_string())
    }
}
