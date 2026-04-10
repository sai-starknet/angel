use crate::TableKey;
use introspect_types::{DecodeError, PrimaryTypeDef, TypeDef};
use sqlx::error::BoxDynError;
use sqlx::Error as SqlxError;
use starknet_types_raw::error::FromStrError;
use starknet_types_raw::Felt;
use std::fmt::Display;
use std::sync::PoisonError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TypeError {
    #[error("Unsupported type for {0}")]
    UnsupportedType(String),
    #[error("Nested arrays are not supported")]
    NestedArrays,
}

pub type TypeResult<T> = std::result::Result<T, TypeError>;

#[derive(Debug, Error)]
pub enum TableError {
    #[error("Table {0} has not got columns: {1:?}")]
    ColumnsNotFound(String, ColumnsNotFoundError),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error("Current type mismatch error")]
    TypeMismatch,
    #[error("Unsupported upgrade for table {table} column {column}: {reason}")]
    UnsupportedUpgrade {
        table: String,
        column: String,
        reason: UpgradeError,
    },
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error("error occurred while encoding a value: {0}")]
    Encode(#[from] BoxDynError),
}

#[derive(Debug)]
pub struct ColumnNotFoundError(pub Felt);

#[derive(Debug, Default, Error)]
pub struct ColumnsNotFoundError(pub Vec<Felt>);

impl ColumnsNotFoundError {
    pub fn to_table_error(self, table: &str) -> TableError {
        TableError::ColumnsNotFound(table.to_string(), self)
    }
}

impl Display for ColumnsNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some((first, rest)) = self.0.split_first() {
            write!(f, "0x{first:#063x}")?;
            for col in rest {
                write!(f, ", 0x{col:#063x}")?;
            }
        }
        Ok(())
    }
}

pub trait CollectColumnResults<T> {
    fn collect_columns(self) -> Result<Vec<T>, ColumnsNotFoundError>;
}

impl<T, I> CollectColumnResults<T> for I
where
    I: Iterator<Item = Result<T, ColumnNotFoundError>>,
{
    fn collect_columns(self) -> Result<Vec<T>, ColumnsNotFoundError> {
        let mut columns = Vec::new();
        let mut not_found = Vec::new();
        for result in self {
            match result {
                Ok(col) => columns.push(col),
                Err(e) => not_found.push(e.0),
            }
        }
        if not_found.is_empty() {
            Ok(columns)
        } else {
            Err(ColumnsNotFoundError(not_found))
        }
    }
}

pub type TableResult<T = ()> = std::result::Result<T, TableError>;

#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    #[error("Failed to upgrade type from {old} to {new}")]
    TypeUpgradeError {
        old: &'static str,
        new: &'static str,
    },
    #[error("Failed to upgrade primary from {old} to {new}")]
    PrimaryUpgradeError {
        old: &'static str,
        new: &'static str,
    },
    #[error(transparent)]
    TypeCreationError(#[from] TypeError),
    #[error("Array length cannot be decreased from {old} to {new}")]
    ArrayLengthDecreaseError { old: u32, new: u32 },
    #[error("Cannot reduce element in tuple")]
    TupleReductionError,
}

pub type UpgradeResult<T = ()> = Result<T, UpgradeError>;

impl UpgradeError {
    pub fn type_upgrade_err<T>(old: &TypeDef, new: &TypeDef) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new: new.item_name(),
        })
    }
    pub fn type_upgrade_to_err<T>(old: &TypeDef, new: &'static str) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new,
        })
    }
    pub fn type_cast_err<T>(old: &TypeDef, new: &'static str) -> UpgradeResult<T> {
        Err(Self::TypeUpgradeError {
            old: old.item_name(),
            new,
        })
    }
    pub fn array_shorten_err<T>(old: u32, new: u32) -> UpgradeResult<T> {
        Err(Self::ArrayLengthDecreaseError { old, new })
    }
    pub fn primary_upgrade_err<T>(old: &TypeDef, new: &PrimaryTypeDef) -> UpgradeResult<T> {
        Err(Self::PrimaryUpgradeError {
            old: old.item_name(),
            new: new.item_name(),
        })
    }
}

pub trait UpgradeResultExt<T> {
    fn to_table_result(self, table: &str, column: &str) -> TableResult<T>;
}

impl<T> UpgradeResultExt<T> for UpgradeResult<T> {
    fn to_table_result(self, table: &str, column: &str) -> TableResult<T> {
        self.map_err(|err| TableError::UnsupportedUpgrade {
            table: table.to_string(),
            column: column.to_string(),
            reason: err,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error("Record does not match table schema")]
    SchemaMismatch,
    #[error(transparent)]
    DecodeError(#[from] DecodeError),
    #[error(transparent)]
    SqlxError(#[from] SqlxError),
    #[error("Columns with ids: {0} not found")]
    ColumnsNotFound(#[from] ColumnsNotFoundError),
}

pub type RecordResult<T> = std::result::Result<T, RecordError>;

pub trait RecordResultExt<T> {
    fn to_db_result(self, table: &str) -> DbResult<T>;
}

impl<T> RecordResultExt<T> for RecordResult<T> {
    fn to_db_result(self, table: &str) -> DbResult<T> {
        self.map_err(|err| DbError::RecordError(table.to_string(), err))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(transparent)]
    DatabaseError(#[from] SqlxError),
    #[error("Invalid event format: {0}")]
    InvalidEventFormat(String),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    TableError(#[from] TableError),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error("Table with id: {0} already exists, incoming name: {1}, existing name: {2}")]
    TableAlreadyExists(TableKey, String, String),
    #[error("Table not found with id: {0}")]
    TableNotFound(TableKey),
    #[error("Table not alive - id: {0}, name: {1}")]
    TableNotAlive(Felt, String),
    #[error("Manager does not support updating")]
    UpdateNotSupported,
    #[error("Table poison error: {0}")]
    PoisonError(String),
    #[error("Namespace not found for address: {0:#063x}")]
    NamespaceNotFound(Felt),
    #[error("Could not parse record for table {0}: {1}")]
    RecordError(String, RecordError),
    #[error("Failed to pass string to felt")]
    FeltFromStrError(#[from] FromStrError),
}

pub type DbResult<T = ()> = std::result::Result<T, DbError>;

impl<T> From<PoisonError<T>> for DbError {
    fn from(err: PoisonError<T>) -> Self {
        Self::PoisonError(err.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NamespaceError {
    #[error("Invalid address length for address: {0} should be 63 characters long")]
    InvalidAddressLength(String),
    #[error(transparent)]
    AddressFromStrError(#[from] FromStrError),
    #[error("Namespace {0} does not match expected namespace {1}")]
    NamespaceMismatch(String, String),
    #[error("Namespace {1} not found for address: {0:#063x}")]
    AddressNotFound(Felt, String),
}

pub type NamespaceResult<T> = std::result::Result<T, NamespaceError>;

#[derive(Debug, thiserror::Error)]
pub enum TableLoadError {
    #[error(transparent)]
    NamespaceError(#[from] NamespaceError),
    #[error("Table {0} {1:#063x} not found for column {2} with id: {3:#063x}")]
    ColumnTableNotFound(String, Felt, String, Felt),
    #[error("Table {0} {1:#063x} not found for dead field {2} with id: {3}")]
    TableDeadNotFound(String, Felt, String, u128),
}
