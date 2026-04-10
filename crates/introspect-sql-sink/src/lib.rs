pub mod backend;
pub mod error;
pub mod namespace;
pub mod processor;
pub mod sink;
pub mod table;
pub mod tables;

pub use backend::{IntrospectInitialize, IntrospectProcessor, IntrospectQueryMaker};
pub use error::{
    DbError, DbResult, RecordError, RecordResult, TableError, TableResult, TypeError, TypeResult,
    UpgradeError, UpgradeResult, UpgradeResultExt,
};
pub use namespace::{NamespaceKey, NamespaceMode, TableKey};
pub use processor::{DbColumn, DbDeadField, DbTable, IntrospectDb};
pub use sink::IntrospectSqlSink;
pub use table::{DeadField, DeadFieldDef, Table};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::IntrospectPgDb;

#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::IntrospectSqliteDb;

#[cfg(feature = "postgres")]
#[cfg(feature = "sqlite")]
pub mod runtime;
