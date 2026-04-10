pub mod connection;
pub mod migrate;
pub mod pool;
pub mod query;
pub mod types;

pub use connection::DbBackend;
pub use migrate::{AcquiredSchema, SchemaMigrator};
pub use pool::{DbPoolOptions, PoolConfig, PoolExt};
pub use query::{Executable, FlexQuery, Queries};

pub use sqlx::Error as SqlxError;
pub type SqlxResult<T = ()> = std::result::Result<T, SqlxError>;

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::{PgArguments, PgDbConnection, PgPool, PgQuery, Postgres};

#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::{Sqlite, SqliteArguments, SqliteDbConnection, SqlitePool, SqliteQuery};

#[cfg(feature = "postgres")]
#[cfg(feature = "sqlite")]
pub mod runtime;

#[cfg(feature = "postgres")]
#[cfg(feature = "sqlite")]
pub use runtime::{DbConnectionOptions, DbPool};
