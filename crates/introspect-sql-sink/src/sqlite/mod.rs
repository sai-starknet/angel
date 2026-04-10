pub mod append_only;
pub mod backend;
pub mod json;
pub mod record;
pub mod table;
pub mod types;

use sqlx::migrate::Migrator;

pub use backend::IntrospectSqliteDb;

pub const INTROSPECT_SQLITE_SINK_MIGRATIONS: Migrator = sqlx::migrate!("./migrations/sqlite");
