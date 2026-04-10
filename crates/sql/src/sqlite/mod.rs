pub mod migrate;
pub mod types;

use futures::future::BoxFuture;
pub use sqlx::sqlite::SqliteArguments;
use sqlx::{Executor, SqliteTransaction};
pub use sqlx::{Sqlite, SqlitePool};

use sqlx::sqlite::SqliteConnectOptions;
use std::str::FromStr;

use crate::{Executable, SqlxResult};

pub type SqliteQuery = super::FlexQuery<Sqlite>;

pub trait SqliteDbConnection: super::PoolExt<Sqlite> {}
impl<T: super::PoolExt<Sqlite>> SqliteDbConnection for T {}

pub fn is_sqlite_memory_path(path: &str) -> bool {
    path == ":memory:"
        || path == "sqlite::memory:"
        || path == "sqlite://:memory:"
        || path.contains("mode=memory")
}

pub fn sqlite_connect_options(path: &str) -> Result<SqliteConnectOptions, sqlx::Error> {
    if path == ":memory:" || path == "sqlite::memory:" {
        return SqliteConnectOptions::from_str("sqlite::memory:");
    }

    let options = if path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(path)?
    } else {
        SqliteConnectOptions::new().filename(path)
    };

    if path.starts_with("sqlite:") && path.contains("mode=") {
        Ok(options)
    } else {
        Ok(options.create_if_missing(true))
    }
}

#[allow(unsafe_code)]
impl Executable<Sqlite> for SqliteQuery {
    fn execute<'t>(self, transaction: &'t mut SqliteTransaction) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            match self.args {
                Some(args) => {
                    // SAFETY: `self.sql` is moved into this async block and lives for
                    // its entire duration. The extended reference is only used by
                    // `query_with` which is immediately awaited, so it cannot outlive
                    // the backing data in `FlexStr`.
                    let sql_ref: &'static str =
                        unsafe { std::mem::transmute::<&str, &'static str>(self.sql.as_ref()) };
                    transaction.execute(sqlx::query_with(sql_ref, args)).await?;
                }
                None => {
                    transaction.execute(self.sql.as_ref()).await?;
                }
            }
            Ok(())
        })
    }
}
