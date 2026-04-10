use std::str::FromStr;

use crate::connection::DbBackend;
use crate::{DbPoolOptions, PoolConfig, SqlxError, SqlxResult};
use sqlx::postgres::PgConnectOptions;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Pool, Postgres, Sqlite};

#[derive(Clone)]
pub enum DbPool {
    Postgres(Pool<Postgres>),
    Sqlite(Pool<Sqlite>),
}

#[derive(Debug, Clone)]
pub enum DbConnectionOptions {
    Postgres(PgConnectOptions),
    Sqlite(SqliteConnectOptions),
}

impl FromStr for DbConnectionOptions {
    type Err = SqlxError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match DbBackend::from_str(s)? {
            DbBackend::Postgres => PgConnectOptions::from_str(s).map(DbConnectionOptions::Postgres),
            DbBackend::Sqlite => SqliteConnectOptions::from_str(s).map(DbConnectionOptions::Sqlite),
        }
    }
}

impl PoolConfig {
    pub async fn connect_any(&self) -> SqlxResult<DbPool> {
        match DbBackend::try_from(self.url.as_str()) {
            Ok(DbBackend::Postgres) => self.connect::<Postgres>().await.map(DbPool::Postgres),
            Ok(DbBackend::Sqlite) => self.connect::<Sqlite>().await.map(DbPool::Sqlite),
            Err(err) => Err(SqlxError::Configuration(err.into())),
        }
    }
}

impl DbPoolOptions {
    pub async fn connect_any(&self, url: &str) -> SqlxResult<DbPool> {
        match DbBackend::try_from(url) {
            Ok(DbBackend::Postgres) => self.connect::<Postgres>(url).await.map(DbPool::Postgres),
            Ok(DbBackend::Sqlite) => self.connect::<Sqlite>(url).await.map(DbPool::Sqlite),
            Err(err) => Err(SqlxError::Configuration(err.into())),
        }
    }

    pub async fn connect_any_with(&self, options: DbConnectionOptions) -> SqlxResult<DbPool> {
        match options {
            DbConnectionOptions::Postgres(opts) => {
                self.connect_with(opts).await.map(DbPool::Postgres)
            }
            DbConnectionOptions::Sqlite(opts) => self.connect_with(opts).await.map(DbPool::Sqlite),
        }
    }
}
