use crate::SqlxError;
use std::fmt::Display;
use std::str::FromStr;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbBackend {
    Postgres,
    Sqlite,
}

pub struct DbOption<T> {
    postgres: T,
    sqlite: T,
}

pub const POSTGRES_URL_SCHEMES: [&str; 2] = ["postgres", "postgresql"];
pub const SQLITE_URL_SCHEMES: [&str; 1] = ["sqlite"];

impl FromStr for DbBackend {
    type Err = SqlxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("postgres") || s.starts_with("postgresql") {
            Ok(DbBackend::Postgres)
        } else if s.starts_with("sqlite") || s == ":memory:" || s == "memory" {
            Ok(DbBackend::Sqlite)
        } else {
            Err(SqlxError::Configuration(
                format!("Unsupported database url: {s}").into(),
            ))
        }
    }
}

impl Display for DbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl DbBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            DbBackend::Postgres => "postgres",
            DbBackend::Sqlite => "sqlite",
        }
    }
}

impl TryFrom<&str> for DbBackend {
    type Error = SqlxError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        DbBackend::from_str(value)
    }
}

impl TryFrom<String> for DbBackend {
    type Error = SqlxError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        DbBackend::try_from(value.as_str())
    }
}

impl<T> DbOption<T> {
    pub fn new(postgres: T, sqlite: T) -> Self {
        Self { postgres, sqlite }
    }

    pub fn value(self, db_type: &DbBackend) -> T {
        match db_type {
            DbBackend::Postgres => self.postgres,
            DbBackend::Sqlite => self.sqlite,
        }
    }
}
