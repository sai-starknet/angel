use crate::SqlxResult;
use futures::future::BoxFuture;
use itertools::Itertools;
use sqlx::{Database, Executor, Transaction};
use std::fmt::Display;
use std::sync::Arc;

pub trait Executable<DB: Database> {
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't;
}

impl<DB: Database> Executable<DB> for &str
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self).await?;
            Ok(())
        })
    }
}

impl<DB: Database> Executable<DB> for &String
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self.as_str()).await?;
            Ok(())
        })
    }
}

impl<DB: Database> Executable<DB> for String
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self.as_str()).await?;
            Ok(())
        })
    }
}

impl<DB: Database> Executable<DB> for FlexStr
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            transaction.execute(self.as_ref()).await?;
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
pub enum FlexStr {
    Owned(String),
    Static(&'static str),
    Shared(Arc<str>),
}

impl Display for FlexStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl FlexStr {
    pub fn as_str(&self) -> &str {
        match self {
            FlexStr::Owned(s) => s.as_str(),
            FlexStr::Shared(s) => s,
            FlexStr::Static(s) => s,
        }
    }
}

impl AsRef<str> for FlexStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for FlexStr {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

/// A query with optional bind arguments.
///
/// SQL can be any `FlexStr` (String, Arc<str>, &'static str).
/// The `Bound` variant carries SQL + pre-built arguments.
/// The per-database `Executable` impls handle the lifetime requirements:
/// Postgres needs no special treatment; SQLite uses an unsafe lifetime extension
/// that is sound because the `FlexStr` outlives the `.await` point.
pub struct FlexQuery<DB: Database> {
    pub(crate) sql: FlexStr,
    pub(crate) args: Option<<DB as Database>::Arguments<'static>>,
}

impl<DB: Database> Clone for FlexQuery<DB>
where
    DB::Arguments<'static>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            sql: self.sql.clone(),
            args: self.args.clone(),
        }
    }
}

impl<DB: Database> std::fmt::Debug for FlexQuery<DB>
where
    DB::Arguments<'static>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlexQuery")
            .field("sql", &self.sql)
            .field("args", &self.args)
            .finish()
    }
}

impl<DB: Database> FlexQuery<DB> {
    pub fn new(sql: impl Into<FlexStr>, args: <DB as Database>::Arguments<'static>) -> Self {
        FlexQuery {
            sql: sql.into(),
            args: Some(args),
        }
    }

    pub fn from_sql(sql: impl Into<FlexStr>) -> Self {
        FlexQuery {
            sql: sql.into(),
            args: None,
        }
    }
}

impl<DB: Database> PartialEq<str> for FlexQuery<DB> {
    fn eq(&self, other: &str) -> bool {
        self.sql.as_ref() == other
    }
}

impl From<String> for FlexStr {
    fn from(s: String) -> Self {
        FlexStr::Owned(s)
    }
}

impl From<&'static str> for FlexStr {
    fn from(s: &'static str) -> Self {
        FlexStr::Static(s)
    }
}

impl From<Arc<str>> for FlexStr {
    fn from(s: Arc<str>) -> Self {
        FlexStr::Shared(s)
    }
}

impl<A: Database> From<FlexStr> for FlexQuery<A> {
    fn from(sql: FlexStr) -> Self {
        FlexQuery::from_sql(sql)
    }
}

impl<A: Database> From<&'static str> for FlexQuery<A> {
    fn from(sql: &'static str) -> Self {
        FlexQuery::from_sql(sql)
    }
}

impl<A: Database> From<String> for FlexQuery<A> {
    fn from(sql: String) -> Self {
        FlexQuery::from_sql(sql)
    }
}

impl<A: Database> From<Arc<str>> for FlexQuery<A> {
    fn from(sql: Arc<str>) -> Self {
        FlexQuery::from_sql(sql)
    }
}

impl<DB: Database, S, A> From<(S, A)> for FlexQuery<DB>
where
    S: Into<FlexStr>,
    A: Into<<DB as Database>::Arguments<'static>>,
{
    fn from((sql, args): (S, A)) -> Self {
        FlexQuery::new(sql, args.into())
    }
}

pub trait Queries<DB: Database> {
    fn add(&mut self, query: impl Into<FlexQuery<DB>>);
    fn adds(&mut self, queries: impl IntoIterator<Item = impl Into<FlexQuery<DB>>>);
}

impl<DB: Database> Queries<DB> for Vec<FlexQuery<DB>> {
    fn add(&mut self, query: impl Into<FlexQuery<DB>>) {
        self.push(query.into());
    }
    fn adds(&mut self, queries: impl IntoIterator<Item = impl Into<FlexQuery<DB>>>) {
        self.extend(queries.into_iter().map_into());
    }
}

impl<DB: Database, const N: usize> Executable<DB> for &[String; N]
where
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for query in self {
                transaction.execute(query.as_str()).await?;
            }
            Ok(())
        })
    }
}

impl<DB: Database, T: Executable<DB> + Send> Executable<DB> for Vec<T> {
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for item in self {
                item.execute(transaction).await?;
            }
            Ok(())
        })
    }
}

impl<'a, DB: Database, T> Executable<DB> for &'a Vec<T>
where
    &'a T: Executable<DB> + Send,
    T: Send + Sync,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for item in self {
                item.execute(transaction).await?;
            }
            Ok(())
        })
    }
}

impl<'a, DB: Database, T> Executable<DB> for &'a [T]
where
    &'a T: Executable<DB> + Send,
    T: Send + Sync,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for item in self {
                item.execute(transaction).await?;
            }
            Ok(())
        })
    }
}

impl<const N: usize, DB: Database, T> Executable<DB> for [T; N]
where
    T: Executable<DB> + Send,
{
    fn execute<'t>(self, transaction: &'t mut Transaction<'_, DB>) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            for item in self {
                item.execute(transaction).await?;
            }
            Ok(())
        })
    }
}
