pub mod migrate;
pub mod types;

pub use sqlx::postgres::PgArguments;
pub use sqlx::{PgPool, Postgres};

use crate::{Executable, FlexQuery, SqlxResult};
use futures::future::BoxFuture;
use sqlx::{Executor, PgTransaction};

pub type PgQuery = crate::FlexQuery<Postgres>;

pub trait PgDbConnection: crate::PoolExt<Postgres> {}
impl<T: PgDbConnection> crate::PgDbConnection for T {}

impl Executable<Postgres> for FlexQuery<Postgres> {
    fn execute<'t>(self, transaction: &'t mut PgTransaction) -> BoxFuture<'t, SqlxResult<()>>
    where
        Self: 't,
    {
        Box::pin(async move {
            match self.args {
                Some(args) => {
                    transaction
                        .execute(sqlx::query_with(self.sql.as_ref(), args))
                        .await?;
                }
                None => {
                    transaction.execute(self.sql.as_ref()).await?;
                }
            }
            Ok(())
        })
    }
}
