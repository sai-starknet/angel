use crate::AcquiredSchema;
use futures::future::BoxFuture;
use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migration};
use sqlx::{query, query_as, Acquire, Executor, Sqlite, SqliteConnection};
use std::borrow::Cow;
use std::time::{Duration, Instant};

impl AcquiredSchema<Sqlite, SqliteConnection> {
    fn table_name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("_sqlx_migrations_{}", self.schema))
    }
}

impl Migrate for AcquiredSchema<Sqlite, SqliteConnection> {
    fn ensure_migrations_table(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            self.connection
                .execute(
                    format!(
                        r#"
CREATE TABLE IF NOT EXISTS "{table_name}" (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
                        "#
                    )
                    .as_str(),
                )
                .await?;

            Ok(())
        })
    }

    fn dirty_version(&mut self) -> BoxFuture<'_, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let row: Option<(i64,)> = query_as(
                format!(
                    r#"SELECT version FROM "{table_name}" WHERE success = false ORDER BY version LIMIT 1"#
                )
                .as_str(),
            )
            .fetch_optional(&mut self.connection)
            .await?;

            Ok(row.map(|row| row.0))
        })
    }

    fn list_applied_migrations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let rows: Vec<(i64, Vec<u8>)> = query_as(
                format!(r#"SELECT version, checksum FROM "{table_name}" ORDER BY version"#)
                    .as_str(),
            )
            .fetch_all(&mut self.connection)
            .await?;

            Ok(rows
                .into_iter()
                .map(|(version, checksum)| AppliedMigration {
                    version,
                    checksum: checksum.into(),
                })
                .collect())
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn apply<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let mut tx = self.begin().await?;
            let start = Instant::now();

            let _ = tx
                .execute(&*migration.sql)
                .await
                .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

            let _ = query(
                format!(
                    r#"
INSERT INTO "{table_name}" (version, description, success, checksum, execution_time)
VALUES (?1, ?2, TRUE, ?3, -1)
                    "#
                )
                .as_str(),
            )
            .bind(migration.version)
            .bind(&*migration.description)
            .bind(&*migration.checksum)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            #[allow(clippy::cast_possible_truncation)]
            let _ = query(
                format!(
                    r#"
UPDATE "{table_name}"
SET execution_time = ?1
WHERE version = ?2
                    "#
                )
                .as_str(),
            )
            .bind(elapsed.as_nanos() as i64)
            .bind(migration.version)
            .execute(&mut self.connection)
            .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let table_name = self.table_name();
            let mut tx = self.begin().await?;
            let start = Instant::now();

            let _ = tx.execute(&*migration.sql).await?;

            let _ = query(format!(r#"DELETE FROM "{table_name}" WHERE version = ?1"#).as_str())
                .bind(migration.version)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            Ok(start.elapsed())
        })
    }
}
