use crate::AcquiredSchema;
use futures::future::BoxFuture;
use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migration};
use sqlx::{query, query_as, query_scalar, Connection, Executor, PgConnection, Postgres};
use std::time::{Duration, Instant};

impl Migrate for AcquiredSchema<Postgres, PgConnection> {
    fn ensure_migrations_table(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            // language=SQL
            self.connection
                .execute(
                    format!(
                        "CREATE SCHEMA IF NOT EXISTS {schema};
                        CREATE TABLE IF NOT EXISTS {schema}._sqlx_migrations (
                            version BIGINT PRIMARY KEY,
                            description TEXT NOT NULL,
                            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
                            success BOOLEAN NOT NULL,
                            checksum BYTEA NOT NULL,
                            execution_time BIGINT NOT NULL
                        );",
                        schema = self.schema
                    )
                    .as_str(),
                )
                .await?;

            Ok(())
        })
    }

    fn dirty_version(&mut self) -> BoxFuture<'_, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            // language=SQL

            let row: Option<(i64,)> = query_as(
                    format!("SELECT version FROM {schema}._sqlx_migrations WHERE success = false ORDER BY version LIMIT 1", schema = self.schema).as_str()
            )
            .fetch_optional(&mut self.connection)
            .await?;

            Ok(row.map(|r: (i64,)| r.0))
        })
    }

    fn list_applied_migrations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            // language=SQL
            let rows: Vec<(i64, Vec<u8>)> = query_as(
                format!(
                    "SELECT version, checksum FROM {schema}._sqlx_migrations ORDER BY version",
                    schema = self.schema
                )
                .as_str(),
            )
            .fetch_all(&mut self.connection)
            .await?;

            let migrations = rows
                .into_iter()
                .map(|(version, checksum)| AppliedMigration {
                    version,
                    checksum: checksum.into(),
                })
                .collect();

            Ok(migrations)
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            let database_name = current_database(&mut self.connection).await?;
            let lock_id = generate_lock_id(&database_name);

            // create an application lock over the database
            // this function will not return until the lock is acquired

            // https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS
            // https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS-TABLE

            // language=SQL
            let _ = query("SELECT pg_advisory_lock($1)")
                .bind(lock_id)
                .execute(&mut self.connection)
                .await?;

            Ok(())
        })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            let database_name = current_database(self).await?;
            let lock_id = generate_lock_id(&database_name);

            // language=SQL
            let _ = query("SELECT pg_advisory_unlock($1)")
                .bind(lock_id)
                .execute(&mut self.connection)
                .await?;

            Ok(())
        })
    }

    fn apply<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();
            let schema = self.schema;
            // execute migration queries
            if migration.no_tx {
                execute_migration(self, schema, migration).await?;
            } else {
                // Use a single transaction for the actual migration script and the essential bookeeping so we never
                // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
                // The `execution_time` however can only be measured for the whole transaction. This value _only_ exists for
                // data lineage and debugging reasons, so it is not super important if it is lost. So we initialize it to -1
                // and update it once the actual transaction completed.
                let mut tx = Connection::begin(&mut self.connection).await?;
                execute_migration(&mut tx, schema, migration).await?;
                tx.commit().await?;
            }

            // Update `elapsed_time`.
            // NOTE: The process may disconnect/die at this point, so the elapsed time value might be lost. We accept
            //       this small risk since this value is not super important.
            let elapsed = start.elapsed();

            // language=SQL
            #[allow(clippy::cast_possible_truncation)]
            let _ = query(&format!(
                "UPDATE {schema}._sqlx_migrations SET execution_time = $1 WHERE version = $2",
                schema = self.schema
            ))
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
            let start = Instant::now();
            let schema = self.schema;
            // execute migration queries
            if migration.no_tx {
                revert_migration(&mut self.connection, schema, migration).await?;
            } else {
                // Use a single transaction for the actual migration script and the essential bookeeping so we never
                // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
                let mut tx = Connection::begin(&mut self.connection).await?;
                revert_migration(&mut tx, schema, migration).await?;
                tx.commit().await?;
            }

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }
}

async fn current_database(conn: &mut PgConnection) -> Result<String, MigrateError> {
    // language=SQL
    Ok(query_scalar("SELECT current_database()")
        .fetch_one(conn)
        .await?)
}

fn generate_lock_id(database_name: &str) -> i64 {
    const CRC_IEEE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
    // 0x3d32ad9e chosen by fair dice roll
    0x3d32ad9e * (CRC_IEEE.checksum(database_name.as_bytes()) as i64)
}

async fn execute_migration(
    conn: &mut PgConnection,
    schema: &'static str,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(&*migration.sql)
        .await
        .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

    // language=SQL
    query(
        format!(r#"INSERT INTO "{schema}"._sqlx_migrations ( version, description, success, checksum, execution_time ) VALUES ( $1, $2, TRUE, $3, -1 )"#).as_str()
    )
    .bind(migration.version)
    .bind(&*migration.description)
    .bind(&*migration.checksum)
    .execute(conn)
    .await?;
    Ok(())
}

async fn revert_migration(
    conn: &mut PgConnection,
    schema: &'static str,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(&*migration.sql)
        .await
        .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

    query(format!(r#"DELETE FROM "{schema}"._sqlx_migrations WHERE version = $1"#).as_str())
        .bind(migration.version)
        .execute(conn)
        .await?;

    Ok(())
}
