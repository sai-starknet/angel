use anyhow::{bail, Result as AnyResult};
use std::path::{Path, PathBuf};
use torii_sql::connection::DbBackend;

pub const DEFAULT_SQLITE_MAX_CONNECTIONS: u32 = 500;

pub fn backend_from_url_or_path(value: &str) -> DbBackend {
    if value.starts_with("postgres://") || value.starts_with("postgresql://") {
        DbBackend::Postgres
    } else {
        DbBackend::Sqlite
    }
}

pub fn validate_uniform_backends(
    named_urls: &[(&str, &str)],
    mixed_backend_message: &str,
) -> AnyResult<DbBackend> {
    let Some((_, first_url)) = named_urls.first() else {
        bail!("at least one database URL must be provided");
    };

    let expected_backend = backend_from_url_or_path(first_url);
    if named_urls
        .iter()
        .any(|(_, url)| backend_from_url_or_path(url) != expected_backend)
    {
        let summary = named_urls
            .iter()
            .map(|(name, url)| format!("{name}={:?}({url})", backend_from_url_or_path(url)))
            .collect::<Vec<_>>()
            .join(", ");
        bail!("{mixed_backend_message}. current: {summary}");
    }

    Ok(expected_backend)
}

#[derive(Debug, Clone)]
pub struct SingleDbSetup {
    pub storage_url: String,
    pub engine_url: String,
    pub database_root: PathBuf,
}

pub fn resolve_single_db_setup(db_path: &str, database_url: Option<&str>) -> SingleDbSetup {
    let database_root = Path::new(db_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let storage_url = database_url.unwrap_or(db_path).to_string();
    let engine_url = database_url.map_or_else(
        || {
            database_root
                .join("engine.db")
                .to_string_lossy()
                .to_string()
        },
        ToOwned::to_owned,
    );

    SingleDbSetup {
        storage_url,
        engine_url,
        database_root,
    }
}

#[derive(Debug, Clone)]
pub struct TokenDbSetup {
    pub engine_url: String,
    pub erc20_url: String,
    pub erc721_url: String,
    pub erc1155_url: String,
    pub engine_backend: DbBackend,
    pub erc20_backend: DbBackend,
    pub erc721_backend: DbBackend,
    pub erc1155_backend: DbBackend,
}

pub fn resolve_token_db_setup(
    db_dir: &Path,
    engine_database_url: Option<&str>,
    storage_database_url: Option<&str>,
) -> AnyResult<TokenDbSetup> {
    let engine_url = engine_database_url.map_or_else(
        || db_dir.join("engine.db").to_string_lossy().to_string(),
        ToOwned::to_owned,
    );
    let erc20_url = resolve_storage_url(
        storage_database_url,
        engine_database_url,
        db_dir,
        "erc20.db",
    );
    let erc721_url = resolve_storage_url(
        storage_database_url,
        engine_database_url,
        db_dir,
        "erc721.db",
    );
    let erc1155_url = resolve_storage_url(
        storage_database_url,
        engine_database_url,
        db_dir,
        "erc1155.db",
    );

    let engine_backend = backend_from_url_or_path(&engine_url);
    let erc20_backend = backend_from_url_or_path(&erc20_url);
    let erc721_backend = backend_from_url_or_path(&erc721_url);
    let erc1155_backend = backend_from_url_or_path(&erc1155_url);

    if engine_database_url
        .map(backend_from_url_or_path)
        .is_some_and(|backend| backend == DbBackend::Postgres)
        && (erc20_backend != DbBackend::Postgres
            || erc721_backend != DbBackend::Postgres
            || erc1155_backend != DbBackend::Postgres)
    {
        bail!(
            "Engine is configured for Postgres but one or more token storages resolved to SQLite. Set --storage-database-url to the same Postgres URL."
        );
    }

    Ok(TokenDbSetup {
        engine_url,
        erc20_url,
        erc721_url,
        erc1155_url,
        engine_backend,
        erc20_backend,
        erc721_backend,
        erc1155_backend,
    })
}

fn resolve_storage_url(
    storage_database_url: Option<&str>,
    engine_database_url: Option<&str>,
    db_dir: &Path,
    fallback_file: &str,
) -> String {
    storage_database_url
        .map(ToOwned::to_owned)
        .or_else(|| engine_database_url.map(ToOwned::to_owned))
        .unwrap_or_else(|| db_dir.join(fallback_file).to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_sqlite_defaults() {
        let db_dir = Path::new("./torii-data");
        let setup = resolve_token_db_setup(db_dir, None, None).unwrap();
        assert_eq!(setup.engine_backend, DbBackend::Sqlite);
        assert_eq!(setup.erc20_backend, DbBackend::Sqlite);
        assert!(setup.engine_url.ends_with("engine.db"));
        assert!(setup.erc20_url.ends_with("erc20.db"));
    }

    #[test]
    fn rejects_mixed_engine_and_storage_backends() {
        let db_dir = Path::new("./torii-data");
        let err = resolve_token_db_setup(
            db_dir,
            Some("postgres://localhost/torii"),
            Some("./torii-data"),
        );
        println!("{err:?}");
        let err = err.expect_err("expected mixed backend validation error");
        assert!(err
            .to_string()
            .contains("Engine is configured for Postgres"));
    }

    #[test]
    fn resolves_postgres_for_all_targets() {
        let db_dir = Path::new("./torii-data");
        let setup = resolve_token_db_setup(
            db_dir,
            Some("postgres://localhost/torii"),
            Some("postgres://localhost/torii"),
        )
        .unwrap();
        assert_eq!(setup.engine_backend, DbBackend::Postgres);
        assert_eq!(setup.erc721_backend, DbBackend::Postgres);
    }

    #[test]
    fn validate_uniform_backends_accepts_matching_backends() {
        let backend = validate_uniform_backends(
            &[
                ("engine", "postgres://localhost/torii"),
                ("storage", "postgres://localhost/torii"),
            ],
            "mixed backends",
        )
        .unwrap();
        assert_eq!(backend, DbBackend::Postgres);
    }

    #[test]
    fn validate_uniform_backends_rejects_mixed_backends() {
        let err = validate_uniform_backends(
            &[
                ("engine", "postgres://localhost/torii"),
                ("storage", "./torii-data/introspect.db"),
            ],
            "mixed backends",
        )
        .expect_err("expected mixed backend error");
        assert!(err.to_string().contains("mixed backends"));
        assert!(err.to_string().contains("engine=Postgres"));
        assert!(err.to_string().contains("storage=Sqlite"));
    }
}
