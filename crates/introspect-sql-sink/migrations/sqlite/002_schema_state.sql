CREATE TABLE IF NOT EXISTS introspect_db_tables (
    namespace TEXT NOT NULL,
    id TEXT NOT NULL,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    "primary" TEXT NOT NULL,
    columns TEXT NOT NULL,
    append_only INTEGER NOT NULL DEFAULT 0,
    alive INTEGER NOT NULL DEFAULT 1,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (namespace, id)
);
