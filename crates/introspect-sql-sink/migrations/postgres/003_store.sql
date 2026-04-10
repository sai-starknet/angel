CREATE SCHEMA IF NOT EXISTS introspect;

DO $$
BEGIN
    IF to_regtype('introspect.attribute') IS NULL THEN
        CREATE TYPE introspect.attribute AS (
            name TEXT,
            data bytea
        );
    END IF;

    IF to_regtype('introspect.primary_def') IS NULL THEN
        CREATE TYPE introspect.primary_def AS (
            name TEXT,
            attributes introspect.attribute[],
            type_def jsonb
        );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS introspect.db_tables (
    "schema" TEXT NOT NULL,
    id felt252 NOT NULL,
    name TEXT NOT NULL,
    "owner" felt252 NOT NULL,
    primary_def introspect.primary_def NOT NULL,
    append_only BOOLEAN NOT NULL DEFAULT FALSE,
    alive BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_block uint64 NOT NULL,
    updated_block uint64 NOT NULL,
    created_tx felt252 NOT NULL,
    updated_tx felt252 NOT NULL,
    PRIMARY KEY ("schema", id)
);

CREATE TABLE IF NOT EXISTS introspect.db_columns(
    "schema" TEXT NOT NULL,
    "table" felt252 NOT NULL,
    id felt252 NOT NULL,
    name TEXT NOT NULL,
    type_def JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_block uint64 NOT NULL,
    updated_block uint64 NOT NULL,
    created_tx felt252 NOT NULL,
    updated_tx felt252 NOT NULL,
    PRIMARY KEY ("schema", "table", id),
    FOREIGN KEY ("schema", "table") REFERENCES introspect.db_tables("schema", id)
);

CREATE TABLE IF NOT EXISTS introspect.db_dead_fields (
    "schema" TEXT NOT NULL,
    "table" felt252,
    id uint128,
    name TEXT,
    type_def JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_block uint64 NOT NULL,
    updated_block uint64 NOT NULL,
    created_tx felt252 NOT NULL,
    updated_tx felt252 NOT NULL,
    PRIMARY KEY ("schema", "table", id),
    FOREIGN KEY ("schema", "table") REFERENCES introspect.db_tables("schema", id)
);