CREATE SCHEMA IF NOT EXISTS introspect;
CREATE SCHEMA IF NOT EXISTS dojo;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'felt252') THEN
        CREATE DOMAIN felt252 AS bytea CHECK (octet_length(VALUE) = 32);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint64') THEN
        CREATE DOMAIN uint64 AS NUMERIC(20, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 64));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS dojo.tables (
    owner felt252,
    id felt252 NOT NULL,
    block_number uint64 NOT NULL,
    name TEXT NOT NULL,
    attributes TEXT[] NOT NULL DEFAULT '{}',
    keys felt252[] NOT NULL,
    "values" felt252[] NOT NULL ,
    legacy BOOLEAN NOT NULL, 
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_tx felt252 NOT NULL,
    updated_tx felt252 NOT NULL,
    PRIMARY KEY (owner, id, block_number)
);

CREATE TABLE IF NOT EXISTS dojo.columns(
    owner felt252,
    "table" felt252 NOT NULL ,
    id felt252 NOT NULL,
    block_number uint64 NOT NULL,
    name TEXT NOT NULL,
    attributes TEXT[] NOT NULL DEFAULT '{}',
    type_def jsonb NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_tx felt252 NOT NULL,
    updated_tx felt252 NOT NULL,
    PRIMARY KEY (owner, "table", id, block_number)
);

