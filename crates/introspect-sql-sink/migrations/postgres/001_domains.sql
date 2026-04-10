-- Initialize all custom domains for Starknet/Cairo types
-- Using DO blocks for conditional creation since IF NOT EXISTS isn't supported in older PostgreSQL versions


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint8') THEN
        CREATE DOMAIN uint8 AS SmallInt
        CHECK (VALUE >= 0 AND VALUE < 256);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint16') THEN
        CREATE DOMAIN uint16 AS Int
        CHECK (VALUE >= 0 AND VALUE < 65536);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint32') THEN
        CREATE DOMAIN uint32 AS BigInt
        CHECK (VALUE >= 0 AND VALUE < 4294967296);
    END IF;
END $$;

-- uint64: Unsigned 64-bit integer (0 to 2^64-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint64') THEN
        CREATE DOMAIN uint64 AS NUMERIC(20, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 64));
    END IF;
END $$;

-- uint128: Unsigned 128-bit integer (0 to 2^128-1)  
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint128') THEN
        CREATE DOMAIN uint128 AS NUMERIC(39, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 128));
    END IF;
END $$;

-- int128: Signed 128-bit integer (-2^127 to 2^127-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'int128') THEN
        CREATE DOMAIN int128 AS NUMERIC(39, 0)
        CHECK (VALUE >= -power(2::numeric, 127) AND VALUE < power(2::numeric, 127));
    END IF;
END $$;

-- uint256: Unsigned 256-bit integer (0 to 2^256-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint256') THEN
        CREATE DOMAIN uint256 AS NUMERIC(78, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 256));
    END IF;
END $$;

-- uint512: Unsigned 512-bit integer (0 to 2^512-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint512') THEN
        CREATE DOMAIN uint512 AS NUMERIC(155, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 512));
    END IF;
END $$;

-- felt252: Cairo field element (0 to PRIME-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'felt252') THEN
        CREATE DOMAIN felt252 AS bytea CHECK (octet_length(VALUE) = 32);
    END IF;
END $$;

-- starknet_hash: Starknet addresses and class hashes (uint251)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'starknet_hash') THEN
        CREATE DOMAIN starknet_hash AS bytea CHECK (octet_length(VALUE) = 32);
    END IF;
END $$;

-- eth_address: Ethereum address (160-bit / 20 bytes)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'eth_address') THEN
        CREATE DOMAIN eth_address AS  bytea CHECK (octet_length(VALUE) = 20);
    END IF;
END $$;

-- byte31: 31-byte array
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'byte31') THEN
        CREATE DOMAIN byte31 AS  bytea CHECK (octet_length(VALUE) = 31);
    END IF;
END $$;

-- char31: character string of max length 31
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'char31') THEN
        CREATE DOMAIN char31 AS  varchar(31);
    END IF;
END $$;

