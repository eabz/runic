CREATE SCHEMA IF NOT EXISTS indexer;

CREATE TABLE IF NOT EXISTS indexer.sync_checkpoints (
    chain_id            BIGINT PRIMARY KEY,
    last_indexed_block  BIGINT,
    updated_at          TIMESTAMPTZ
);

-- Cron job checkpoints to persist last_run timestamps across restarts
CREATE TABLE IF NOT EXISTS indexer.cron_checkpoints (
    job_name            TEXT PRIMARY KEY,
    last_run_at         TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS indexer.chains (
    chain_id                BIGINT PRIMARY KEY,
    name                    TEXT,
    enabled                 BOOLEAN,
    rpc_url                 TEXT,
    hypersync_url           TEXT,
    native_token_address    TEXT,
    native_token_decimals   INTEGER,
    native_token_name       TEXT,
    native_token_symbol     TEXT,
    stable_token_address    TEXT,
    stable_token_decimals   INTEGER,
    stable_pool_address     TEXT,
    stablecoins             TEXT[],
    major_tokens            TEXT[],
    updated_at              TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS indexer.tokens (
    chain_id                BIGINT NOT NULL,
    address                 TEXT NOT NULL,
    symbol                  TEXT,
    name                    TEXT,
    decimals                INTEGER,
    price_usd               DOUBLE PRECISION,
    price_updated_at        TIMESTAMPTZ,
    price_change_24h        DOUBLE PRECISION,
    price_change_7d         DOUBLE PRECISION,
    logo_url                TEXT,
    banner_url              TEXT,
    website                 TEXT,
    twitter                 TEXT,
    telegram                TEXT,
    discord                 TEXT,
    volume_24h              DOUBLE PRECISION,
    swaps_24h               BIGINT,
    total_swaps             BIGINT,
    total_volume_usd        DOUBLE PRECISION,
    pool_count              BIGINT,
    circulating_supply      DOUBLE PRECISION,
    market_cap_usd          DOUBLE PRECISION,
    first_seen_block        BIGINT,
    last_activity_at        TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    PRIMARY KEY (chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON indexer.tokens (chain_id, symbol);
CREATE INDEX IF NOT EXISTS idx_tokens_market_cap ON indexer.tokens (chain_id, market_cap_usd DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_tokens_volume_24h ON indexer.tokens (chain_id, volume_24h DESC NULLS LAST);
-- Index for price-based queries (Token Details panel)
CREATE INDEX IF NOT EXISTS idx_tokens_price ON indexer.tokens (chain_id, price_usd DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS indexer.pools (
    chain_id                BIGINT NOT NULL,
    address                 TEXT NOT NULL,
    token0                  TEXT,
    token1                  TEXT,
    token0_symbol           TEXT,
    token1_symbol           TEXT,
    token0_decimals         INTEGER,
    token1_decimals         INTEGER,
    base_token              TEXT,
    quote_token             TEXT,
    is_inverted             BOOLEAN,
    quote_token_priority    INTEGER,
    protocol                TEXT,
    protocol_version        TEXT,
    factory                 TEXT,
    fee                     INTEGER,
    initial_fee             INTEGER,
    hook_address            TEXT,
    created_at              TIMESTAMPTZ,
    block_number            BIGINT,
    tx_hash                 TEXT,
    reserve0                TEXT,
    reserve1                TEXT,
    reserve0_adjusted       DOUBLE PRECISION,
    reserve1_adjusted       DOUBLE PRECISION,
    sqrt_price_x96          TEXT,
    tick                    INTEGER,
    tick_spacing            INTEGER,
    liquidity               TEXT,
    price                   DOUBLE PRECISION,
    token0_price            DOUBLE PRECISION,
    token1_price            DOUBLE PRECISION,
    price_usd               DOUBLE PRECISION,
    price_change_24h        DOUBLE PRECISION,
    price_change_7d         DOUBLE PRECISION,
    volume_24h              DOUBLE PRECISION,
    swaps_24h               BIGINT,
    total_swaps             BIGINT,
    total_volume_usd        DOUBLE PRECISION,
    tvl_usd                 DOUBLE PRECISION,
    last_swap_at            TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    PRIMARY KEY (chain_id, address)
);

CREATE INDEX IF NOT EXISTS idx_pools_token0 ON indexer.pools (chain_id, token0);
CREATE INDEX IF NOT EXISTS idx_pools_token1 ON indexer.pools (chain_id, token1);
CREATE INDEX IF NOT EXISTS idx_pools_protocol ON indexer.pools (chain_id, protocol);
CREATE INDEX IF NOT EXISTS idx_pools_tvl ON indexer.pools (chain_id, tvl_usd DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_pools_volume_24h ON indexer.pools (chain_id, volume_24h DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_pools_created_at ON indexer.pools (chain_id, created_at DESC);
-- Index for "ORDER BY total_volume_usd" (Top Pools by Volume panel)
CREATE INDEX IF NOT EXISTS idx_pools_total_volume ON indexer.pools (chain_id, total_volume_usd DESC NULLS LAST);
-- Index for 24h TVL queries (filters by updated_at)
CREATE INDEX IF NOT EXISTS idx_pools_updated_at ON indexer.pools (chain_id, updated_at DESC);
-- Partial index for TVL > 0 queries (much smaller, faster to scan)
CREATE INDEX IF NOT EXISTS idx_pools_tvl_positive ON indexer.pools (chain_id, tvl_usd DESC NULLS LAST) WHERE tvl_usd > 0;
-- Covering index for Top Pools by TVL panel (index-only scans)
CREATE INDEX IF NOT EXISTS idx_pools_tvl_covering ON indexer.pools (chain_id, tvl_usd DESC NULLS LAST)
    INCLUDE (address, protocol, token0_symbol, token1_symbol, volume_24h, price_usd) WHERE tvl_usd > 0;
-- Covering index for Top Pools by Volume panel (index-only scans)
CREATE INDEX IF NOT EXISTS idx_pools_volume_covering ON indexer.pools (chain_id, total_volume_usd DESC NULLS LAST)
    INCLUDE (address, protocol, token0_symbol, token1_symbol, tvl_usd, price_usd);

CREATE TABLE IF NOT EXISTS indexer.pools_by_token (
    chain_id            BIGINT NOT NULL,
    token_address       TEXT NOT NULL,
    pool_address        TEXT NOT NULL,
    paired_token        TEXT,
    paired_token_symbol TEXT,
    protocol            TEXT,
    protocol_version    TEXT,
    fee                 INTEGER,
    tvl_usd             DOUBLE PRECISION,
    volume_24h          DOUBLE PRECISION,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ,
    PRIMARY KEY (chain_id, token_address, pool_address)
);

CREATE INDEX IF NOT EXISTS idx_pools_by_token_tvl ON indexer.pools_by_token (chain_id, token_address, tvl_usd DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_pools_by_token_volume ON indexer.pools_by_token (chain_id, token_address, volume_24h DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS indexer.native_token_prices (
    chain_id            BIGINT PRIMARY KEY,
    price_usd           DOUBLE PRECISION,
    updated_at          TIMESTAMPTZ
);

-- Materialized view for pre-aggregated pool stats (refreshed periodically)
-- Eliminates repeated COUNT(*) and SUM(tvl_usd) queries on the large pools table
CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_pool_summary AS
SELECT
    chain_id,
    COUNT(*) as pool_count,
    COALESCE(SUM(tvl_usd), 0) as total_tvl_usd,
    COALESCE(SUM(CASE WHEN tvl_usd > 0 THEN tvl_usd ELSE 0 END), 0) as active_tvl_usd,
    COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '24 hours' THEN 1 END) as pools_updated_24h,
    COALESCE(SUM(CASE WHEN updated_at >= NOW() - INTERVAL '24 hours' THEN tvl_usd ELSE 0 END), 0) as tvl_24h
FROM indexer.pools
GROUP BY chain_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pool_summary_chain ON indexer.mv_pool_summary (chain_id);

-- Materialized view for pre-aggregated token stats
CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_token_summary AS
SELECT
    chain_id,
    COUNT(*) as token_count,
    COALESCE(SUM(market_cap_usd), 0) as total_market_cap
FROM indexer.tokens
GROUP BY chain_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_token_summary_chain ON indexer.mv_token_summary (chain_id);

-- Function to refresh all summary materialized views (call via pg_cron or trigger)
CREATE OR REPLACE FUNCTION indexer.refresh_summary_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY indexer.mv_pool_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY indexer.mv_token_summary;
END;
$$ LANGUAGE plpgsql;
