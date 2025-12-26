CREATE TABLE IF NOT EXISTS indexer.events (
    chain_id            UInt64 CODEC(Delta, LZ4),
    block_number        UInt64 CODEC(Delta, ZSTD(1)),
    tx_hash             String CODEC(ZSTD(1)),
    tx_index            UInt32 DEFAULT 0 CODEC(Delta, LZ4),
    log_index           UInt32 CODEC(Delta, LZ4),
    timestamp           DateTime CODEC(DoubleDelta, ZSTD(1)),
    pool_address        String CODEC(ZSTD(1)),
    token0              String CODEC(ZSTD(1)),
    token1              String CODEC(ZSTD(1)),
    maker               String DEFAULT '' CODEC(ZSTD(1)),
    owner               String DEFAULT '' CODEC(ZSTD(1)),
    event_type          LowCardinality(String),
    amount0             UInt256 CODEC(ZSTD(1)),
    amount1             UInt256 CODEC(ZSTD(1)),
    amount0_adjusted    Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    amount1_adjusted    Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    amount0_direction   Int8 DEFAULT 0 CODEC(T64, LZ4),
    amount1_direction   Int8 DEFAULT 0 CODEC(T64, LZ4),
    price               Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    price_usd           Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    volume_usd          Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    fees_usd            Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    fee                 UInt32 DEFAULT 0 CODEC(T64, LZ4),
    is_suspicious       UInt8 DEFAULT 0 CODEC(T64, LZ4),
    sqrt_price_x96      UInt256 DEFAULT 0 CODEC(ZSTD(1)),
    tick                Int32 DEFAULT 0 CODEC(Delta, LZ4),
    tick_lower          Int32 DEFAULT 0 CODEC(Delta, LZ4),
    tick_upper          Int32 DEFAULT 0 CODEC(Delta, LZ4),
    liquidity           UInt256 DEFAULT 0 CODEC(ZSTD(1)),
    
    PROJECTION by_token0 (
        SELECT * ORDER BY (chain_id, token0, timestamp, tx_hash, log_index)
    ),
    PROJECTION by_token1 (
        SELECT * ORDER BY (chain_id, token1, timestamp, tx_hash, log_index)
    ),
    PROJECTION by_maker (
        SELECT * ORDER BY (chain_id, maker, timestamp, tx_hash, log_index)
    ),
    PROJECTION by_owner (
        SELECT * ORDER BY (chain_id, owner, timestamp, tx_hash, log_index)
    ),
    -- Projection for efficient timestamp-based queries (ClickHouse scans in reverse for DESC)
    PROJECTION by_time_desc (
        SELECT * ORDER BY (chain_id, timestamp, tx_hash, log_index)
    ),
    
    INDEX idx_event_type event_type TYPE set(0) GRANULARITY 4,
    INDEX idx_block_number block_number TYPE minmax GRANULARITY 4,
    -- Index for volume filtering (whale queries)
    INDEX idx_volume_usd volume_usd TYPE minmax GRANULARITY 4,
    -- Index for suspicious event filtering (manipulation detection)
    INDEX idx_suspicious is_suspicious TYPE set(0) GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (chain_id, pool_address, timestamp, tx_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS indexer.candles_1m (
    chain_id        UInt64 CODEC(Delta, LZ4),
    pool_address    String CODEC(ZSTD(1)),
    time            DateTime CODEC(DoubleDelta, ZSTD(1)),
    open            Float64 CODEC(Gorilla, ZSTD(1)),
    high            Float64 CODEC(Gorilla, ZSTD(1)),
    low             Float64 CODEC(Gorilla, ZSTD(1)),
    close           Float64 CODEC(Gorilla, ZSTD(1)),
    volume_usd      Float64 CODEC(Gorilla, ZSTD(1)),
    fees_usd        Float64 CODEC(Gorilla, ZSTD(1)),
    tx_count        UInt64 CODEC(Delta, LZ4)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (chain_id, pool_address, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_candles_1m TO indexer.candles_1m AS
SELECT
    chain_id,
    pool_address,
    toStartOfMinute(timestamp) AS time,
    argMin(price_usd, timestamp) AS open,
    max(price_usd) AS high,
    min(price_usd) AS low,
    argMax(price_usd, timestamp) AS close,
    sum(volume_usd) AS volume_usd,
    sum(fees_usd) AS fees_usd,
    count() AS tx_count
FROM indexer.events
WHERE event_type = 'swap' AND price_usd > 0
GROUP BY chain_id, pool_address, time;

CREATE TABLE IF NOT EXISTS indexer.candles_5m (
    chain_id        UInt64 CODEC(Delta, LZ4),
    pool_address    String CODEC(ZSTD(1)),
    time            DateTime CODEC(DoubleDelta, ZSTD(1)),
    open            Float64 CODEC(Gorilla, ZSTD(1)),
    high            Float64 CODEC(Gorilla, ZSTD(1)),
    low             Float64 CODEC(Gorilla, ZSTD(1)),
    close           Float64 CODEC(Gorilla, ZSTD(1)),
    volume_usd      Float64 CODEC(Gorilla, ZSTD(1)),
    fees_usd        Float64 CODEC(Gorilla, ZSTD(1)),
    tx_count        UInt64 CODEC(Delta, LZ4)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (chain_id, pool_address, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_candles_5m TO indexer.candles_5m AS
SELECT
    chain_id,
    pool_address,
    toStartOfFiveMinutes(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume_usd) AS volume_usd,
    sum(fees_usd) AS fees_usd,
    sum(tx_count) AS tx_count
FROM indexer.candles_1m
GROUP BY chain_id, pool_address, time;

CREATE TABLE IF NOT EXISTS indexer.candles_15m (
    chain_id        UInt64 CODEC(Delta, LZ4),
    pool_address    String CODEC(ZSTD(1)),
    time            DateTime CODEC(DoubleDelta, ZSTD(1)),
    open            Float64 CODEC(Gorilla, ZSTD(1)),
    high            Float64 CODEC(Gorilla, ZSTD(1)),
    low             Float64 CODEC(Gorilla, ZSTD(1)),
    close           Float64 CODEC(Gorilla, ZSTD(1)),
    volume_usd      Float64 CODEC(Gorilla, ZSTD(1)),
    fees_usd        Float64 CODEC(Gorilla, ZSTD(1)),
    tx_count        UInt64 CODEC(Delta, LZ4)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (chain_id, pool_address, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_candles_15m TO indexer.candles_15m AS
SELECT
    chain_id,
    pool_address,
    toStartOfFifteenMinutes(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume_usd) AS volume_usd,
    sum(fees_usd) AS fees_usd,
    sum(tx_count) AS tx_count
FROM indexer.candles_5m
GROUP BY chain_id, pool_address, time;

CREATE TABLE IF NOT EXISTS indexer.candles_1h (
    chain_id        UInt64 CODEC(Delta, LZ4),
    pool_address    String CODEC(ZSTD(1)),
    time            DateTime CODEC(DoubleDelta, ZSTD(1)),
    open            Float64 CODEC(Gorilla, ZSTD(1)),
    high            Float64 CODEC(Gorilla, ZSTD(1)),
    low             Float64 CODEC(Gorilla, ZSTD(1)),
    close           Float64 CODEC(Gorilla, ZSTD(1)),
    volume_usd      Float64 CODEC(Gorilla, ZSTD(1)),
    fees_usd        Float64 CODEC(Gorilla, ZSTD(1)),
    tx_count        UInt64 CODEC(Delta, LZ4)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (chain_id, pool_address, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_candles_1h TO indexer.candles_1h AS
SELECT
    chain_id,
    pool_address,
    toStartOfHour(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume_usd) AS volume_usd,
    sum(fees_usd) AS fees_usd,
    sum(tx_count) AS tx_count
FROM indexer.candles_15m
GROUP BY chain_id, pool_address, time;

CREATE TABLE IF NOT EXISTS indexer.candles_4h (
    chain_id        UInt64 CODEC(Delta, LZ4),
    pool_address    String CODEC(ZSTD(1)),
    time            DateTime CODEC(DoubleDelta, ZSTD(1)),
    open            Float64 CODEC(Gorilla, ZSTD(1)),
    high            Float64 CODEC(Gorilla, ZSTD(1)),
    low             Float64 CODEC(Gorilla, ZSTD(1)),
    close           Float64 CODEC(Gorilla, ZSTD(1)),
    volume_usd      Float64 CODEC(Gorilla, ZSTD(1)),
    fees_usd        Float64 CODEC(Gorilla, ZSTD(1)),
    tx_count        UInt64 CODEC(Delta, LZ4)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (chain_id, pool_address, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_candles_4h TO indexer.candles_4h AS
SELECT
    chain_id,
    pool_address,
    toStartOfInterval(time, INTERVAL 4 HOUR) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume_usd) AS volume_usd,
    sum(fees_usd) AS fees_usd,
    sum(tx_count) AS tx_count
FROM indexer.candles_1h
GROUP BY chain_id, pool_address, time;

CREATE TABLE IF NOT EXISTS indexer.candles_1d (
    chain_id        UInt64 CODEC(Delta, LZ4),
    pool_address    String CODEC(ZSTD(1)),
    time            Date CODEC(Delta, LZ4),
    open            Float64 CODEC(Gorilla, ZSTD(1)),
    high            Float64 CODEC(Gorilla, ZSTD(1)),
    low             Float64 CODEC(Gorilla, ZSTD(1)),
    close           Float64 CODEC(Gorilla, ZSTD(1)),
    volume_usd      Float64 CODEC(Gorilla, ZSTD(1)),
    fees_usd        Float64 CODEC(Gorilla, ZSTD(1)),
    tx_count        UInt64 CODEC(Delta, LZ4)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYear(time)
ORDER BY (chain_id, pool_address, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_candles_1d TO indexer.candles_1d AS
SELECT
    chain_id,
    pool_address,
    toDate(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume_usd) AS volume_usd,
    sum(fees_usd) AS fees_usd,
    sum(tx_count) AS tx_count
FROM indexer.candles_4h
GROUP BY chain_id, pool_address, time;

CREATE TABLE IF NOT EXISTS indexer.pool_snapshots (
    chain_id        UInt64 CODEC(Delta, LZ4),
    pool_address    String CODEC(ZSTD(1)),
    time            DateTime CODEC(DoubleDelta, ZSTD(1)),
    price           Float64 CODEC(Gorilla, ZSTD(1)),
    price_usd       Float64 CODEC(Gorilla, ZSTD(1)),
    tvl_usd         Float64 CODEC(Gorilla, ZSTD(1)),
    reserve0        Float64 CODEC(Gorilla, ZSTD(1)),
    reserve1        Float64 CODEC(Gorilla, ZSTD(1)),
    liquidity       UInt256 DEFAULT 0 CODEC(ZSTD(1)),
    volume_24h      Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    swaps_24h       UInt64 DEFAULT 0 CODEC(Delta, LZ4),
    fees_24h        Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    INDEX idx_tvl tvl_usd TYPE minmax GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (chain_id, pool_address, time);

CREATE TABLE IF NOT EXISTS indexer.token_snapshots (
    chain_id            UInt64 CODEC(Delta, LZ4),
    token_address       String CODEC(ZSTD(1)),
    time                DateTime CODEC(DoubleDelta, ZSTD(1)),
    price_usd           Float64 CODEC(Gorilla, ZSTD(1)),
    price_open          Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    price_high          Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    price_low           Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    market_cap_usd      Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    circulating_supply  Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    volume_usd          Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    swap_count          UInt64 DEFAULT 0 CODEC(Delta, LZ4),
    pool_count          UInt32 DEFAULT 0 CODEC(Delta, LZ4),
    INDEX idx_market_cap market_cap_usd TYPE minmax GRANULARITY 4,
    INDEX idx_volume volume_usd TYPE minmax GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (chain_id, token_address, time);



CREATE TABLE IF NOT EXISTS indexer.trader_stats (
    chain_id            UInt64 CODEC(Delta, LZ4),
    address             String CODEC(ZSTD(1)),
    total_swaps         SimpleAggregateFunction(sum, UInt64),
    total_volume_usd    SimpleAggregateFunction(sum, Float64),
    total_fees_usd      SimpleAggregateFunction(sum, Float64),
    first_trade         SimpleAggregateFunction(min, DateTime),
    last_trade          SimpleAggregateFunction(max, DateTime),
    INDEX idx_volume total_volume_usd TYPE minmax GRANULARITY 4
) ENGINE = AggregatingMergeTree()
ORDER BY (chain_id, address);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_trader_stats 
TO indexer.trader_stats AS
SELECT
    chain_id,
    maker AS address,
    toUInt64(1) AS total_swaps,
    volume_usd AS total_volume_usd,
    fees_usd AS total_fees_usd,
    timestamp AS first_trade,
    timestamp AS last_trade
FROM indexer.events
WHERE event_type = 'swap' 
  AND maker != '' 
  AND maker != '0x0000000000000000000000000000000000000000';

CREATE TABLE IF NOT EXISTS indexer.new_pools (
    chain_id            UInt64 CODEC(Delta, LZ4),
    pool_address        String CODEC(ZSTD(1)),
    created_at          DateTime CODEC(DoubleDelta, ZSTD(1)),
    block_number        UInt64 CODEC(Delta, ZSTD(1)),
    tx_hash             String CODEC(ZSTD(1)),
    token0              String CODEC(ZSTD(1)),
    token1              String CODEC(ZSTD(1)),
    token0_symbol       String CODEC(ZSTD(1)),
    token1_symbol       String CODEC(ZSTD(1)),
    protocol            LowCardinality(String),
    protocol_version    LowCardinality(String),
    fee                 UInt32 DEFAULT 0 CODEC(T64, LZ4),
    initial_tvl_usd     Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    
    PROJECTION by_created (
        SELECT * ORDER BY (chain_id, created_at, pool_address)
    ),
    PROJECTION by_token0 (
        SELECT * ORDER BY (chain_id, token0, created_at)
    ),
    PROJECTION by_token1 (
        SELECT * ORDER BY (chain_id, token1, created_at)
    ),
    
    INDEX idx_protocol protocol TYPE set(0) GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (chain_id, pool_address);

-- Pre-aggregated global stats for fast "All Time" dashboard metrics
CREATE TABLE IF NOT EXISTS indexer.global_stats (
    chain_id            UInt64 CODEC(Delta, LZ4),
    total_swaps         SimpleAggregateFunction(sum, UInt64),
    total_volume_usd    SimpleAggregateFunction(sum, Float64),
    total_fees_usd      SimpleAggregateFunction(sum, Float64),
    total_events        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY chain_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_global_stats 
TO indexer.global_stats AS
SELECT
    chain_id,
    toUInt64(if(event_type = 'swap', 1, 0)) AS total_swaps,
    if(event_type = 'swap', volume_usd, 0) AS total_volume_usd,
    if(event_type = 'swap', fees_usd, 0) AS total_fees_usd,
    toUInt64(1) AS total_events
FROM indexer.events;

-- Pre-aggregated hourly stats for fast "24h" dashboard metrics
-- Queries for 24h metrics only need to sum 24 rows instead of millions
CREATE TABLE IF NOT EXISTS indexer.hourly_stats (
    chain_id            UInt64 CODEC(Delta, LZ4),
    hour                DateTime CODEC(DoubleDelta, ZSTD(1)),
    swap_count          SimpleAggregateFunction(sum, UInt64),
    volume_usd          SimpleAggregateFunction(sum, Float64),
    fees_usd            SimpleAggregateFunction(sum, Float64),
    event_count         SimpleAggregateFunction(sum, UInt64),
    active_pools        AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (chain_id, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_hourly_stats 
TO indexer.hourly_stats AS
SELECT
    chain_id,
    toStartOfHour(timestamp) AS hour,
    countIf(event_type = 'swap') AS swap_count,
    sumIf(volume_usd, event_type = 'swap') AS volume_usd,
    sumIf(fees_usd, event_type = 'swap') AS fees_usd,
    count() AS event_count,
    uniqState(pool_address) AS active_pools
FROM indexer.events
GROUP BY chain_id, hour;



-- Pre-aggregated global pool counts (for "New Pools (All Time)" panel)
CREATE TABLE IF NOT EXISTS indexer.global_pool_stats (
    chain_id            UInt64 CODEC(Delta, LZ4),
    pool_count          SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY chain_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_global_pool_stats 
TO indexer.global_pool_stats AS
SELECT
    chain_id,
    toUInt64(1) AS pool_count
FROM indexer.new_pools;

-- Pre-aggregated hourly new pools stats (for "New Pools/24h" and "New Tokens/24h" panels)
CREATE TABLE IF NOT EXISTS indexer.hourly_new_pools_stats (
    chain_id            UInt64 CODEC(Delta, LZ4),
    hour                DateTime CODEC(DoubleDelta, ZSTD(1)),
    pool_count          SimpleAggregateFunction(sum, UInt64),
    unique_tokens       AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (chain_id, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_hourly_new_pools_stats 
TO indexer.hourly_new_pools_stats AS
SELECT
    chain_id,
    toStartOfHour(created_at) AS hour,
    toUInt64(1) AS pool_count,
    uniqState(arrayJoin([token0, token1])) AS unique_tokens
FROM indexer.new_pools
GROUP BY chain_id, hour;

-- Supply Events (Mints and Burns)
CREATE TABLE IF NOT EXISTS indexer.supply_events (
    chain_id            UInt64 CODEC(Delta, LZ4),
    block_number        UInt64 CODEC(Delta, ZSTD(1)),
    timestamp           DateTime CODEC(DoubleDelta, ZSTD(1)),
    tx_hash             String CODEC(ZSTD(1)),
    log_index           UInt32 CODEC(Delta, LZ4),
    token_address       String CODEC(ZSTD(1)),
    type                LowCardinality(String), -- 'mint' or 'burn'
    amount              UInt256 CODEC(ZSTD(1)),
    amount_adjusted     Float64 DEFAULT 0 CODEC(Gorilla, ZSTD(1)),
    
    INDEX idx_token token_address TYPE minmax GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (chain_id, token_address, timestamp);

-- Aggregated Token Supplies
CREATE TABLE IF NOT EXISTS indexer.token_supplies (
    chain_id            UInt64 CODEC(Delta, LZ4),
    token_address       String CODEC(ZSTD(1)),
    total_supply        SimpleAggregateFunction(sum, Float64), -- Utilizing adjusted amounts for easier aggregation
    total_minted        SimpleAggregateFunction(sum, Float64),
    total_burnt         SimpleAggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
ORDER BY (chain_id, token_address);

CREATE MATERIALIZED VIEW IF NOT EXISTS indexer.mv_token_supplies
TO indexer.token_supplies AS
SELECT
    chain_id,
    token_address,
    sumIf(amount_adjusted, type = 'mint') - sumIf(amount_adjusted, type = 'burn') AS total_supply,
    sumIf(amount_adjusted, type = 'mint') AS total_minted,
    sumIf(amount_adjusted, type = 'burn') AS total_burnt
FROM indexer.supply_events
GROUP BY chain_id, token_address;


