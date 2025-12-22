# ClickHouse Schema Documentation

## Overview

**Purpose:** Historical data storage and fast analytics for DEX data

This database stores time-series and historical data:
- All swap, mint, burn, collect events
- OHLCV candlestick data at multiple resolutions
- Token transfer history and balance aggregation
- Historical snapshots for TVL/price charts

## Design Principles

1. Optimized for analytical queries (aggregations, time-series)
2. Heavy use of Materialized Views for pre-aggregation
3. Projections for alternative query patterns without data duplication
4. Partitioned by month for efficient data lifecycle management
5. TTL policies to automatically expire old granular data

---

## Section 1: Raw Events

### Table: `events`

**Purpose:** Store all DEX events (swaps, mints, burns, collects)

**Query Patterns:**
- "Get recent swaps for pool X" → Primary key
- "Get all swaps involving token X" → `by_token0`/`by_token1` projections
- "Get user's swap history" → `by_maker` projection
- "Get LP activity for user" → `by_owner` projection

**Event Types:**
- `swap` - Token exchange
- `mint` - Add liquidity (V2/V3/V4)
- `burn` - Remove liquidity (V2/V3/V4)
- `collect` - Collect fees (V3)
- `modify_liquidity` - Add/remove liquidity (V4)

**Data Lifecycle:**
- Raw events have 1-year TTL (configurable)
- Aggregated data (candles, snapshots) kept longer

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| chain_id | UInt64 | Blockchain identifier |
| block_number | UInt64 | Block containing this event |
| tx_hash | String | Transaction hash |
| tx_index | UInt32 | Position in block (for MEV analysis) |
| log_index | UInt32 | Event index within transaction |
| timestamp | DateTime | Block timestamp |
| pool_address | String | Pool contract address |
| token0/token1 | String | Token addresses |
| maker | String | User who swapped (tx.origin or router) |
| owner | String | LP position owner |
| event_type | LowCardinality(String) | Event type |
| amount0/amount1 | UInt256 | Raw amounts from blockchain |
| amount0_adjusted/amount1_adjusted | Float64 | Decimal-adjusted amounts |
| amount0_direction/amount1_direction | Int8 | Flow direction (-1=in, 1=out, 0=N/A) |
| price | Float64 | Execution price (quote/base) |
| price_usd | Float64 | Base token price in USD |
| volume_usd | Float64 | Trade volume in USD |
| sqrt_price_x96 | UInt256 | V3/V4: sqrt(price) × 2^96 |
| tick | Int32 | V3/V4: Current tick |
| tick_lower/tick_upper | Int32 | V3/V4: Position bounds |
| liquidity | UInt256 | V3/V4: Liquidity amount |

**Projections:**
- `by_token0` - Query events by token0
- `by_token1` - Query events by token1
- `by_maker` - Query swap history by user
- `by_owner` - Query LP activity by user

---

## Section 2: OHLCV Candlesticks

**Purpose:** Pre-aggregated OHLCV data for efficient charting

**Resolution Hierarchy:**
```
1m  → Raw candles from events (base)
5m  → Aggregated from 1m
15m → Aggregated from 5m  
1h  → Aggregated from 15m
4h  → Aggregated from 1h
1d  → Aggregated from 4h
```

**Why Multiple Resolutions?**
- 1-year chart with 1m candles = 525,600 rows per pool
- 1-year chart with 1d candles = 365 rows per pool
- Pre-aggregation trades storage for query speed

**Tables:**
- `candles_1m` - 1-minute candles (base)
- `candles_5m` - 5-minute candles
- `candles_15m` - 15-minute candles
- `candles_1h` - 1-hour candles
- `candles_4h` - 4-hour candles
- `candles_1d` - Daily candles

**Materialized Views:**
Each candle table has a corresponding MV that auto-populates from the lower resolution.

---

## Section 3: Historical Snapshots

### Table: `pool_snapshots` (Hourly)

**Purpose:** Historical pool state for TVL/metrics charts

**Query Patterns:**
- "Get TVL history for pool X over 30 days"
- "Get volume trend for pool X"
- "Compare pool metrics across time"

**Population:** Background job queries current state + aggregates periodically

**TTL:** 2 years

### Table: `token_snapshots` (Daily)

**Purpose:** Historical token metrics for price/market cap charts

**Query Patterns:**
- "Get price history for token X over 90 days"
- "Get market cap trend for token X"
- "Compare token performance"

**Population:** Background job aggregates daily at midnight UTC

**TTL:** 5 years

---

## Section 4: Token Transfers

### Table: `transfers`

**Purpose:** Track all ERC20 token transfers

**Query Patterns:**
- "Get all transfers for token X"
- "Get transfer history for user X" (via projection)

**Uses:**
- Calculate circulating supply (mints - burns)
- Track whale movements
- Portfolio tracking

**Projections:**
- `by_sender` - Query transfers sent by user
- `by_receiver` - Query transfers received by user

**TTL:** 1 year

### Table: `transfers_state` (Aggregated Balances)

**Purpose:** Maintain running balance per user per token

**Engine:** SummingMergeTree automatically sums numeric columns on merge

**Query Patterns:**
- "Get balance of token X for user Y"
- "Get all token balances for user Y"

**Balance Calculation:** `balance = value_in - value_out`

---

## Section 5: Token Search

### Table: `token_search`

**Purpose:** Fast token search by symbol/name

**Query Pattern:** "Search tokens matching 'USD'"

**Why Separate from ScyllaDB?**
- ScyllaDB: Point lookups by exact address
- ClickHouse: Fuzzy search with bloom filters

**Indexes:**
- Bloom filters for fast text search (`tokenbf_v1`)
- Ngram index for partial matching (`ngrambf_v1`)

---

## Section 6: Trader Statistics

### Table: `trader_stats`

**Purpose:** Aggregated trading statistics per user

**Query Patterns:**
- "Get top traders by volume"
- "Get trading stats for user X"
- "Leaderboard queries"

**Engine:** AggregatingMergeTree for efficient incremental aggregation

**Materialized View:** `mv_trader_stats` auto-aggregates from swap events

---

## Section 7: New Pools Discovery

### Table: `new_pools`

**Purpose:** Track recently created pools for discovery feeds

**Query Patterns:**
- "Get newest pools across all chains"
- "Get new pools for chain X in last 24h"
- "Get new pools for token X"

**Population:** Inserted when pool creation event is indexed

**Projections:**
- `by_created` - Query by creation time
- `by_token0` - Query by token0
- `by_token1` - Query by token1

**TTL:** 90 days

---

## Query Examples

```sql
-- Get OHLCV candles for pool (1-hour resolution)
SELECT * FROM indexer.candles_1h 
WHERE chain_id = 1 AND pool_address = '0x...'
  AND time >= now() - INTERVAL 7 DAY
ORDER BY time;

-- Get user's swap history
SELECT * FROM indexer.events
WHERE chain_id = 1 AND maker = '0x...'
ORDER BY timestamp DESC
LIMIT 100;

-- Get TVL history for pool
SELECT time, tvl_usd FROM indexer.pool_snapshots
WHERE chain_id = 1 AND pool_address = '0x...'
  AND time >= now() - INTERVAL 30 DAY
ORDER BY time;

-- Get top traders by volume
SELECT address, total_volume_usd, total_swaps
FROM indexer.trader_stats
WHERE chain_id = 1
ORDER BY total_volume_usd DESC
LIMIT 100;

-- Search tokens by symbol
SELECT * FROM indexer.token_search
WHERE chain_id = 1 AND symbol LIKE '%USD%';

-- Get user's token balance
SELECT token_address, value_in - value_out as balance
FROM indexer.transfers_state
WHERE chain_id = 1 AND owner = '0x...'
ORDER BY balance DESC;

-- Get newest pools in last 24h
SELECT * FROM indexer.new_pools
WHERE chain_id = 1 AND created_at >= now() - INTERVAL 1 DAY
ORDER BY created_at DESC;
```

---

## Maintenance Notes

### TTL Policies
| Table | TTL |
|-------|-----|
| events | 1 year |
| transfers | 1 year |
| pool_snapshots | 2 years |
| token_snapshots | 5 years |
| new_pools | 90 days |
| candles | No TTL (small) |

### Background Jobs Required
- `pool_snapshots`: Hourly job to snapshot pool state
- `token_snapshots`: Daily job to snapshot token metrics

### Projections
- Automatically maintained by ClickHouse
- Use `EXPLAIN` to verify projection is being used

### Optimization
- Run `OPTIMIZE TABLE` periodically for better compression
- Monitor parts count with `system.parts`

