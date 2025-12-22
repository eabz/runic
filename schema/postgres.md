# PostgreSQL Schema Documentation

This document describes the PostgreSQL schema for the indexer, converted from the ScyllaDB schema.

## Schema Overview

All tables are created within the `indexer` schema.

## Tables

### sync_checkpoints

Tracks the indexing progress for each blockchain.

| Column | Type | Description |
|--------|------|-------------|
| chain_id | BIGINT | Primary key. Unique identifier for the blockchain |
| last_indexed_block | BIGINT | The last block number that was successfully indexed |
| updated_at | TIMESTAMPTZ | Timestamp of the last update |

### chains

Stores configuration and metadata for each supported blockchain.

| Column | Type | Description |
|--------|------|-------------|
| chain_id | BIGINT | Primary key. Unique identifier for the blockchain |
| name | TEXT | Human-readable name of the chain |
| enabled | BOOLEAN | Whether indexing is enabled for this chain |
| rpc_url | TEXT | RPC endpoint URL for the chain |
| hypersync_url | TEXT | HyperSync endpoint URL for fast data retrieval |
| native_token_address | TEXT | Address of the native token (e.g., WETH) |
| native_token_decimals | INTEGER | Decimal places for the native token |
| native_token_name | TEXT | Name of the native token |
| native_token_symbol | TEXT | Symbol of the native token |
| stable_token_address | TEXT | Address of the primary stablecoin |
| stable_token_decimals | INTEGER | Decimal places for the stablecoin |
| stable_pool_address | TEXT | Address of the native/stable liquidity pool |
| stablecoins | TEXT[] | List of stablecoin addresses on this chain |
| major_tokens | TEXT[] | List of major token addresses for routing |
| updated_at | TIMESTAMPTZ | Timestamp of the last update |

### tokens

Stores token metadata, pricing, and trading statistics.

| Column | Type | Description |
|--------|------|-------------|
| chain_id | BIGINT | Part of composite primary key |
| address | TEXT | Part of composite primary key. Token contract address |
| symbol | TEXT | Token symbol (e.g., "USDC") |
| name | TEXT | Full token name |
| decimals | INTEGER | Token decimal places |
| price_usd | DOUBLE PRECISION | Current USD price |
| price_updated_at | TIMESTAMPTZ | When the price was last updated |
| price_change_24h | DOUBLE PRECISION | 24-hour price change percentage |
| price_change_7d | DOUBLE PRECISION | 7-day price change percentage |
| logo_url | TEXT | URL to the token logo |
| banner_url | TEXT | URL to the token banner image |
| website | TEXT | Official website URL |
| twitter | TEXT | Twitter/X handle or URL |
| telegram | TEXT | Telegram group URL |
| discord | TEXT | Discord server URL |
| volume_24h | DOUBLE PRECISION | 24-hour trading volume in USD |
| swaps_24h | BIGINT | Number of swaps in the last 24 hours |
| total_swaps | BIGINT | Total number of swaps all-time |
| total_volume_usd | DOUBLE PRECISION | Total trading volume in USD |
| pool_count | BIGINT | Number of liquidity pools containing this token |
| circulating_supply | DOUBLE PRECISION | Circulating token supply |
| market_cap_usd | DOUBLE PRECISION | Market capitalization in USD |
| first_seen_block | BIGINT | Block number when token was first indexed |
| last_activity_at | TIMESTAMPTZ | Timestamp of last trading activity |
| updated_at | TIMESTAMPTZ | Timestamp of the last update |

**Indexes:**
- `idx_tokens_symbol` - Lookup by chain and symbol
- `idx_tokens_market_cap` - Sort by market cap (descending)
- `idx_tokens_volume_24h` - Sort by 24h volume (descending)

### pools

Stores liquidity pool information including reserves, pricing, and statistics.

| Column | Type | Description |
|--------|------|-------------|
| chain_id | BIGINT | Part of composite primary key |
| address | TEXT | Part of composite primary key. Pool contract address |
| token0 | TEXT | Address of the first token in the pair |
| token1 | TEXT | Address of the second token in the pair |
| token0_symbol | TEXT | Symbol of token0 |
| token1_symbol | TEXT | Symbol of token1 |
| token0_decimals | INTEGER | Decimal places for token0 |
| token1_decimals | INTEGER | Decimal places for token1 |
| base_token | TEXT | Address of the base token for price calculation |
| quote_token | TEXT | Address of the quote token for price calculation |
| is_inverted | BOOLEAN | Whether the price is inverted |
| quote_token_priority | INTEGER | Priority for selecting quote token |
| protocol | TEXT | DEX protocol name (e.g., "uniswap") |
| protocol_version | TEXT | Protocol version (e.g., "v2", "v3") |
| factory | TEXT | Factory contract address that created this pool |
| fee | INTEGER | Pool fee in basis points or ppm |
| hook_address | TEXT | Hook contract address (for Uniswap v4) |
| created_at | TIMESTAMPTZ | When the pool was created |
| block_number | BIGINT | Block number when pool was created |
| tx_hash | TEXT | Transaction hash of pool creation |
| reserve0 | TEXT | Raw reserve amount for token0 |
| reserve1 | TEXT | Raw reserve amount for token1 |
| reserve0_adjusted | DOUBLE PRECISION | Decimal-adjusted reserve for token0 |
| reserve1_adjusted | DOUBLE PRECISION | Decimal-adjusted reserve for token1 |
| sqrt_price_x96 | TEXT | Square root price (for concentrated liquidity pools) |
| tick | INTEGER | Current tick (for concentrated liquidity pools) |
| tick_spacing | INTEGER | Tick spacing (for concentrated liquidity pools) |
| liquidity | TEXT | Current liquidity value |
| price | DOUBLE PRECISION | Current price (token1/token0) |
| token0_price | DOUBLE PRECISION | Price of token0 in terms of token1 |
| token1_price | DOUBLE PRECISION | Price of token1 in terms of token0 |
| price_usd | DOUBLE PRECISION | Current price in USD |
| price_change_24h | DOUBLE PRECISION | 24-hour price change percentage |
| price_change_7d | DOUBLE PRECISION | 7-day price change percentage |
| volume_24h | DOUBLE PRECISION | 24-hour trading volume in USD |
| swaps_24h | BIGINT | Number of swaps in the last 24 hours |
| total_swaps | BIGINT | Total number of swaps all-time |
| total_volume_usd | DOUBLE PRECISION | Total trading volume in USD |
| tvl_usd | DOUBLE PRECISION | Total value locked in USD |
| last_swap_at | TIMESTAMPTZ | Timestamp of the last swap |
| updated_at | TIMESTAMPTZ | Timestamp of the last update |

**Indexes:**
- `idx_pools_token0` - Find pools by token0 address
- `idx_pools_token1` - Find pools by token1 address
- `idx_pools_protocol` - Filter by protocol
- `idx_pools_tvl` - Sort by TVL (descending)
- `idx_pools_volume_24h` - Sort by 24h volume (descending)
- `idx_pools_created_at` - Sort by creation date (descending)

### pools_by_token

Denormalized table for efficient lookup of pools by token address.

| Column | Type | Description |
|--------|------|-------------|
| chain_id | BIGINT | Part of composite primary key |
| token_address | TEXT | Part of composite primary key. The token to look up pools for |
| pool_address | TEXT | Part of composite primary key. The pool containing this token |
| paired_token | TEXT | Address of the other token in the pool |
| paired_token_symbol | TEXT | Symbol of the paired token |
| protocol | TEXT | DEX protocol name |
| protocol_version | TEXT | Protocol version |
| fee | INTEGER | Pool fee |
| tvl_usd | DOUBLE PRECISION | Total value locked in USD |
| volume_24h | DOUBLE PRECISION | 24-hour trading volume in USD |
| created_at | TIMESTAMPTZ | When the pool was created |
| updated_at | TIMESTAMPTZ | Timestamp of the last update |

**Indexes:**
- `idx_pools_by_token_tvl` - Sort pools for a token by TVL (descending)
- `idx_pools_by_token_volume` - Sort pools for a token by 24h volume (descending)

### native_token_prices

Caches the current price of each chain's native token.

| Column | Type | Description |
|--------|------|-------------|
| chain_id | BIGINT | Primary key. Unique identifier for the blockchain |
| price_usd | DOUBLE PRECISION | Current USD price of the native token |
| updated_at | TIMESTAMPTZ | Timestamp of the last price update |

## Type Mappings

| ScyllaDB Type | PostgreSQL Type |
|---------------|-----------------|
| bigint | BIGINT |
| text | TEXT |
| boolean | BOOLEAN |
| double | DOUBLE PRECISION |
| int | INTEGER |
| timestamp | TIMESTAMPTZ |
| list\<text\> | TEXT[] |

