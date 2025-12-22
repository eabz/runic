use clickhouse::Row;
use serde::Serialize;
use time::OffsetDateTime;

/// Tracks recently created pools for discovery feeds.
///
/// Population: Inserted when pool creation event is indexed.
///
/// Query Patterns:
///   - "Get newest pools across all chains"
///   - "Get new pools for chain X in last 24h"
///   - "Get new pools for token X"
#[derive(Debug, Clone, Serialize, Row)]
pub struct NewPool {
    // Identifiers
    pub chain_id: u64,
    pub pool_address: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub created_at: OffsetDateTime,
    pub block_number: u64,
    pub tx_hash: String,

    // Pool metadata (denormalized for display)
    pub token0: String,
    pub token1: String,
    pub token0_symbol: String,
    pub token1_symbol: String,

    // Protocol info
    pub protocol: String,
    pub protocol_version: String,
    pub fee: u32,

    // Initial metrics
    pub initial_tvl_usd: f64,
}

impl NewPool {
    pub fn new(
        chain_id: u64,
        pool_address: String,
        created_at: OffsetDateTime,
        block_number: u64,
        tx_hash: String,
        token0: String,
        token1: String,
        token0_symbol: String,
        token1_symbol: String,
        protocol: String,
        protocol_version: String,
        fee: u32,
        initial_tvl_usd: f64,
    ) -> Self {
        Self {
            chain_id,
            pool_address,
            created_at,
            block_number,
            tx_hash,
            token0,
            token1,
            token0_symbol,
            token1_symbol,
            protocol,
            protocol_version,
            fee,
            initial_tvl_usd,
        }
    }

    /// Create from pool creation event data
    pub fn from_pool_created(
        chain_id: u64,
        pool_address: String,
        block_number: u64,
        tx_hash: String,
        block_timestamp: u64,
        token0: String,
        token1: String,
        token0_symbol: String,
        token1_symbol: String,
        protocol: String,
        protocol_version: String,
        fee: u32,
    ) -> Self {
        let created_at = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        Self {
            chain_id,
            pool_address,
            created_at,
            block_number,
            tx_hash,
            token0,
            token1,
            token0_symbol,
            token1_symbol,
            protocol,
            protocol_version,
            fee,
            initial_tvl_usd: 0.0,
        }
    }

    /// Update the initial TVL from the pool's calculated TVL
    pub fn set_initial_tvl(&mut self, tvl_usd: f64) {
        self.initial_tvl_usd = tvl_usd;
    }
}
