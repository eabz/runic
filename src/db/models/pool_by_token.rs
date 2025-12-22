use chrono::{DateTime, Utc};

use super::Pool;

/// Pool indexed by token address for efficient path finding (PostgreSQL)
///
/// Primary Key: (chain_id, token_address, pool_address)
/// Query Pattern: "Find all pools containing token X on chain Y"
///
/// This table is populated in both directions for each pool:
/// - (token0, pool) with paired_token = token1
/// - (token1, pool) with paired_token = token0
#[derive(Debug, Clone)]
pub struct PoolByToken {
    pub chain_id: i64,
    pub token_address: String,
    pub pool_address: String,
    pub paired_token: String,
    pub paired_token_symbol: String,
    pub protocol: Option<String>,
    pub protocol_version: Option<String>,
    pub fee: Option<i32>,
    pub tvl_usd: Option<f64>,
    pub volume_24h: Option<f64>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl PoolByToken {
    /// Create two PoolByToken entries from a Pool (one for each token direction)
    pub fn from_pool(pool: &Pool) -> (Self, Self) {
        let entry_for_token0 = Self {
            chain_id: pool.chain_id,
            token_address: pool.token0.clone(),
            pool_address: pool.address.clone(),
            paired_token: pool.token1.clone(),
            paired_token_symbol: pool.token1_symbol.clone(),
            protocol: pool.protocol.clone(),
            protocol_version: pool.protocol_version.clone(),
            fee: pool.fee,
            tvl_usd: pool.tvl_usd,
            volume_24h: pool.volume_24h,
            created_at: pool.created_at,
            updated_at: pool.updated_at,
        };

        let entry_for_token1 = Self {
            chain_id: pool.chain_id,
            token_address: pool.token1.clone(),
            pool_address: pool.address.clone(),
            paired_token: pool.token0.clone(),
            paired_token_symbol: pool.token0_symbol.clone(),
            protocol: pool.protocol.clone(),
            protocol_version: pool.protocol_version.clone(),
            fee: pool.fee,
            tvl_usd: pool.tvl_usd,
            volume_24h: pool.volume_24h,
            created_at: pool.created_at,
            updated_at: pool.updated_at,
        };

        (entry_for_token0, entry_for_token1)
    }
}
