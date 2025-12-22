use clickhouse::{types::UInt256, Row};
use serde::Serialize;
use time::OffsetDateTime;

/// Hourly snapshot of pool state for historical charts.
///
/// Population: Background job snapshots pool state periodically.
///
/// Query Patterns:
///   - "Get TVL history for pool X over 30 days"
///   - "Get volume trend for pool X"
#[derive(Debug, Clone, Serialize, Row)]
pub struct PoolSnapshot {
    // Identifiers
    pub chain_id: u64,
    pub pool_address: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub time: OffsetDateTime,

    // Price state
    pub price: f64,
    pub price_usd: f64,

    // Liquidity state
    pub tvl_usd: f64,
    pub reserve0: f64,
    pub reserve1: f64,
    pub liquidity: UInt256,

    // Period metrics (rolling 24h at snapshot time)
    pub volume_24h: f64,
    pub swaps_24h: u64,
    pub fees_24h: f64,
}

impl PoolSnapshot {
    pub fn new(
        chain_id: u64,
        pool_address: String,
        time: OffsetDateTime,
        price: f64,
        price_usd: f64,
        tvl_usd: f64,
        reserve0: f64,
        reserve1: f64,
        liquidity: UInt256,
        volume_24h: f64,
        swaps_24h: u64,
        fees_24h: f64,
    ) -> Self {
        Self {
            chain_id,
            pool_address,
            time,
            price,
            price_usd,
            tvl_usd,
            reserve0,
            reserve1,
            liquidity,
            volume_24h,
            swaps_24h,
            fees_24h,
        }
    }
}
