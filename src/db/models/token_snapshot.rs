use clickhouse::Row;
use serde::Serialize;
use time::OffsetDateTime;

/// Hourly snapshot of token metrics for historical charts.
///
/// Population: Background job aggregates hourly at top of hour.
///
/// Query Patterns:
///   - "Get price history for token X over 24h/7d"
///   - "Get volume trend for token X"
#[derive(Debug, Clone, Serialize, Row)]
pub struct TokenSnapshot {
    // Identifiers
    pub chain_id: u64,
    pub token_address: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub time: OffsetDateTime,

    // Price data (OHLC for the day)
    pub price_usd: f64,
    pub price_open: f64,
    pub price_high: f64,
    pub price_low: f64,

    // Market data
    pub market_cap_usd: f64,
    pub circulating_supply: f64,

    // Activity metrics
    pub volume_usd: f64,
    pub swap_count: u64,
    pub pool_count: u32,
}

impl TokenSnapshot {
    pub fn new(
        chain_id: u64,
        token_address: String,
        time: OffsetDateTime,
        price_usd: f64,
        price_open: f64,
        price_high: f64,
        price_low: f64,
        market_cap_usd: f64,
        circulating_supply: f64,
        volume_usd: f64,
        swap_count: u64,
        pool_count: u32,
    ) -> Self {
        Self {
            chain_id,
            token_address,
            time,
            price_usd,
            price_open,
            price_high,
            price_low,
            market_cap_usd,
            circulating_supply,
            volume_usd,
            swap_count,
            pool_count,
        }
    }
}
