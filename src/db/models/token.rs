use chrono::{DateTime, Utc};

/// Token metadata and current market state (PostgreSQL)
///
/// Primary Key: (chain_id, address)
/// Query Pattern: "Get token info for address X on chain Y"
#[derive(Debug, Clone, serde::Serialize)]
pub struct Token {
    // Primary key
    pub chain_id: u64,
    pub address: String,

    // On-chain metadata (immutable after first fetch)
    pub symbol: String,
    pub name: String,
    pub decimals: u8,

    // Current price state
    pub price_usd: Option<f64>,
    pub price_updated_at: Option<DateTime<Utc>>,

    // Price changes (rolling windows)
    pub price_change_24h: Option<f64>,
    pub price_change_7d: Option<f64>,

    // Visual assets (external/manual)
    pub logo_url: Option<String>,
    pub banner_url: Option<String>,

    // Social links (external/manual)
    pub website: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub discord: Option<String>,

    // Rolling window stats (24h)
    pub volume_24h: Option<f64>,
    pub swaps_24h: Option<u64>,

    // Lifetime stats
    pub total_swaps: Option<u64>,
    pub total_volume_usd: Option<f64>,
    pub pool_count: Option<u64>,

    // Market data (calculated internally)
    pub circulating_supply: Option<f64>,
    pub market_cap_usd: Option<f64>,

    // Activity tracking
    pub first_seen_block: Option<u64>,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl Token {
    /// Constructor for just metadata (used by TokenFetcher)
    pub fn new(chain_id: u64, address: String, symbol: String, name: String, decimals: u8) -> Self {
        Self {
            chain_id,
            // Always lowercase addresses for consistent comparisons
            address: address.to_lowercase(),
            symbol,
            name,
            decimals,
            price_usd: None,
            price_updated_at: None,
            price_change_24h: None,
            price_change_7d: None,
            logo_url: None,
            banner_url: None,
            website: None,
            twitter: None,
            telegram: None,
            discord: None,
            volume_24h: None,
            swaps_24h: None,
            total_swaps: None,
            total_volume_usd: None,
            pool_count: None,
            circulating_supply: None,
            market_cap_usd: None,
            first_seen_block: None,
            last_activity_at: None,
            updated_at: None,
        }
    }
}
