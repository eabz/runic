use clickhouse::Row;
use serde::Serialize;

/// Token search entry for ClickHouse full-text search.
///
/// Uses bloom filter indexes for fast symbol/name matching.
///
/// Query Pattern: "Search tokens matching 'USD'"
#[derive(Debug, Clone, Serialize, Row)]
pub struct TokenSearch {
    pub chain_id: u64,
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
}

impl TokenSearch {
    pub fn new(chain_id: u64, address: String, symbol: String, name: String, decimals: u8) -> Self {
        Self {
            chain_id,
            address,
            symbol,
            name,
            decimals,
        }
    }
}
