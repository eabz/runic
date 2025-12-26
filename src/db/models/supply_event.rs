use crate::utils::{into_u256, u256_to_f64};
use clickhouse::{types::UInt256, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct SupplyEvent {
    pub chain_id: u64,
    pub block_number: u64,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub timestamp: time::OffsetDateTime,
    pub tx_hash: String,
    pub log_index: u32,
    pub token_address: String,
    #[serde(rename = "type")]
    pub event_type: String, // "mint" or "burn"
    pub amount: UInt256,
    pub amount_adjusted: f64,
}

impl SupplyEvent {
    pub fn new(
        chain_id: u64,
        block_number: u64,
        timestamp: u64,
        tx_hash: String,
        log_index: u32,
        token_address: String,
        event_type: String,
        amount: alloy::primitives::U256,
        decimals: u8,
    ) -> Self {
        let amount_adjusted = u256_to_f64(amount, decimals);
        Self {
            chain_id,
            block_number,
            timestamp: time::OffsetDateTime::from_unix_timestamp(timestamp as i64).unwrap(),
            tx_hash,
            log_index,
            token_address: token_address.to_lowercase(),
            event_type,
            amount: into_u256(amount),
            amount_adjusted,
        }
    }
}
