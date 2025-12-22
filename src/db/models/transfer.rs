use clickhouse::{types::UInt256, Row};
use serde::Serialize;
use time::OffsetDateTime;

use crate::{
    abis::transfer,
    utils::{hex_encode, u256_to_f64, ZERO_ADDRESS},
};

/// Native token decimals (always 18 for EVM chains)
const NATIVE_DECIMALS: i32 = 18;

/// ERC20 Transfer event (ClickHouse)
#[derive(Debug, Clone, Serialize, Row)]
pub struct Transfer {
    // Identifiers
    pub chain_id: u64,
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u32,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub timestamp: OffsetDateTime,

    // Transfer data
    pub token_address: String,
    pub from_address: String,
    pub to_address: String,
    pub amount: UInt256,
    pub amount_adjusted: f64,
}

impl Transfer {
    /// Create a Transfer from a standard ERC20 Transfer event
    pub fn from_event(
        chain_id: i64,
        token_address: String,
        event: transfer::Transfer,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        timestamp: u32,
        decimals: i32,
    ) -> Self {
        let block_timestamp = OffsetDateTime::from_unix_timestamp(timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount = UInt256::from_le_bytes(event.value.to_le_bytes());

        // Use event.value (alloy U256) for decimal adjustment, not the converted UInt256
        let amount_adjusted = u256_to_f64(event.value, decimals);

        Self {
            chain_id: chain_id as u64,
            block_number,
            timestamp: block_timestamp,
            tx_hash,
            log_index,
            token_address: token_address.to_lowercase(),
            from_address: hex_encode(event.from.as_slice()),
            to_address: hex_encode(event.to.as_slice()),
            amount,
            amount_adjusted,
        }
    }

    /// Create a Transfer from a WETH Deposit event (wrap ETH -> WETH)
    ///
    /// This is treated as a mint: from zero address to user
    pub fn from_weth_deposit(
        chain_id: i64,
        token_address: String,
        event: transfer::Deposit,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        timestamp: u32,
    ) -> Self {
        let block_timestamp = OffsetDateTime::from_unix_timestamp(timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount = UInt256::from_le_bytes(event.amount.to_le_bytes());
        let amount_adjusted = u256_to_f64(event.amount, NATIVE_DECIMALS);

        Self {
            chain_id: chain_id as u64,
            block_number,
            timestamp: block_timestamp,
            tx_hash,
            log_index,
            token_address: token_address.to_lowercase(),
            from_address: ZERO_ADDRESS.to_string(),
            to_address: hex_encode(event.user.as_slice()),
            amount,
            amount_adjusted,
        }
    }

    /// Create a Transfer from a WETH Withdrawal event (unwrap WETH -> ETH)
    ///
    /// This is treated as a burn: from user to zero address
    pub fn from_weth_withdrawal(
        chain_id: i64,
        token_address: String,
        event: transfer::Withdrawal,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        timestamp: u32,
    ) -> Self {
        let block_timestamp = OffsetDateTime::from_unix_timestamp(timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount = UInt256::from_le_bytes(event.amount.to_le_bytes());
        let amount_adjusted = u256_to_f64(event.amount, NATIVE_DECIMALS);

        Self {
            chain_id: chain_id as u64,
            block_number,
            timestamp: block_timestamp,
            tx_hash,
            log_index,
            token_address: token_address.to_lowercase(),
            from_address: hex_encode(event.user.as_slice()),
            to_address: ZERO_ADDRESS.to_string(),
            amount,
            amount_adjusted,
        }
    }
}
