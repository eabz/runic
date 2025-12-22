//! Log parsing module for HyperSync logs.
//!
//! Pre-parses blockchain logs into typed structures to avoid redundant parsing
//! in multiple processing passes.

use alloy::{
    primitives::{LogData, B256},
    sol_types::SolEvent,
};
use rustc_hash::FxHashMap;

use crate::{
    abis::{transfer, v2, v3, v4},
    utils::hex_encode,
};

/// Pre-parsed log data to avoid re-parsing in multiple passes.
/// Contains all extracted metadata needed for processing.
pub enum ParsedLog {
    // Pool creation events
    V2PairCreated {
        event: v2::PairCreated,
        log_address: String,
        block_number: u64,
        tx_hash: String,
        block_timestamp: u64,
    },
    V3PoolCreated {
        event: v3::PoolCreated,
        log_address: String,
        block_number: u64,
        tx_hash: String,
        block_timestamp: u64,
    },
    V4Initialize {
        event: v4::Initialize,
        log_address: String,
        block_number: u64,
        tx_hash: String,
        block_timestamp: u64,
    },
    // V3 Initialize (sets initial price)
    V3Initialize {
        event: v3::Initialize,
        log_address: String,
        block_number: u64,
        block_timestamp: u64,
    },
    // Transfer events
    Transfer {
        event: transfer::Transfer,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    WethDeposit {
        event: transfer::Deposit,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    WethWithdrawal {
        event: transfer::Withdrawal,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    // Liquidity events
    V2Mint {
        event: v2::Mint,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    V3Mint {
        event: v3::Mint,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    V2Burn {
        event: v2::Burn,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    V3Burn {
        event: v3::Burn,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    V2Sync {
        event: v2::Sync,
        log_address: String,
        block_number: u64,
        block_timestamp: u64,
    },
    V3Collect {
        event: v3::Collect,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    V4ModifyLiquidity {
        event: v4::ModifyLiquidity,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    // Swap events
    V2Swap {
        event: v2::Swap,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    V3Swap {
        event: v3::Swap,
        log_address: String,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
    V4Swap {
        event: v4::Swap,
        block_number: u64,
        log_index: u32,
        tx_hash: String,
        block_timestamp: u64,
    },
}

/// Result of parsing logs from a HyperSync response.
pub struct ParseResult {
    /// Pre-parsed logs in sequential order
    pub parsed_logs: Vec<ParsedLog>,
    /// Token addresses found in logs (for fetching token metadata)
    pub token_addresses: Vec<String>,
    /// Pool addresses modified by events (for fetching pool state)
    pub modified_pools_addresses: Vec<String>,
}

/// Parse HyperSync logs into typed structures.
///
/// This function:
/// 1. Extracts all logs from the response
/// 2. Decodes each log based on its topic0 signature
/// 3. Collects token addresses and modified pool addresses
/// 4. Returns parsed logs in sequential order (critical for correct processing)
pub fn parse_logs(
    logs: impl Iterator<Item = hypersync_client::simple_types::Log>,
    block_timestamps: &FxHashMap<u64, u64>,
    log_count_estimate: usize,
) -> ParseResult {
    let mut parsed_logs: Vec<ParsedLog> = Vec::with_capacity(log_count_estimate);
    let mut token_addresses: Vec<String> = Vec::with_capacity(log_count_estimate * 2);
    let mut modified_pools_addresses: Vec<String> = Vec::with_capacity(log_count_estimate);

    for log in logs {
        // Ignore logs without topics
        if log.topics.is_empty() {
            continue;
        }

        // Parse the log data as raw bytes
        let data = log
            .data
            .as_ref()
            .map(|d| d.as_ref().to_vec())
            .unwrap_or_default()
            .into();

        // Parse the log topics as alloy B256
        let topics: Vec<B256> = log
            .topics
            .iter()
            .flatten()
            .map(|t| B256::from_slice(t.as_ref()))
            .collect();

        let log_data = LogData::new_unchecked(topics, data);
        let Some(topic0) = log_data.topics().first() else {
            continue;
        };

        let tx_hash = log
            .transaction_hash
            .as_ref()
            .map(|h| hex_encode(h.as_ref()))
            .unwrap_or_default();

        let block_number: u64 = log.block_number.map(|x| x.into()).unwrap_or(0);
        let block_timestamp = block_timestamps.get(&block_number).copied().unwrap_or(0);

        let log_index = log
            .log_index
            .map(|i| {
                let v: u64 = i.into();
                v as u32
            })
            .unwrap_or(0);

        let log_address = log
            .address
            .as_ref()
            .map(|a| hex_encode(a.as_ref()).to_lowercase())
            .unwrap_or_default();

        match topic0 {
            // Transfer events
            t if t == &transfer::Transfer::SIGNATURE_HASH.0 => {
                token_addresses.push(log_address.clone());
                if let Ok(event) = transfer::Transfer::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::Transfer {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &transfer::Deposit::SIGNATURE_HASH.0 => {
                if let Ok(event) = transfer::Deposit::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::WethDeposit {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &transfer::Withdrawal::SIGNATURE_HASH.0 => {
                if let Ok(event) = transfer::Withdrawal::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::WethWithdrawal {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            // Pool creation events
            t if t == &v2::PairCreated::SIGNATURE_HASH.0 => {
                if let Ok(event) = v2::PairCreated::decode_log_data(&log_data) {
                    token_addresses.push(hex_encode(event.token0.as_slice()));
                    token_addresses.push(hex_encode(event.token1.as_slice()));
                    parsed_logs.push(ParsedLog::V2PairCreated {
                        event,
                        log_address,
                        block_number,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v3::PoolCreated::SIGNATURE_HASH.0 => {
                if let Ok(event) = v3::PoolCreated::decode_log_data(&log_data) {
                    token_addresses.push(hex_encode(event.token0.as_slice()));
                    token_addresses.push(hex_encode(event.token1.as_slice()));
                    parsed_logs.push(ParsedLog::V3PoolCreated {
                        event,
                        log_address,
                        block_number,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v4::Initialize::SIGNATURE_HASH.0 => {
                if let Ok(event) = v4::Initialize::decode_log_data(&log_data) {
                    token_addresses.push(hex_encode(event.currency0.as_slice()));
                    token_addresses.push(hex_encode(event.currency1.as_slice()));
                    parsed_logs.push(ParsedLog::V4Initialize {
                        event,
                        log_address,
                        block_number,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v3::Initialize::SIGNATURE_HASH.0 => {
                if let Ok(event) = v3::Initialize::decode_log_data(&log_data) {
                    modified_pools_addresses.push(log_address.clone());
                    parsed_logs.push(ParsedLog::V3Initialize {
                        event,
                        log_address,
                        block_number,
                        block_timestamp,
                    });
                }
            },
            // Liquidity events
            t if t == &v2::Mint::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v2::Mint::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V2Mint {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v3::Mint::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v3::Mint::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V3Mint {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v2::Burn::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v2::Burn::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V2Burn {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v3::Burn::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v3::Burn::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V3Burn {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v2::Sync::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v2::Sync::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V2Sync {
                        event,
                        log_address,
                        block_number,
                        block_timestamp,
                    });
                }
            },
            t if t == &v3::Collect::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v3::Collect::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V3Collect {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v4::ModifyLiquidity::SIGNATURE_HASH.0 => {
                if let Ok(event) = v4::ModifyLiquidity::decode_log_data(&log_data) {
                    modified_pools_addresses.push(hex_encode(event.id.as_slice()));
                    parsed_logs.push(ParsedLog::V4ModifyLiquidity {
                        event,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            // Swap events
            t if t == &v2::Swap::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v2::Swap::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V2Swap {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v3::Swap::SIGNATURE_HASH.0 => {
                modified_pools_addresses.push(log_address.clone());
                if let Ok(event) = v3::Swap::decode_log_data(&log_data) {
                    parsed_logs.push(ParsedLog::V3Swap {
                        event,
                        log_address,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            t if t == &v4::Swap::SIGNATURE_HASH.0 => {
                if let Ok(event) = v4::Swap::decode_log_data(&log_data) {
                    modified_pools_addresses.push(hex_encode(event.id.as_slice()));
                    parsed_logs.push(ParsedLog::V4Swap {
                        event,
                        block_number,
                        log_index,
                        tx_hash,
                        block_timestamp,
                    });
                }
            },
            _ => {},
        }
    }

    ParseResult {
        parsed_logs,
        token_addresses,
        modified_pools_addresses,
    }
}
