use alloy::primitives::U256;
use clickhouse::{types::UInt256, Row};
use serde::Serialize;
use time::OffsetDateTime;

use crate::{
    abis::{v2, v3, v4},
    db::models::{Pool, Token},
    utils::{
        calculate_mint_amounts, hex_encode, into_u256, sqrt_price_x96_str_to_adjusted_price,
        u256_to_f64,
    },
};

/// DEX Event (swap, mint, burn, collect, modify_liquidity) stored in ClickHouse
///
/// All fields match the ClickHouse schema with appropriate defaults.
#[derive(Debug, Clone, Serialize, Row)]
pub struct Event {
    // Identifiers
    pub chain_id: u64,
    pub block_number: u64,
    pub tx_hash: String,
    pub tx_index: u32,
    pub log_index: u32,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub timestamp: OffsetDateTime,

    // Topology
    pub pool_address: String,
    pub token0: String,
    pub token1: String,

    // Actors
    pub maker: String, // Swap: tx.origin or router
    pub owner: String, // Mint/Burn: LP position owner

    // Event type
    pub event_type: String, // 'swap', 'mint', 'burn', 'collect', 'modify_liquidity'

    // Amounts (raw)
    pub amount0: UInt256,
    pub amount1: UInt256,

    // Amounts (adjusted)
    pub amount0_adjusted: f64,
    pub amount1_adjusted: f64,

    // Amount direction
    pub amount0_direction: i8,
    pub amount1_direction: i8,

    // Prices & values
    pub price: f64,
    pub price_usd: f64,
    pub volume_usd: f64,
    pub fees_usd: f64,
    pub fee: u32,
    pub is_suspicious: bool, // True if price manipulation suspected

    // V3/V4 concentrated liquidity data
    pub sqrt_price_x96: UInt256,
    pub tick: i32,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub liquidity: UInt256,
}

impl Event {
    pub fn from_v2_swap(
        chain_id: u64,
        event: v2::Swap,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        // V2 `amount0In` > `amount0Out` => user sent tokens to pool (Net Input).
        // Our convention: Pool Gain = -1, Pool Loss = 1.
        let (raw_amount0, amount0_direction) = if event.amount0In > event.amount0Out {
            (event.amount0In - event.amount0Out, -1i8)
        } else {
            (event.amount0Out - event.amount0In, 1i8)
        };

        let amount0 = into_u256(raw_amount0);
        let amount0_adjusted = u256_to_f64(raw_amount0, token0.decimals);

        let (raw_amount1, amount1_direction) = if event.amount1In > event.amount1Out {
            (event.amount1In - event.amount1Out, -1i8)
        } else {
            (event.amount1Out - event.amount1In, 1i8)
        };
        let amount1 = into_u256(raw_amount1);
        let amount1_adjusted = u256_to_f64(raw_amount1, token1.decimals);

        // Calculate price from swap amounts (simple V2 price)
        let price = if amount0_adjusted.abs() > 1e-15 {
            (amount1_adjusted / amount0_adjusted).abs()
        } else {
            0.0
        };

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: hex_encode(event.sender.as_slice()),
            owner: String::new(),
            event_type: String::from("swap"),
            amount0,
            amount1,
            amount0_direction,
            amount1_direction,
            amount0_adjusted,
            amount1_adjusted,
            price,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: UInt256::from_le_bytes([0u8; 32]),
            tick: 0,
            tick_lower: 0,
            tick_upper: 0,
            liquidity: UInt256::from_le_bytes([0u8; 32]),
        }
    }

    pub fn from_v3_swap(
        chain_id: u64,
        event: v3::Swap,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        // V3 Swap: amount0/amount1 are I256
        // Negative = tokens flow OUT of pool (user receives) = Pool Loss = 1
        // Positive = tokens flow INTO pool (user sends) = Pool Gain = -1
        let (abs_amount0, amount0_direction) = if event.amount0.is_negative() {
            (event.amount0.unsigned_abs(), 1i8)
        } else {
            (event.amount0.unsigned_abs(), -1i8)
        };

        let (abs_amount1, amount1_direction) = if event.amount1.is_negative() {
            (event.amount1.unsigned_abs(), 1i8)
        } else {
            (event.amount1.unsigned_abs(), -1i8)
        };

        let amount0 = into_u256(abs_amount0);
        let amount1 = into_u256(abs_amount1);

        let amount0_adjusted = u256_to_f64(abs_amount0, token0.decimals);
        let amount1_adjusted = u256_to_f64(abs_amount1, token1.decimals);

        let sqrt_price_u256 = U256::from(event.sqrtPriceX96);
        let price = sqrt_price_x96_str_to_adjusted_price(
            &sqrt_price_u256.to_string(),
            token0.decimals,
            token1.decimals,
        )
        .unwrap_or(0.0);

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: hex_encode(event.sender.as_slice()),
            owner: String::new(),
            event_type: String::from("swap"),
            amount0,
            amount1,
            amount0_direction,
            amount1_direction,
            amount0_adjusted,
            amount1_adjusted,
            price,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: into_u256(sqrt_price_u256),
            tick: event.tick.try_into().unwrap_or(0),
            tick_lower: 0,
            tick_upper: 0,
            liquidity: into_u256(U256::from(event.liquidity)),
        }
    }

    pub fn from_v4_swap(
        chain_id: u64,
        event: v4::Swap,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        // V4 Swap: int128 amounts
        let (abs_amount0, amount0_direction) = if event.amount0 < 0 {
            (event.amount0.unsigned_abs(), 1i8)
        } else {
            (event.amount0.unsigned_abs(), -1i8)
        };

        let (abs_amount1, amount1_direction) = if event.amount1 < 0 {
            (event.amount1.unsigned_abs(), 1i8)
        } else {
            (event.amount1.unsigned_abs(), -1i8)
        };

        let abs_amount0_u256 = U256::from(abs_amount0);
        let abs_amount1_u256 = U256::from(abs_amount1);

        let amount0 = into_u256(abs_amount0_u256);
        let amount1 = into_u256(abs_amount1_u256);

        let amount0_adjusted = u256_to_f64(abs_amount0_u256, token0.decimals);
        let amount1_adjusted = u256_to_f64(abs_amount1_u256, token1.decimals);

        let sqrt_price_u256 = U256::from(event.sqrtPriceX96);
        let price = sqrt_price_x96_str_to_adjusted_price(
            &sqrt_price_u256.to_string(),
            token0.decimals,
            token1.decimals,
        )
        .unwrap_or(0.0);

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: hex_encode(event.sender.as_slice()),
            owner: String::new(),
            event_type: String::from("swap"),
            amount0,
            amount1,
            amount0_direction,
            amount1_direction,
            amount0_adjusted,
            amount1_adjusted,
            price,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: event.fee.to::<u32>(),
            is_suspicious: false,
            sqrt_price_x96: into_u256(sqrt_price_u256),
            tick: event.tick.try_into().unwrap_or(0),
            tick_lower: 0,
            tick_upper: 0,
            liquidity: into_u256(U256::from(event.liquidity)),
        }
    }

    pub fn from_v2_mint(
        chain_id: u64,
        event: v2::Mint,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount0 = into_u256(event.amount0);
        let amount1 = into_u256(event.amount1);
        let amount0_adjusted = u256_to_f64(event.amount0, token0.decimals);
        let amount1_adjusted = u256_to_f64(event.amount1, token1.decimals);

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: String::new(),
            owner: hex_encode(event.sender.as_slice()),
            event_type: String::from("mint"),
            amount0,
            amount1,
            amount0_direction: -1,
            amount1_direction: -1,
            amount0_adjusted,
            amount1_adjusted,
            price: 0.0,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: UInt256::from_le_bytes([0u8; 32]),
            tick: 0,
            tick_lower: 0,
            tick_upper: 0,
            liquidity: UInt256::from_le_bytes([0u8; 32]),
        }
    }

    pub fn from_v2_burn(
        chain_id: u64,
        event: v2::Burn,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount0 = into_u256(event.amount0);
        let amount1 = into_u256(event.amount1);
        let amount0_adjusted = u256_to_f64(event.amount0, token0.decimals);
        let amount1_adjusted = u256_to_f64(event.amount1, token1.decimals);

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: String::new(),
            owner: hex_encode(event.sender.as_slice()),
            event_type: String::from("burn"),
            amount0,
            amount1,
            amount0_direction: 1,
            amount1_direction: 1,
            amount0_adjusted,
            amount1_adjusted,
            price: 0.0,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: UInt256::from_le_bytes([0u8; 32]),
            tick: 0,
            tick_lower: 0,
            tick_upper: 0,
            liquidity: UInt256::from_le_bytes([0u8; 32]),
        }
    }

    pub fn from_v3_mint(
        chain_id: u64,
        event: v3::Mint,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount0 = into_u256(event.amount0);
        let amount1 = into_u256(event.amount1);
        let amount0_adjusted = u256_to_f64(event.amount0, token0.decimals);
        let amount1_adjusted = u256_to_f64(event.amount1, token1.decimals);

        let liquidity = into_u256(U256::from(event.amount));

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: String::new(),
            owner: hex_encode(event.owner.as_slice()),
            event_type: String::from("mint"),
            amount0,
            amount1,
            amount0_direction: -1,
            amount1_direction: -1,
            amount0_adjusted,
            amount1_adjusted,
            price: 0.0,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: UInt256::from_le_bytes([0u8; 32]),
            tick: 0,
            tick_lower: event.tickLower.try_into().unwrap_or(0),
            tick_upper: event.tickUpper.try_into().unwrap_or(0),
            liquidity,
        }
    }

    pub fn from_v3_burn(
        chain_id: u64,
        event: v3::Burn,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount0 = into_u256(event.amount0);
        let amount1 = into_u256(event.amount1);
        let amount0_adjusted = u256_to_f64(event.amount0, token0.decimals);
        let amount1_adjusted = u256_to_f64(event.amount1, token1.decimals);

        let liquidity = into_u256(U256::from(event.amount));

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: String::new(),
            owner: hex_encode(event.owner.as_slice()),
            event_type: String::from("burn"),
            amount0,
            amount1,
            amount0_direction: 1,
            amount1_direction: 1,
            amount0_adjusted,
            amount1_adjusted,
            price: 0.0,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: UInt256::from_le_bytes([0u8; 32]),
            tick: 0,
            tick_lower: event.tickLower.try_into().unwrap_or(0),
            tick_upper: event.tickUpper.try_into().unwrap_or(0),
            liquidity,
        }
    }

    pub fn from_v3_collect(
        chain_id: u64,
        event: v3::Collect,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let amount0_u256 = U256::from(event.amount0);
        let amount1_u256 = U256::from(event.amount1);

        let amount0 = into_u256(amount0_u256);
        let amount1 = into_u256(amount1_u256);
        let amount0_adjusted = u256_to_f64(amount0_u256, token0.decimals);
        let amount1_adjusted = u256_to_f64(amount1_u256, token1.decimals);

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: String::new(),
            owner: hex_encode(event.owner.as_slice()),
            event_type: String::from("collect"),
            amount0,
            amount1,
            amount0_direction: 1,
            amount1_direction: 1,
            amount0_adjusted,
            amount1_adjusted,
            price: 0.0,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: UInt256::from_le_bytes([0u8; 32]),
            tick: 0,
            tick_lower: event.tickLower.try_into().unwrap_or(0),
            tick_upper: event.tickUpper.try_into().unwrap_or(0),
            liquidity: UInt256::from_le_bytes([0u8; 32]),
        }
    }

    pub fn from_v4_modify_liquidity(
        chain_id: u64,
        event: v4::ModifyLiquidity,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        log_index: u32,
        pool_address: String,
        block_timestamp: u64,
        pool: &Pool,
    ) -> Self {
        let timestamp = OffsetDateTime::from_unix_timestamp(block_timestamp as i64)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);

        let delta = event.liquidityDelta;

        // Negative delta = removing liquidity (burn) = tokens flow OUT = 1
        // Positive delta = adding liquidity (mint) = tokens flow IN = -1
        let (liq_abs, direction) = if delta.is_negative() {
            (delta.unsigned_abs(), 1i8)
        } else {
            (delta.unsigned_abs(), -1i8)
        };

        let (amount0_f64, amount1_f64) = if let Some(current_tick) = pool.tick {
            let liquidity_f64 = u256_to_f64(U256::from(liq_abs), 0);

            calculate_mint_amounts(
                liquidity_f64,
                current_tick,
                event.tickLower.as_i32(),
                event.tickUpper.as_i32(),
            )
        } else {
            (0.0, 0.0)
        };

        let amount0 =
            into_u256(U256::from_str_radix(&format!("{:.0}", amount0_f64), 10).unwrap_or_default());
        let amount1 =
            into_u256(U256::from_str_radix(&format!("{:.0}", amount1_f64), 10).unwrap_or_default());

        let amount0_adjusted = u256_to_f64(
            U256::from_str_radix(&format!("{:.0}", amount0_f64), 10).unwrap_or_default(),
            token0.decimals,
        );
        let amount1_adjusted = u256_to_f64(
            U256::from_str_radix(&format!("{:.0}", amount1_f64), 10).unwrap_or_default(),
            token1.decimals,
        );

        Self {
            chain_id,
            block_number,
            timestamp,
            tx_hash,
            tx_index: 0,
            log_index,
            pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            maker: String::new(),
            owner: hex_encode(event.sender.as_slice()),
            event_type: String::from("modify_liquidity"),
            amount0,
            amount1,
            amount0_direction: direction,
            amount1_direction: direction,
            amount0_adjusted,
            amount1_adjusted,
            price: 0.0,
            price_usd: 0.0,
            volume_usd: 0.0,
            fees_usd: 0.0,
            fee: 0,
            is_suspicious: false,
            sqrt_price_x96: UInt256::from_le_bytes([0u8; 32]),
            tick: 0,
            tick_lower: event.tickLower.try_into().unwrap_or(0),
            tick_upper: event.tickUpper.try_into().unwrap_or(0),
            liquidity: into_u256(U256::from(liq_abs)),
        }
    }
}
