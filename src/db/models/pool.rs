use alloy::primitives::{Address, U256};
use chrono::{DateTime, Utc};
use clickhouse::types::UInt256;

use crate::{
    abis::{v2, v3, v4},
    db::models::{chain::ChainTokens, Event, Token},
    utils::{
        bigint_add, bigint_sub, hex_encode, reserve_to_f64, sqrt_price_x96_str_to_adjusted_price,
        validate_price_ratio,
    },
};

/// Priority tiers for quote token detection
/// Higher priority token becomes the quote token (pricing reference)
/// Lower priority token becomes the base token (the one being priced)
///
/// Uses arrays from chains table:
///   - stablecoins array: All tokens in this array get Stablecoin priority (100)
///   - major_tokens array: All tokens in this array get MajorToken priority (50)
///
/// For WMON/USDC pools:
///   - USDC (Stablecoin, priority 100) → quote token (reference, $1)
///   - WMON (WrappedNative, priority 80) → base token (being priced, price_usd = WMON price)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum QuoteTokenPriority {
    Unknown = 0,
    Generic = 10, // Generic tokens (both get same priority, uses token0/token1 ordering)
    MajorToken = 50, // Major tokens (from major_tokens array in chains table)
    WrappedNative = 80, // Base token when paired with stablecoin
    Stablecoin = 100, // Quote token (highest priority) - from stablecoins array in chains table
}

/// Liquidity pool metadata and current state (PostgreSQL)
///
/// Primary Key: (chain_id, address)
/// Query Pattern: "Get pool info for address X on chain Y"
#[derive(Debug, Clone, serde::Serialize)]
pub struct Pool {
    // Primary key
    pub chain_id: u64,
    pub address: String,

    // Token pair metadata (denormalized)
    pub token0: String,
    pub token1: String,
    pub token0_symbol: String,
    pub token1_symbol: String,
    pub token0_decimals: u8,
    pub token1_decimals: u8,

    // Price routing (quote token detection)
    pub base_token: String,
    pub quote_token: String,
    pub is_inverted: bool,
    pub quote_token_priority: i32,

    // Protocol metadata
    pub protocol: Option<String>,
    pub protocol_version: Option<String>,
    pub factory: Option<String>,
    /// Current fee (may change for V4 dynamic fee pools)
    pub fee: Option<u32>,
    /// Initial fee at pool creation (immutable, used for V4 pool ID calculation)
    /// For V2/V3: same as fee. For V4: the PoolKey fee from Initialize event.
    pub initial_fee: Option<u32>,
    pub hook_address: Option<String>,
    pub created_at: Option<DateTime<Utc>>,

    // Last update reference
    pub block_number: Option<u64>,
    pub tx_hash: Option<String>,

    // V2 state: reserves
    pub reserve0: Option<String>,
    pub reserve1: Option<String>,
    pub reserve0_adjusted: Option<f64>,
    pub reserve1_adjusted: Option<f64>,

    // V3/V4 state: concentrated liquidity
    pub sqrt_price_x96: Option<String>,
    pub tick: Option<i32>,
    pub tick_spacing: Option<i32>,
    pub liquidity: Option<String>,

    // Computed prices (Uniswap style)
    // price = always token1/token0 (raw Uniswap convention)
    // token0_price = token0 per token1 (1/price)
    // token1_price = token1 per token0 (= price)
    pub price: Option<f64>,
    pub token0_price: Option<f64>,
    pub token1_price: Option<f64>,
    pub price_usd: Option<f64>,

    // Rolling window stats (24h / 7d)
    pub price_change_24h: Option<f64>,
    pub price_change_7d: Option<f64>,
    pub volume_24h: Option<f64>,
    pub swaps_24h: Option<u64>,

    // Lifetime stats
    pub total_swaps: Option<u64>,
    pub total_volume_usd: Option<f64>,

    // TVL
    pub tvl_usd: Option<f64>,

    // Activity tracking
    pub last_swap_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl Pool {
    pub fn from_v2_pool_created(
        chain_id: u64,
        factory: String,
        event: v2::PairCreated,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        timestamp: u64,
        chain_tokens: &ChainTokens,
    ) -> Self {
        let (base_token, quote_token, is_inverted, quote_token_priority) =
            detect_quote_token(&event.token0, &event.token1, chain_tokens);

        let pool_address = hex_encode(event.pair.as_slice());

        Self {
            chain_id,
            address: pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            token0_symbol: token0.symbol.clone(),
            token1_symbol: token1.symbol.clone(),
            token0_decimals: token0.decimals,
            token1_decimals: token1.decimals,
            fee: Some(3000), // V2 standard fee: 0.3% = 3000 basis points
            initial_fee: Some(3000),
            protocol: None,
            protocol_version: Some(String::from("v2")),
            factory: Some(factory),
            hook_address: None,
            created_at: DateTime::from_timestamp_secs(timestamp as i64),
            base_token,
            quote_token,
            is_inverted,
            quote_token_priority,
            block_number: Some(block_number),
            tx_hash: Some(tx_hash),
            reserve0: None,
            reserve1: None,
            reserve0_adjusted: None,
            reserve1_adjusted: None,
            liquidity: None,
            sqrt_price_x96: None,
            tick: None,
            tick_spacing: None,
            price: None,
            token0_price: None,
            token1_price: None,
            price_usd: None,
            price_change_24h: None,
            price_change_7d: None,
            volume_24h: None,
            swaps_24h: None,
            total_swaps: None,
            total_volume_usd: None,
            tvl_usd: None,
            last_swap_at: None,
            updated_at: None,
        }
    }

    pub fn from_v3_pool_created(
        chain_id: u64,
        factory: String,
        event: v3::PoolCreated,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        timestamp: u64,
        chain_tokens: &ChainTokens,
    ) -> Self {
        let (base_token, quote_token, is_inverted, quote_token_priority) =
            detect_quote_token(&event.token0, &event.token1, chain_tokens);

        let pool_address = hex_encode(event.pool.as_slice());

        Self {
            chain_id,
            address: pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            token0_symbol: token0.symbol.clone(),
            token1_symbol: token1.symbol.clone(),
            token0_decimals: token0.decimals,
            token1_decimals: token1.decimals,
            fee: Some(event.fee.as_limbs()[0] as u32),
            initial_fee: Some(event.fee.as_limbs()[0] as u32),
            protocol: None,
            protocol_version: Some(String::from("v3")),
            factory: Some(factory),
            hook_address: None,
            created_at: DateTime::from_timestamp_secs(timestamp as i64),
            base_token,
            quote_token,
            is_inverted,
            quote_token_priority,
            block_number: Some(block_number),
            tx_hash: Some(tx_hash),
            reserve0: None,
            reserve1: None,
            reserve0_adjusted: Some(0.0), // Initialize with 0 for V3 balance tracking
            reserve1_adjusted: Some(0.0), // Initialize with 0 for V3 balance tracking
            liquidity: Some(String::from("0")), // Initialize with 0 liquidity for V3
            sqrt_price_x96: None,
            tick: None,
            tick_spacing: Some(event.tickSpacing.as_i32()),
            price: None,
            token0_price: None,
            token1_price: None,
            price_usd: None,
            price_change_24h: None,
            price_change_7d: None,
            volume_24h: None,
            swaps_24h: None,
            total_swaps: None,
            total_volume_usd: None,
            tvl_usd: None,
            last_swap_at: None,
            updated_at: None,
        }
    }

    pub fn from_v4_pool_created(
        chain_id: u64,
        factory: String,
        event: v4::Initialize,
        token0: &Token,
        token1: &Token,
        block_number: u64,
        tx_hash: String,
        timestamp: u64,
        chain_tokens: &ChainTokens,
    ) -> Self {
        // CRITICAL: For V4 pools, currencies can be zero address (native token).
        // We must use the normalized token addresses (from token0/token1) for priority detection,
        // NOT the raw event.currency0/currency1 addresses.
        // Otherwise, zero address won't match wrapped_native_token and gets wrong priority.
        let token0_addr: Address = token0.address.parse().unwrap_or_default();
        let token1_addr: Address = token1.address.parse().unwrap_or_default();
        let (base_token, quote_token, is_inverted, quote_token_priority) =
            detect_quote_token(&token0_addr, &token1_addr, chain_tokens);

        let pool_address = hex_encode(event.id.as_slice());

        // Calculate initial price from sqrtPriceX96
        // This is critical for correct TVL and token price derivation at pool creation
        // Use string-based conversion for full precision/range.
        let sqrt_price_x96 = U256::from(event.sqrtPriceX96);
        let sqrt_price_str = sqrt_price_x96.to_string();
        let (price, token0_price, token1_price) = if let Some(adjusted_price) =
            sqrt_price_x96_str_to_adjusted_price(
                &sqrt_price_str,
                token0.decimals as u8,
                token1.decimals as u8,
            ) {
            // price = token1/token0 (Uniswap convention)
            // token1_price = token1 per token0 (= price)
            // token0_price = token0 per token1 (= 1/price)
            // Validate inverse price as well
            let inverse = validate_price_ratio(1.0 / adjusted_price);
            (Some(adjusted_price), inverse, Some(adjusted_price))
        } else {
            (None, None, None)
        };

        Self {
            chain_id,
            address: pool_address,
            token0: token0.address.clone(),
            token1: token1.address.clone(),
            token0_symbol: token0.symbol.clone(),
            token1_symbol: token1.symbol.clone(),
            token0_decimals: token0.decimals,
            token1_decimals: token1.decimals,
            fee: Some(event.fee.as_limbs()[0] as u32),
            initial_fee: Some(event.fee.as_limbs()[0] as u32),
            protocol: None,
            protocol_version: Some(String::from("v4")),
            factory: Some(factory),
            hook_address: Some(hex_encode(event.hooks.as_slice())),
            created_at: DateTime::from_timestamp_secs(timestamp as i64),
            base_token,
            quote_token,
            is_inverted,
            quote_token_priority,
            block_number: Some(block_number),
            tx_hash: Some(tx_hash),
            reserve0: None,
            reserve1: None,
            reserve0_adjusted: Some(0.0), // Initialize with 0 for V4 balance tracking
            reserve1_adjusted: Some(0.0), // Initialize with 0 for V4 balance tracking
            liquidity: Some(String::from("0")), // Initialize with 0 liquidity
            sqrt_price_x96: Some(sqrt_price_str),
            tick_spacing: Some(event.tickSpacing.as_i32()),
            tick: Some(event.tick.as_i32()),
            price,
            token0_price,
            token1_price,
            price_usd: None,
            price_change_24h: None,
            price_change_7d: None,
            volume_24h: None,
            swaps_24h: None,
            total_swaps: None,
            total_volume_usd: None,
            tvl_usd: None,
            last_swap_at: None,
            updated_at: None,
        }
    }

    pub fn update_from_event(&mut self, event: &Event) {
        // Only update if the event block number is greater than or equal to the pool's last block number
        if event.block_number >= self.block_number.unwrap_or(0) {
            self.block_number = Some(event.block_number);
            self.updated_at = DateTime::from_timestamp(event.timestamp.unix_timestamp(), 0);
            self.tx_hash = Some(event.tx_hash.clone());

            // 1. Stats
            if event.event_type == "swap" {
                self.total_swaps = Some(self.total_swaps.unwrap_or(0) + 1);
                // Update last_swap_at timestamp
                self.last_swap_at = self.updated_at;
            }

            // 2. V2 vs V3/V4 Price Update
            // For V2 pools (have reserves), price is calculated from reserves via Sync events only.
            // We do NOT modify reserves from swap events to avoid accumulating errors.
            // For V3/V4 pools (no reserves), we use the event.price from sqrtPriceX96.
            if self.protocol_version.as_deref() != Some("v2") {
                // V3/V4 Pool: event.price is always token1/token0 (from sqrtPriceX96)
                // Uniswap style: store raw price and both directions
                // Validate price is within reasonable bounds before storing
                if let Some(valid_price) = validate_price_ratio(event.price) {
                    self.price = Some(valid_price);
                    self.token1_price = Some(valid_price);
                    // Calculate and validate inverse price
                    if let Some(inverse) = validate_price_ratio(1.0 / valid_price) {
                        self.token0_price = Some(inverse);
                    }
                }

                // V3/V4 Balance Tracking (Reserve accumulation)
                // We track actual token balances by accumulating deltas from events.
                // - Swap: delta = amount * -direction
                // - Mint: delta = amount (direction is -1) -> amount * -(-1) = amount.
                // - Collect: delta = amount (direction is 1) -> amount * -1 = -amount.
                // - Burn: IGNORE (double counting with Collect)

                let update_reserves = match event.event_type.as_str() {
                    "swap" | "mint" | "collect" | "modify_liquidity" => true,
                    "burn" => false, // V3 Burn just updates position, Collect moves tokens.
                    _ => false,
                };

                if update_reserves {
                    let r0 = self.reserve0_adjusted.unwrap_or(0.0);
                    let r1 = self.reserve1_adjusted.unwrap_or(0.0);

                    // direction: 1 = Out (Loss), -1 = In (Gain)
                    // delta = amount * -direction
                    // Example Swap Out: dir=1, delta = -amount
                    // Example Swap In: dir=-1, delta = amount
                    let delta0 = event.amount0_adjusted * (-event.amount0_direction as f64);
                    let delta1 = event.amount1_adjusted * (-event.amount1_direction as f64);

                    self.reserve0_adjusted = Some((r0 + delta0).max(0.0));
                    self.reserve1_adjusted = Some((r1 + delta1).max(0.0));
                }
            }
            // For V2 pools, price is updated via update_v2_reserves from Sync events

            // 4. Update Ticks & SqrtPrice (V3/V4)
            // V3/V4 Swap events emit the tick/sqrtPriceX96 active AFTER the swap.
            // This is the correct "current state" of the pool.
            if event.tick != 0 {
                self.tick = Some(event.tick);
            }
            if event.sqrt_price_x96 != UInt256::from_le_bytes([0u8; 32]) {
                self.sqrt_price_x96 = Some(event.sqrt_price_x96.to_string());
            }

            // 5. Liquidity Handling (V3/V4)
            if event.event_type == "swap" {
                // For Swaps, event.liquidity is the active liquidity available in the constant product formula
                // for the next swap step. It IS the absolute value.
                if event.liquidity != UInt256::from_le_bytes([0u8; 32]) {
                    self.liquidity = Some(event.liquidity.to_string());
                }
            } else if event.event_type == "mint"
                || event.event_type == "burn"
                || event.event_type == "modify_liquidity"
            {
                // For Mint/Burn/ModifyLiquidity, the event gives us the Amount of liquidity added/removed.
                // We must apply this delta to the current pool state.
                // Note: V4 uses "modify_liquidity" event type instead of separate mint/burn
                if event.liquidity != UInt256::from_le_bytes([0u8; 32]) {
                    let delta_str = event.liquidity.to_string();

                    // Get current liquidity, defaulting to "0" if not set
                    let current_liq_str = self.liquidity.as_deref().unwrap_or("0");

                    // For V4 modify_liquidity, direction is encoded in amount0_direction
                    // -1 = adding liquidity (mint), 1 = removing liquidity (burn)
                    // For V3 mint/burn, we use the event_type directly
                    let is_adding = if event.event_type == "modify_liquidity" {
                        event.amount0_direction == -1
                    } else {
                        event.event_type == "mint"
                    };

                    // Use BigInt-based arithmetic to handle full U256 range without overflow
                    let new_liq = if is_adding {
                        bigint_add(current_liq_str, &delta_str)
                    } else {
                        bigint_sub(current_liq_str, &delta_str)
                    };
                    self.liquidity = Some(new_liq);
                }
            }
        }
    }

    /// Update V2 pool reserves from Sync event.
    ///
    /// This sets reserves and calculates price from the reserve ratio.
    pub fn update_v2_sync(&mut self, event: &v2::Sync, block_number: u64, timestamp: u64) {
        if block_number >= self.block_number.unwrap_or(0) {
            self.block_number = Some(block_number);
            self.updated_at = DateTime::from_timestamp(timestamp as i64, 0);

            // Convert raw event data to reserves
            let reserve0: u128 = event.reserve0.to();
            let reserve1: u128 = event.reserve1.to();

            self.reserve0 = Some(reserve0.to_string());
            self.reserve1 = Some(reserve1.to_string());

            // Use BigDecimal-based conversion to avoid precision loss for large reserves
            let r0_adjusted = reserve_to_f64(reserve0, self.token0_decimals);
            let r1_adjusted = reserve_to_f64(reserve1, self.token1_decimals);

            self.reserve0_adjusted = Some(r0_adjusted);
            self.reserve1_adjusted = Some(r1_adjusted);

            // Calculate prices from reserves (Uniswap style)
            // price = always token1/token0
            // token0_price = token0 per token1 (1/price)
            // token1_price = token1 per token0 (= price)
            // Validate prices are within reasonable bounds
            if r0_adjusted > 0.0 && r1_adjusted > 0.0 {
                if let Some(price) = validate_price_ratio(r1_adjusted / r0_adjusted) {
                    self.price = Some(price);
                    self.token1_price = Some(price);
                    if let Some(inverse) = validate_price_ratio(r0_adjusted / r1_adjusted) {
                        self.token0_price = Some(inverse);
                    }
                }
            }
        }
    }

    /// Update V3 pool state from Initialize event.
    ///
    /// This sets the initial sqrtPriceX96 and tick, which are needed for:
    /// 1. Calculating initial token prices
    /// 2. Computing TVL from liquidity
    pub fn update_v3_initialize(
        &mut self,
        event: &v3::Initialize,
        block_number: u64,
        timestamp: u64,
    ) {
        if block_number >= self.block_number.unwrap_or(0) {
            self.block_number = Some(block_number);
            self.updated_at = DateTime::from_timestamp(timestamp as i64, 0);

            // Convert raw event data to pool state
            let sqrt_price_x96 = U256::from(event.sqrtPriceX96);
            let tick: i32 = event.tick.try_into().unwrap_or(0);

            let sqrt_price_str = sqrt_price_x96.to_string();
            self.sqrt_price_x96 = Some(sqrt_price_str.clone());
            self.tick = Some(tick);

            // Calculate price from sqrtPriceX96
            // Use string-based conversion for full precision/range.
            if let Some(adjusted_price) = sqrt_price_x96_str_to_adjusted_price(
                &sqrt_price_str,
                self.token0_decimals as u8,
                self.token1_decimals as u8,
            ) {
                // price = token1/token0 (Uniswap convention)
                // token1_price = token1 per token0 (= price)
                // token0_price = token0 per token1 (= 1/price)
                self.price = Some(adjusted_price);
                self.token1_price = Some(adjusted_price);
                // Validate inverse price as well
                if let Some(inverse) = validate_price_ratio(1.0 / adjusted_price) {
                    self.token0_price = Some(inverse);
                }
            }
        }
    }

    /// Update V4 pool fee from swap event (dynamic fees).
    pub fn update_v4_fee(&mut self, event: &v4::Swap) {
        self.fee = Some(event.fee.as_limbs()[0] as u32);
    }
}

fn detect_quote_token(
    token0: &Address,
    token1: &Address,
    chain_tokens: &ChainTokens,
) -> (String, String, bool, i32) {
    let priority0 = get_token_priority(token0, chain_tokens);
    let priority1 = get_token_priority(token1, chain_tokens);

    // Higher priority becomes quote token
    if priority0 > priority1 {
        (
            hex_encode(token1.as_slice()),
            hex_encode(token0.as_slice()),
            true,
            priority0,
        )
    } else {
        // When priorities are equal (e.g., both Generic tokens), defaults to token0=base, token1=quote
        // This matches Uniswap's convention: price = token1/token0, so token0 is base, token1 is quote
        // Uniswap determines token0/token1 by address ordering (lower address = token0)
        (
            hex_encode(token0.as_slice()),
            hex_encode(token1.as_slice()),
            false,
            priority1,
        )
    }
}

fn get_token_priority(token: &Address, chain_tokens: &ChainTokens) -> i32 {
    let token_lower = hex_encode(token.as_slice());

    // Tier 1: Stable token (highest priority - becomes quote token)
    if chain_tokens.is_stable(&token_lower) {
        return QuoteTokenPriority::Stablecoin as i32;
    }

    // Tier 2: Wrapped native token (becomes base token when paired with stablecoin)
    if chain_tokens.is_wrapped_native(&token_lower) {
        return QuoteTokenPriority::WrappedNative as i32;
    }

    // Tier 3: Major token (from major_tokens array in chains table)
    if chain_tokens.is_major_token(&token_lower) {
        return QuoteTokenPriority::MajorToken as i32;
    }

    // Tier 4: Generic token
    QuoteTokenPriority::Generic as i32
}
