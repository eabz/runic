use chrono::{DateTime, Utc};

use super::Pool;

/// Native token price per chain (PostgreSQL)
///
/// Stores the current USD price of the native token (ETH, MATIC, etc.)
/// for each chain. Used for calculating USD values of gas and native transfers.
///
/// Price is derived from the stable pool (native/stablecoin pair) using:
/// - V2: Reserve-based pricing (reserve_stable / reserve_native)
/// - V3/V4: sqrtPriceX96-based pricing
#[derive(Debug, Clone)]
pub struct NativeTokenPrice {
    pub chain_id: i64,
    pub price_usd: f64,
    pub updated_at: DateTime<Utc>,
}

impl NativeTokenPrice {
    pub fn new(chain_id: i64, price_usd: f64) -> Self {
        Self {
            chain_id,
            price_usd,
            updated_at: Utc::now(),
        }
    }

    /// Update the native token price from the stable pool state.
    ///
    /// CRITICAL: This function now takes the wrapped native token address to verify
    /// the stable pool actually contains the native token. This prevents misconfigured
    /// pools from producing incorrect prices.
    ///
    /// ## Algorithm:
    /// 1. Verify the pool contains the wrapped native token (as either token0 or token1)
    /// 2. Determine which token in the pool is the native token
    /// 3. Calculate: native_price_usd = stablecoin_amount / native_amount
    ///
    /// ## Price Convention:
    /// - pool.price = token1/token0 (always, Uniswap convention)
    /// - pool.token0_price = token0 per token1 = 1/price
    /// - pool.token1_price = token1 per token0 = price
    ///
    /// ## Native Price Calculation:
    /// - If native is token0: we need token1 (stable) per token0 (native) = token1_price = price
    /// - If native is token1: we need token0 (stable) per token1 (native) = token0_price = 1/price
    ///
    /// Returns true if the price was updated, false if pool doesn't contain native or no valid price.
    pub fn update_from_pool(&mut self, pool: &Pool, wrapped_native_address: &str) -> bool {
        // Get pool price - return false if no price
        let Some(price) = pool.price else {
            return false;
        };

        // Basic validity check only
        if price <= 0.0 || !price.is_finite() {
            return false;
        }

        let native_lower = wrapped_native_address.to_lowercase();
        let token0_lower = pool.token0.to_lowercase();
        let token1_lower = pool.token1.to_lowercase();

        // Determine which token is the native token
        let native_is_token0 = token0_lower == native_lower;
        let native_is_token1 = token1_lower == native_lower;

        // Verify the pool contains the native token
        if !native_is_token0 && !native_is_token1 {
            return false;
        }

        // Calculate native token USD price based on which token is native
        // pool.price = token1/token0 (Uniswap convention)
        let native_price_usd = if native_is_token0 {
            // Native is token0, stablecoin is token1
            // price = stablecoin/native = USD per native
            price
        } else {
            // Native is token1, stablecoin is token0
            // price = native/stablecoin, so we need 1/price
            if price < 1e-15 {
                // Avoid division by very small numbers
                return false;
            }
            1.0 / price
        };

        // Validate price is reasonable for native tokens
        // Native tokens can range from very cheap to expensive
        const MIN_NATIVE_PRICE: f64 = 0.0001; // $0.0001 - supports cheap native tokens
        const MAX_NATIVE_PRICE: f64 = 1e6; // $1 million - no native token should exceed this

        if !native_price_usd.is_finite()
            || native_price_usd < MIN_NATIVE_PRICE
            || native_price_usd > MAX_NATIVE_PRICE
        {
            return false;
        }

        self.price_usd = native_price_usd;
        self.updated_at = Utc::now();
        true
    }

    /// Check if this event type can impact the native token price.
    ///
    /// Events that affect price:
    /// - swap: Changes reserves (V2) or sqrtPriceX96 (V3/V4)
    /// - sync: Updates V2 reserves directly
    /// - mint/burn: Changes liquidity which affects reserves
    pub fn is_price_impacting_event(event_type: &str) -> bool {
        matches!(
            event_type,
            "swap" | "sync" | "mint" | "burn" | "modify_liquidity"
        )
    }
}
