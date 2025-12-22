//! Price validation constants and helper functions.
//!
//! These bounds are designed to catch calculation errors while allowing
//! legitimate extreme values. The key insight is:
//!
//! 1. TOKEN PRICE: Even the most expensive token (BTC at peak ~$70k) costs far less
//!    than $1M. A token costing $100M+ is almost certainly a calculation error.
//!
//! 2. PRICE RATIO: The ratio between two tokens can be extreme (e.g., BTC/SHIB ≈ 7e9)
//!    but ratios beyond 1e12 usually indicate decimal/conversion errors.
//!
//! 3. VOLUME: The largest single DeFi swap ever was in the hundreds of millions.
//!    A swap showing $1B+ is suspicious and likely a calculation error.
//!
//! 4. NATIVE MULTIPLIER: A token shouldn't be worth more than 100,000x the native token.
//!    Even BTC/ETH ratio is only ~20x. Anything higher suggests an inversion error.

// ============================================
// Price Validation Constants
// ============================================

/// Maximum reasonable price ratio between two tokens (token1/token0).
/// 1e12 allows for extreme pairs like BTC/SHIB while catching decimal errors.
pub const MAX_PRICE_RATIO: f64 = 1e12;

/// Minimum reasonable price ratio. Inverse of MAX_PRICE_RATIO.
pub const MIN_PRICE_RATIO: f64 = 1e-12;

/// Maximum reasonable token price in USD.
/// No legitimate token costs more than $1 million per unit.
/// Even wrapped BTC at 10x current ATH would be ~$700k.
pub const MAX_TOKEN_USD_PRICE: f64 = 1e6;

/// Maximum reasonable volume in USD for a single swap.
/// $1 billion is far beyond typical DeFi swaps.
pub const MAX_VOLUME_USD: f64 = 1e9;

/// Maximum reasonable TVL in USD for a single pool.
/// $100 billion covers the largest DeFi pools with margin.
pub const MAX_TVL_USD: f64 = 1e11;

/// Maximum multiplier of native token price for any derived token.
/// A token shouldn't be worth more than 100,000x ETH/native.
/// This catches inversion errors where rate is used instead of 1/rate.
pub const MAX_NATIVE_MULTIPLIER: f64 = 1e5;

// ============================================
// Price Validation Helpers
// ============================================

/// Validate a price ratio (token1/token0) is within reasonable bounds.
/// Returns Some(price) if valid, None if invalid.
#[inline]
pub fn validate_price_ratio(price: f64) -> Option<f64> {
    if price > 0.0 && price.is_finite() && price >= MIN_PRICE_RATIO && price <= MAX_PRICE_RATIO {
        Some(price)
    } else {
        None
    }
}

/// Validate a USD price is within reasonable bounds.
/// Returns the price if valid, 0.0 if invalid.
#[inline]
pub fn validate_usd_price(price: f64) -> f64 {
    if price > 0.0 && price.is_finite() && price <= MAX_TOKEN_USD_PRICE {
        price
    } else {
        0.0
    }
}

/// Validate a USD volume is within reasonable bounds.
/// Returns the volume if valid, 0.0 if invalid.
#[inline]
pub fn validate_usd_volume(volume: f64) -> f64 {
    if volume >= 0.0 && volume.is_finite() && volume <= MAX_VOLUME_USD {
        volume
    } else {
        0.0
    }
}

/// Validate a USD TVL is within reasonable bounds.
/// Returns the TVL if valid, 0.0 if invalid.
#[inline]
pub fn validate_usd_tvl(tvl: f64) -> f64 {
    if tvl >= 0.0 && tvl.is_finite() && tvl <= MAX_TVL_USD {
        tvl
    } else {
        0.0
    }
}

/// Validate a token's USD price relative to the native token price.
///
/// This catches inversion errors where a token is calculated to be worth
/// millions of dollars due to using the wrong direction of exchange rate.
///
/// # Arguments
/// * `token_usd` - The calculated USD price of the token
/// * `native_price_usd` - The current USD price of the native token (e.g., ETH)
///
/// # Returns
/// * The price if valid, 0.0 if it exceeds reasonable bounds relative to native
#[inline]
pub fn validate_usd_price_relative(token_usd: f64, native_price_usd: f64) -> f64 {
    if token_usd <= 0.0 || !token_usd.is_finite() {
        return 0.0;
    }

    // First check absolute bounds
    if token_usd > MAX_TOKEN_USD_PRICE {
        return 0.0;
    }

    // Then check relative to native price
    // No token should be worth more than MAX_NATIVE_MULTIPLIER times the native token
    if native_price_usd > 0.0 {
        let multiplier = token_usd / native_price_usd;
        if multiplier > MAX_NATIVE_MULTIPLIER {
            return 0.0;
        }
    }

    token_usd
}

/// Cross-validate calculated price against implied price from swap amounts.
///
/// If we calculated a token price, we can verify it makes sense by checking:
/// volume ≈ amount * calculated_price
///
/// # Arguments
/// * `calculated_price` - The USD price we calculated for the base token
/// * `amount` - The decimal-adjusted amount of the base token in the swap
/// * `expected_volume_usd` - What we expect the volume to be (from quote side)
/// * `tolerance` - How much divergence to allow (e.g., 0.5 = 50%)
///
/// # Returns
/// * true if the calculated price is plausible, false if it seems wrong
#[inline]
pub fn validate_price_against_volume(
    calculated_price: f64,
    amount: f64,
    expected_volume_usd: f64,
    tolerance: f64,
) -> bool {
    if calculated_price <= 0.0 || amount <= 0.0 || expected_volume_usd <= 0.0 {
        return true; // Can't validate, assume OK
    }

    let implied_volume = amount * calculated_price;
    let ratio = implied_volume / expected_volume_usd;

    // Check if ratio is within tolerance of 1.0
    ratio > (1.0 - tolerance) && ratio < (1.0 + tolerance)
}

// ============================================
// Manipulation Detection
// ============================================

/// Minimum USD value of native token liquidity for price trust.
/// Pools with less than this amount of native token liquidity will be flagged.
/// Works across all chains regardless of native token price (WETH, Monad tokens, etc.)
pub const MIN_NATIVE_LIQUIDITY_USD: f64 = 5_000.0;

/// Maximum volume-to-TVL ratio before flagging as suspicious.
/// A swap > 50% of pool TVL is suspicious (potential manipulation).
pub const MAX_VOLUME_TO_TVL_RATIO: f64 = 0.5;

/// Check if pool has sufficient native token liquidity (USD-denominated).
/// Returns true if the pool's native token side has >= MIN_NATIVE_LIQUIDITY_USD.
#[inline]
pub fn has_sufficient_native_liquidity(native_amount: f64, native_price_usd: f64) -> bool {
    if native_amount <= 0.0 || native_price_usd <= 0.0 {
        return false;
    }
    let native_value_usd = native_amount * native_price_usd;
    native_value_usd >= MIN_NATIVE_LIQUIDITY_USD
}

/// Detect suspicious price deviation relative to TVL.
/// A swap's volume shouldn't exceed MAX_VOLUME_TO_TVL_RATIO of pool TVL.
#[inline]
pub fn is_suspicious_volume_to_tvl(volume_usd: f64, tvl_usd: f64) -> bool {
    if tvl_usd <= 0.0 || volume_usd <= 0.0 {
        return false;
    }
    let ratio = volume_usd / tvl_usd;
    ratio > MAX_VOLUME_TO_TVL_RATIO
}
