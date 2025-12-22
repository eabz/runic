//! Price conversion utilities for Uniswap V3/V4.
//!
//! Functions for converting sqrtPriceX96 values to adjusted prices,
//! and calculating reserves from liquidity.

use bigdecimal::BigDecimal;
use num_bigint::Sign;
use num_traits::ToPrimitive;
use std::str::FromStr;

use super::conversion::big_pow10;
use super::validation::validate_price_ratio;

// ============================================
// Constants
// ============================================

/// Constant: 2^96 (Q64.96 fixed point scaling factor)
/// Value: 79228162514264337593543950336.0
pub(crate) const Q96: f64 = 79228162514264337593543950336.0;

/// 1/Q96 precomputed for faster division (multiply instead of divide)
pub(crate) const Q96_INV: f64 = 1.0 / 79228162514264337593543950336.0;

// ============================================
// sqrtPriceX96 to Price Conversion
// ============================================

/// Convert sqrtPriceX96 string to adjusted price with full precision.
///
/// This variant accepts the sqrtPriceX96 as a string to preserve full precision
/// from the original U256 value. Use this when you have the raw string representation.
///
/// # Arguments
/// * `sqrt_price_x96_str` - The sqrtPriceX96 value as a string
/// * `token0_decimals` - Decimal places of token0
/// * `token1_decimals` - Decimal places of token1
///
/// # Returns
/// * `Some(adjusted_price)` if valid and within reasonable bounds, `None` if invalid
pub fn sqrt_price_x96_str_to_adjusted_price(
    sqrt_price_x96_str: &str,
    token0_decimals: u8,
    token1_decimals: u8,
) -> Option<f64> {
    if token0_decimals > 24 || token1_decimals > 24 {
        return None;
    }

    let sqrt_price = BigDecimal::from_str(sqrt_price_x96_str).ok()?;
    if sqrt_price.sign() == Sign::NoSign {
        return None;
    }

    // Q96 = 2^96 (exact)
    let q96 = BigDecimal::from_str("79228162514264337593543950336").ok()?;

    // raw_price = (sqrtPriceX96 / Q96)^2
    let normalized = &sqrt_price / &q96;
    let raw_price = &normalized * &normalized;

    // decimal adjustment: 10^(decimals0 - decimals1)
    let decimal_diff = token0_decimals as i32 - token1_decimals as i32;
    let adjusted = if decimal_diff >= 0 {
        raw_price * big_pow10(decimal_diff as u32)
    } else {
        raw_price / big_pow10((-decimal_diff) as u32)
    };

    let adjusted_f64 = adjusted.to_f64()?;
    validate_price_ratio(adjusted_f64)
}

// ============================================
// Reserve Calculations
// ============================================

/// Calculate reserves from current in-range liquidity (Uniswap V3 subgraph method)
///
/// This is the simplified approach used by Uniswap V3 subgraph:
/// - Uses ONLY the current in-range liquidity (from swap event)
/// - Does NOT sum all positions across tick ranges
///
/// Formula (from Uniswap V3 whitepaper):
/// - amount0 = L / sqrt(P) where P = price (token1/token0)
/// - amount1 = L * sqrt(P)
///
/// Note: Returns RAW amounts (before decimal adjustment)
///
/// # Arguments
/// * `liquidity` - Current in-range liquidity (from swap event's `liquidity` field)
/// * `sqrt_price_x96` - Current sqrtPriceX96 (from swap event)
///
/// # Returns
/// * `(amount0_raw, amount1_raw)` - Raw token amounts
pub fn calculate_reserves_from_liquidity_subgraph(
    liquidity: f64,
    sqrt_price_x96: f64,
) -> (f64, f64) {
    // Validate inputs with reasonable bounds
    // Max liquidity is u128::MAX ≈ 3.4e38, but realistic pools have much less
    const MAX_LIQUIDITY: f64 = 1e35;
    // sqrtPriceX96 range: MIN_SQRT_RATIO ≈ 4.3e9 to MAX_SQRT_RATIO ≈ 1.46e48
    const MIN_SQRT_PRICE_X96: f64 = 4.0e9;
    const MAX_SQRT_PRICE_X96: f64 = 1.5e48;

    if !liquidity.is_finite() || liquidity <= 0.0 || liquidity > MAX_LIQUIDITY {
        return (0.0, 0.0);
    }
    if !sqrt_price_x96.is_finite()
        || sqrt_price_x96 < MIN_SQRT_PRICE_X96
        || sqrt_price_x96 > MAX_SQRT_PRICE_X96
    {
        return (0.0, 0.0);
    }

    // Normalize sqrtPriceX96 to get sqrt(price)
    // sqrt_price = sqrtPriceX96 / 2^96 (using multiply by inverse for speed)
    let sqrt_price = sqrt_price_x96 * Q96_INV;

    if sqrt_price <= 0.0 || !sqrt_price.is_finite() {
        return (0.0, 0.0);
    }

    // Uniswap V3 subgraph formula:
    // amount0 = liquidity / sqrtPrice (in raw units)
    // amount1 = liquidity * sqrtPrice (in raw units)
    //
    // This represents the virtual reserves at the current price point
    let amount0_raw = liquidity / sqrt_price;
    let amount1_raw = liquidity * sqrt_price;

    // Validate results
    if !amount0_raw.is_finite() || !amount1_raw.is_finite() {
        return (0.0, 0.0);
    }

    // Cap at reasonable maximum (1e35 raw units)
    // For 18-decimal tokens, this is 1e17 tokens which is still very large
    let max_raw = 1e35;
    let amount0_capped = amount0_raw.min(max_raw).max(0.0);
    let amount1_capped = amount1_raw.min(max_raw).max(0.0);

    (amount0_capped, amount1_capped)
}
