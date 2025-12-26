//! Type conversion and formatting utilities.
//!
//! Functions for converting between different numeric types (U256, f64, BigInt, etc.)
//! with proper decimal handling and precision preservation.

use alloy::primitives::{hex, U256};
use bigdecimal::BigDecimal;
use clickhouse::types::UInt256;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use once_cell::sync::Lazy;
use std::str::FromStr;

// ============================================
// Hex Encoding
// ============================================

/// Encode bytes as a lowercase hex string with 0x prefix.
pub fn hex_encode(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

// ============================================
// U256 Conversions
// ============================================

/// Convert alloy U256 to clickhouse UInt256.
pub fn into_u256(v: alloy::primitives::U256) -> UInt256 {
    UInt256::from_le_bytes(v.to_le_bytes())
}

/// Convert U256 to f64 with decimal adjustment using BigDecimal for precision.
///
/// This function uses BigDecimal for precise conversion of large U256 values,
/// avoiding precision loss that occurs when directly converting to f64.
///
/// # Arguments
/// * `value` - The U256 value to convert
/// * `decimals` - The number of decimal places to adjust by
///
/// # Returns
/// * The adjusted f64 value, or 0.0 if conversion fails
///
/// # Example
/// ```ignore
/// let value = U256::from(1_000_000_000_000_000_000u128); // 1e18
/// let adjusted = u256_to_f64(value, 18); // Returns 1.0
/// ```
pub fn u256_to_f64(value: U256, decimals: u8) -> f64 {
    u256_to_f64_safe(value, decimals).unwrap_or(0.0)
}

/// Convert U256 to f64 with decimal adjustment, returning Option for error handling.
///
/// Uses BigDecimal for precise conversion of large U256 values.
/// Returns None if the value cannot be converted to a valid f64.
///
/// # Arguments
/// * `value` - The U256 value to convert
/// * `decimals` - The number of decimal places to adjust by
///
/// # Returns
/// * `Some(f64)` if conversion succeeds, `None` if it fails
pub fn u256_to_f64_safe(value: U256, decimals: u8) -> Option<f64> {
    // Convert U256 to BigDecimal via bytes (faster than string parsing)
    let bytes: [u8; 32] = value.to_le_bytes();
    let big_int = BigInt::from_bytes_le(num_bigint::Sign::Plus, &bytes);
    let big_value = BigDecimal::from(big_int);

    // Apply decimal adjustment
    let adjusted = big_value / big_pow10(decimals);

    // Convert to f64
    let result = adjusted.to_f64()?;

    // Validate result is finite
    if result.is_finite() {
        Some(result)
    } else {
        None
    }
}

// ============================================
// String to f64 Conversions
// ============================================

/// Parse a string representation of a large number to f64 with decimal adjustment.
///
/// Uses BigDecimal for precise conversion. This is useful for parsing
/// liquidity and sqrtPriceX96 values stored as strings.
///
/// # Arguments
/// * `value_str` - The string representation of the number
/// * `decimals` - The number of decimal places to adjust by
///
/// # Returns
/// * `Some(f64)` if parsing succeeds and value is valid, `None` otherwise
pub fn str_to_f64_with_decimals(value_str: &str, decimals: u8) -> Option<f64> {
    let big_value = BigDecimal::from_str(value_str).ok()?;

    let adjusted = big_value / big_pow10(decimals);

    let result = adjusted.to_f64()?;

    if result.is_finite() && result >= 0.0 {
        Some(result)
    } else {
        None
    }
}

/// Convert a u128 reserve value to f64 with decimal adjustment using BigDecimal.
///
/// This avoids precision loss that occurs when directly casting u128 to f64
/// for values larger than 2^53.
///
/// # Arguments
/// * `reserve` - The raw reserve value as u128
/// * `decimals` - The number of decimal places to adjust by
///
/// # Returns
/// * The adjusted f64 value, or 0.0 if conversion fails
pub fn reserve_to_f64(reserve: u128, decimals: u8) -> f64 {
    let big_value = BigDecimal::from(reserve);

    let adjusted = if decimals == 0 { big_value } else { big_value / big_pow10(decimals) };

    adjusted.to_f64().unwrap_or(0.0)
}

// ============================================
// BigInt Arithmetic
// ============================================

/// Add two large numbers represented as strings, returning the result as a string.
///
/// Uses BigInt for precise addition without overflow.
/// Returns the input value unchanged if parsing fails.
///
/// # Arguments
/// * `current` - The current value as a string
/// * `delta` - The amount to add
///
/// # Returns
/// * The sum as a string
pub fn bigint_add(current: &str, delta: &str) -> String {
    let current_big = match BigInt::from_str(current) {
        Ok(v) => v,
        Err(_) => return current.to_string(),
    };
    let delta_big = match BigInt::from_str(delta) {
        Ok(v) => v,
        Err(_) => return current.to_string(),
    };

    let result = current_big + delta_big;
    // Ensure non-negative
    if result < BigInt::from(0) {
        "0".to_string()
    } else {
        result.to_string()
    }
}

/// Subtract a delta from a large number represented as strings, returning the result as a string.
///
/// Uses BigInt for precise subtraction without underflow (saturates at 0).
///
/// # Arguments
/// * `current` - The current value as a string
/// * `delta` - The amount to subtract
///
/// # Returns
/// * The difference as a string, saturating at "0" if result would be negative
pub fn bigint_sub(current: &str, delta: &str) -> String {
    let current_big = match BigInt::from_str(current) {
        Ok(v) => v,
        Err(_) => return current.to_string(),
    };
    let delta_big = match BigInt::from_str(delta) {
        Ok(v) => v,
        Err(_) => return current.to_string(),
    };

    let result = current_big - delta_big;
    // Saturate at 0 (no negative liquidity)
    if result < BigInt::from(0) {
        "0".to_string()
    } else {
        result.to_string()
    }
}

// ============================================
// Internal Helpers
// ============================================

static POW10_CACHE: Lazy<[BigDecimal; 25]> =
    Lazy::new(|| std::array::from_fn(|i| BigDecimal::from(BigInt::from(10u32).pow(i as u32))));

/// Compute 10^exp as BigDecimal.
pub(crate) fn big_pow10(exp: u8) -> BigDecimal {
    if (exp as usize) < POW10_CACHE.len() {
        POW10_CACHE[exp as usize].clone()
    } else {
        BigDecimal::from(BigInt::from(10u32).pow(exp as u32))
    }
}
