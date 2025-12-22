//! Tick math for Uniswap V3/V4 calculations.
//!
//! Implements tick-to-price conversions and liquidity amount calculations
//! following Uniswap V3's TickMath.sol approach.

// ============================================
// Precomputed Constants
// ============================================

use crate::utils::price::{Q96, Q96_INV};

// Precomputed multipliers for tick_to_sqrt_price_x96
// These are 1/sqrt(1.0001)^(2^i) = (1.0001)^(-0.5 * 2^i) for i = 0..19
// Used for bit-manipulation approach matching Uniswap V3's TickMath.sol
// Computed with high precision: pow(1.0001, -0.5 * 2^i)
const TICK_MULTIPLIERS: [f64; 20] = [
    0.9999500037496876, // 1/sqrt(1.0001)^1      = 1.0001^(-0.5)
    0.9999000099990001, // 1/sqrt(1.0001)^2      = 1.0001^(-1)
    0.9998000299960005, // 1/sqrt(1.0001)^4      = 1.0001^(-2)
    0.9996000999800035, // 1/sqrt(1.0001)^8      = 1.0001^(-4)
    0.9992003598800331, // 1/sqrt(1.0001)^16     = 1.0001^(-8)
    0.9984013591843877, // 1/sqrt(1.0001)^32     = 1.0001^(-16)
    0.9968052740212325, // 1/sqrt(1.0001)^64     = 1.0001^(-32)
    0.9936207543165446, // 1/sqrt(1.0001)^128    = 1.0001^(-64)
    0.9872822034085791, // 1/sqrt(1.0001)^256    = 1.0001^(-128)
    0.9747261491672988, // 1/sqrt(1.0001)^512    = 1.0001^(-256)
    0.9500910658705113, // 1/sqrt(1.0001)^1024   = 1.0001^(-512)
    0.9026730334469643, // 1/sqrt(1.0001)^2048   = 1.0001^(-1024)
    0.8148186053123443, // 1/sqrt(1.0001)^4096   = 1.0001^(-2048)
    0.6639293595631539, // 1/sqrt(1.0001)^8192   = 1.0001^(-4096)
    0.4408021944899397, // 1/sqrt(1.0001)^16384  = 1.0001^(-8192)
    0.1943065746671466, // 1/sqrt(1.0001)^32768  = 1.0001^(-16384)
    0.0377550449588794, // 1/sqrt(1.0001)^65536  = 1.0001^(-32768)
    0.0014254434198470, // 1/sqrt(1.0001)^131072 = 1.0001^(-65536)
    0.0000020318889432, // 1/sqrt(1.0001)^262144 = 1.0001^(-131072)
    0.0000000000041286, // 1/sqrt(1.0001)^524288 = 1.0001^(-262144)
];

// ============================================
// Tick to Price Conversion
// ============================================

/// Convert a tick to sqrt price ratio (Q64.96 format)
///
/// Formula: sqrt(1.0001^tick) * 2^96
/// Valid tick range: -887272 to 887272 (Uniswap V3 limits)
#[inline(always)]
pub fn tick_to_sqrt_price_x96(tick: i32) -> f64 {
    // Clamp tick to valid range
    let clamped_tick = tick.clamp(-887272, 887272);
    let abs_tick = clamped_tick.unsigned_abs() as usize;

    // Start with ratio = 1.0
    let mut ratio = 1.0_f64;

    // Unrolled loop for the first 10 bits (most common case)
    // This covers ticks up to 1023 which is very common
    if abs_tick & 0x1 != 0 {
        ratio *= TICK_MULTIPLIERS[0];
    }
    if abs_tick & 0x2 != 0 {
        ratio *= TICK_MULTIPLIERS[1];
    }
    if abs_tick & 0x4 != 0 {
        ratio *= TICK_MULTIPLIERS[2];
    }
    if abs_tick & 0x8 != 0 {
        ratio *= TICK_MULTIPLIERS[3];
    }
    if abs_tick & 0x10 != 0 {
        ratio *= TICK_MULTIPLIERS[4];
    }
    if abs_tick & 0x20 != 0 {
        ratio *= TICK_MULTIPLIERS[5];
    }
    if abs_tick & 0x40 != 0 {
        ratio *= TICK_MULTIPLIERS[6];
    }
    if abs_tick & 0x80 != 0 {
        ratio *= TICK_MULTIPLIERS[7];
    }
    if abs_tick & 0x100 != 0 {
        ratio *= TICK_MULTIPLIERS[8];
    }
    if abs_tick & 0x200 != 0 {
        ratio *= TICK_MULTIPLIERS[9];
    }
    if abs_tick & 0x400 != 0 {
        ratio *= TICK_MULTIPLIERS[10];
    }
    if abs_tick & 0x800 != 0 {
        ratio *= TICK_MULTIPLIERS[11];
    }
    if abs_tick & 0x1000 != 0 {
        ratio *= TICK_MULTIPLIERS[12];
    }
    if abs_tick & 0x2000 != 0 {
        ratio *= TICK_MULTIPLIERS[13];
    }
    if abs_tick & 0x4000 != 0 {
        ratio *= TICK_MULTIPLIERS[14];
    }
    if abs_tick & 0x8000 != 0 {
        ratio *= TICK_MULTIPLIERS[15];
    }
    if abs_tick & 0x10000 != 0 {
        ratio *= TICK_MULTIPLIERS[16];
    }
    if abs_tick & 0x20000 != 0 {
        ratio *= TICK_MULTIPLIERS[17];
    }
    if abs_tick & 0x40000 != 0 {
        ratio *= TICK_MULTIPLIERS[18];
    }
    if abs_tick & 0x80000 != 0 {
        ratio *= TICK_MULTIPLIERS[19];
    }

    // For positive ticks, take reciprocal
    if clamped_tick > 0 {
        ratio = 1.0 / ratio;
    }

    // Convert to Q64.96 format by multiplying by 2^96
    ratio * Q96
}

// ============================================
// Liquidity Amount Calculations
// ============================================

/// Calculate amount0 from liquidity and tick range (for V3/V4 Mint/Burn event parsing)
/// Uniswap V3 formula: amount0 = L * (1/sqrt_a - 1/sqrt_b)
fn calculate_amount0_from_liquidity(liquidity: f64, tick_lower: i32, tick_upper: i32) -> f64 {
    if !liquidity.is_finite() || !(0.0..=1e40).contains(&liquidity) {
        return 0.0;
    }

    let sqrt_ratio_a_x96 = tick_to_sqrt_price_x96(tick_lower);
    let sqrt_ratio_b_x96 = tick_to_sqrt_price_x96(tick_upper);

    if sqrt_ratio_a_x96 <= 0.0 || sqrt_ratio_b_x96 <= 0.0 {
        return 0.0;
    }

    let (sqrt_ratio_a_x96, sqrt_ratio_b_x96) = if sqrt_ratio_a_x96 > sqrt_ratio_b_x96 {
        (sqrt_ratio_b_x96, sqrt_ratio_a_x96)
    } else {
        (sqrt_ratio_a_x96, sqrt_ratio_b_x96)
    };

    let sqrt_ratio_a = sqrt_ratio_a_x96 * Q96_INV;
    let sqrt_ratio_b = sqrt_ratio_b_x96 * Q96_INV;

    if sqrt_ratio_a < 1e-15 {
        return 0.0;
    }

    let inv_sqrt_a = 1.0 / sqrt_ratio_a;
    let inv_sqrt_b = 1.0 / sqrt_ratio_b;

    if inv_sqrt_a > 1e15 || inv_sqrt_b > 1e15 {
        return 0.0;
    }

    let diff = inv_sqrt_a - inv_sqrt_b;
    if diff <= 0.0 || !diff.is_finite() {
        return 0.0;
    }

    let result = liquidity * diff;
    if !result.is_finite() || !(0.0..=1e35).contains(&result) {
        return 0.0;
    }

    result
}

/// Calculate amount1 from liquidity and tick range (for V3/V4 Mint/Burn event parsing)
/// Uniswap V3 formula: amount1 = L * (sqrtRatioBX96 - sqrtRatioAX96) / Q96
fn calculate_amount1_from_liquidity(liquidity: f64, tick_lower: i32, tick_upper: i32) -> f64 {
    if !liquidity.is_finite() || !(0.0..=1e40).contains(&liquidity) {
        return 0.0;
    }

    let sqrt_ratio_a_x96 = tick_to_sqrt_price_x96(tick_lower);
    let sqrt_ratio_b_x96 = tick_to_sqrt_price_x96(tick_upper);

    if !sqrt_ratio_a_x96.is_finite() || !sqrt_ratio_b_x96.is_finite() {
        return 0.0;
    }

    let (sqrt_ratio_a_x96, sqrt_ratio_b_x96) = if sqrt_ratio_a_x96 > sqrt_ratio_b_x96 {
        (sqrt_ratio_b_x96, sqrt_ratio_a_x96)
    } else {
        (sqrt_ratio_a_x96, sqrt_ratio_b_x96)
    };

    let diff_x96 = sqrt_ratio_b_x96 - sqrt_ratio_a_x96;
    if diff_x96 <= 0.0 || !diff_x96.is_finite() {
        return 0.0;
    }

    let result = liquidity * diff_x96 * Q96_INV;
    if !result.is_finite() || !(0.0..=1e50).contains(&result) {
        return 0.0;
    }

    result
}

/// Calculate token amounts for a liquidity change based on current tick (for V4 event parsing)
/// Returns (amount0, amount1)
///
/// Includes bounds checking to prevent overflow from malformed tick data
pub fn calculate_mint_amounts(
    liquidity: f64,
    current_tick: i32,
    tick_lower: i32,
    tick_upper: i32,
) -> (f64, f64) {
    // Sanity check: tick_lower must be less than tick_upper
    if tick_lower >= tick_upper {
        return (0.0, 0.0);
    }

    // Sanity check: liquidity bounds
    const MAX_LIQUIDITY: f64 = 1e35;
    if !liquidity.is_finite() || liquidity <= 0.0 || liquidity > MAX_LIQUIDITY {
        return (0.0, 0.0);
    }

    // Valid tick range is -887272 to 887272 (Uniswap V3 limits)
    // These are already clamped in tick_to_sqrt_price_x96, but check anyway
    if tick_lower < -887272 || tick_upper > 887272 {
        return (0.0, 0.0);
    }

    if current_tick < tick_lower {
        let a0 = calculate_amount0_from_liquidity(liquidity, tick_lower, tick_upper);
        (a0, 0.0)
    } else if current_tick >= tick_upper {
        let a1 = calculate_amount1_from_liquidity(liquidity, tick_lower, tick_upper);
        (0.0, a1)
    } else {
        let a0 = calculate_amount0_from_liquidity(liquidity, current_tick, tick_upper);
        let a1 = calculate_amount1_from_liquidity(liquidity, tick_lower, current_tick);
        (a0, a1)
    }
}
