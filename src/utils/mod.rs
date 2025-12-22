//! Utility functions for the Runic indexer.
//!
//! This module is organized into focused submodules:
//!
//! - [`validation`] - Price validation constants and helper functions
//! - [`conversion`] - Type conversions (U256, f64, BigInt, hex encoding)
//! - [`tick_math`] - Uniswap V3/V4 tick calculations
//! - [`price`] - Price conversion utilities (sqrtPriceX96, reserves)
//! - [`pool_id`] - Uniswap V4 pool ID computation

mod conversion;
mod pool_id;
mod price;
mod tick_math;
mod validation;

// ============================================
// Common Constants
// ============================================

/// The Ethereum zero address (0x0000000000000000000000000000000000000000)
/// Used for mint/burn transfers and native token handling.
pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

// ============================================
// Re-exports
// ============================================

// Conversion utilities
pub use conversion::{
    bigint_add, bigint_sub, hex_encode, into_u256, reserve_to_f64, str_to_f64_with_decimals,
    u256_to_f64,
};

// Pool ID utilities (V4)
pub use pool_id::{compute_v4_pool_id, compute_v4_pool_id_from_stored};

// Price conversion utilities
pub use price::{calculate_reserves_from_liquidity_subgraph, sqrt_price_x96_str_to_adjusted_price};

// Tick math utilities
pub use tick_math::calculate_mint_amounts;

// Validation utilities
pub use validation::{
    has_sufficient_native_liquidity, is_suspicious_volume_to_tvl, validate_price_against_volume,
    validate_price_ratio, validate_usd_price, validate_usd_price_relative, validate_usd_tvl,
    validate_usd_volume, MAX_PRICE_RATIO, MIN_NATIVE_LIQUIDITY_USD,
};
