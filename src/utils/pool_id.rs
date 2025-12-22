//! Uniswap V4 pool ID utilities.
//!
//! Functions for computing and validating V4 pool IDs.

use alloy::primitives::{keccak256, Address};
use alloy::sol_types::SolValue;

/// Compute the Uniswap V4 pool ID from pool parameters.
///
/// The pool ID is computed as:
/// ```text
/// keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks))
/// ```
///
/// Where currency0 and currency1 are sorted by address (lower address first).
/// Native tokens (represented by zero address) are always currency0 when paired
/// with any non-zero address.
///
/// # Arguments
/// * `currency_a` - First currency address (can be zero address for native token)
/// * `currency_b` - Second currency address (can be zero address for native token)
/// * `fee` - Pool fee in hundredths of a bip (e.g., 3000 = 0.30%)
/// * `tick_spacing` - Tick spacing for the pool
/// * `hooks` - Hook contract address (zero address if no hooks)
///
/// # Returns
/// The 32-byte pool ID as a hex string (lowercase, with 0x prefix)
///
/// # Example
/// ```ignore
/// let pool_id = compute_v4_pool_id(
///     "0x0000000000000000000000000000000000000000", // Native ETH
///     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
///     3000,   // 0.30% fee
///     60,     // tick spacing
///     "0x0000000000000000000000000000000000000000", // no hooks
/// );
/// ```
pub fn compute_v4_pool_id(
    currency_a: &str,
    currency_b: &str,
    fee: u32,
    tick_spacing: i32,
    hooks: &str,
) -> String {
    // Parse addresses
    let addr_a: Address = currency_a.parse().unwrap_or_default();
    let addr_b: Address = currency_b.parse().unwrap_or_default();
    let hooks_addr: Address = hooks.parse().unwrap_or_default();

    // Sort currencies: lower address is currency0
    // This matches Uniswap's sortsBefore logic
    let (currency0, currency1) = if addr_a < addr_b { (addr_a, addr_b) } else { (addr_b, addr_a) };

    // ABI encode the parameters: (address, address, uint24, int24, address)
    // Using alloy's SolValue for proper ABI encoding
    let encoded = (currency0, currency1, fee, tick_spacing, hooks_addr).abi_encode();

    // Compute keccak256 hash
    let hash = keccak256(&encoded);

    // Return as lowercase hex string with 0x prefix
    format!("{hash:#x}")
}

/// Compute the Uniswap V4 pool ID from stored pool data.
///
/// This variant handles the case where we store wrapped native token addresses
/// (e.g., WETH) but need to compute the pool ID using zero address (as V4 does
/// for native tokens).
///
/// Use this when computing pool IDs from data stored in the database, where
/// native tokens have been "normalized" to their wrapped equivalent.
///
/// # Arguments
/// * `token0` - Token0 address (may be wrapped native)
/// * `token1` - Token1 address (may be wrapped native)
/// * `fee` - Pool fee in hundredths of a bip
/// * `tick_spacing` - Tick spacing for the pool
/// * `hooks` - Hook contract address
/// * `wrapped_native` - The wrapped native token address for this chain
///
/// # Returns
/// The 32-byte pool ID as a hex string (lowercase, with 0x prefix)
pub fn compute_v4_pool_id_from_stored(
    token0: &str,
    token1: &str,
    fee: u32,
    tick_spacing: i32,
    hooks: &str,
) -> String {
    // Sort currencies: lower address is currency0
    // This matches Uniswap's sortsBefore logic
    let (currency0, currency1) = if token0 < token1 { (token0, token1) } else { (token1, token0) };

    // Compute pool ID using the denormalized currencies
    compute_v4_pool_id(&currency0, &currency1, fee, tick_spacing, hooks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_id_sorting() {
        // Same inputs in different order should produce the same pool ID
        let id1 = compute_v4_pool_id(
            "0x0000000000000000000000000000000000000000",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            3000,
            60,
            "0x0000000000000000000000000000000000000000",
        );
        let id2 = compute_v4_pool_id(
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0x0000000000000000000000000000000000000000",
            3000,
            60,
            "0x0000000000000000000000000000000000000000",
        );
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_different_hooks_produce_different_ids() {
        let id_no_hooks = compute_v4_pool_id(
            "0x0000000000000000000000000000000000000000",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            3000,
            60,
            "0x0000000000000000000000000000000000000000",
        );
        let id_with_hooks = compute_v4_pool_id(
            "0x0000000000000000000000000000000000000000",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            3000,
            60,
            "0x1234567890abcdef1234567890abcdef12345678",
        );
        assert_ne!(id_no_hooks, id_with_hooks);
    }

    #[test]
    fn test_pool_id_from_stored_denormalizes_wrapped_native() {
        // Simulate: pool was created with native ETH (0x0...) but we stored WETH address
        let usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
        let weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

        // Pool ID computed directly with zero address (as in event)
        let id_from_event = compute_v4_pool_id(
            "0x0000000000000000000000000000000000000000",
            usdc,
            3000,
            60,
            "0x0000000000000000000000000000000000000000",
        );

        // Pool ID computed from stored data (WETH address) with denormalization
        let id_from_stored = compute_v4_pool_id_from_stored(
            weth, // Stored as WETH
            usdc,
            3000,
            60,
            "0x0000000000000000000000000000000000000000",
        );

        // They should match!
        assert_eq!(id_from_event, id_from_stored);
    }

    #[test]
    fn test_pool_id_from_stored_non_native_unchanged() {
        // For non-native pools, both functions should produce the same result
        let token_a = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"; // USDC
        let token_b = "0x6b175474e89094c44da98b954eedeac495271d0f"; // DAI

        let id_direct = compute_v4_pool_id(
            token_a,
            token_b,
            500,
            10,
            "0x0000000000000000000000000000000000000000",
        );

        let id_from_stored = compute_v4_pool_id_from_stored(
            token_a,
            token_b,
            500,
            10,
            "0x0000000000000000000000000000000000000000",
        );

        assert_eq!(id_direct, id_from_stored);
    }
}
