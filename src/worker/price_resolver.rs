use rustc_hash::FxHashMap;
use std::sync::Arc;

use crate::{
    db::models::{ChainTokens, Event, Pool},
    utils::{
        calculate_reserves_from_liquidity_subgraph, has_sufficient_native_liquidity,
        is_suspicious_volume_to_tvl, str_to_f64_with_decimals, validate_price_against_volume,
        validate_price_ratio, validate_usd_price, validate_usd_price_relative, validate_usd_tvl,
        validate_usd_volume, MAX_PRICE_RATIO,
    },
};

/// Maximum allowed divergence between sqrtPriceX96-derived price and implied price from swap amounts.
/// If divergence exceeds this threshold, we use the implied price instead.
/// This handles V4 pools with custom curve hooks that modify pricing.
/// 10% = 0.10
const MAX_PRICE_DIVERGENCE: f64 = 0.10;

/// Minimum TVL for a pool to be considered for pricing/volume (dev safeguard).
/// Helps drop illiquid pools that can create outsized USD volumes from tiny swaps.
const MIN_POOL_TVL_USD: f64 = 5_000.0;

/// Price resolution for USD calculations.
///
/// Resolves token prices using a priority-based approach:
/// 1. Stablecoins → $1.00 (direct)
/// 2. Native token → native_price_usd (direct)
/// 3. Major tokens → derive via native/stable pools (1-hop)
/// 4. Generic tokens → derive via 2-hop path finding
///
/// Uses Uniswap's whitelist approach for volume tracking:
/// - Only tracks USD volume when at least one token is whitelisted
/// - Whitelist = stablecoins + wrapped native + major tokens
///
/// For V4 pools with hooks, validates sqrtPriceX96-derived prices against
/// implied prices from swap amounts to detect custom curve hooks.
pub struct PriceResolver {
    chain_tokens: Arc<ChainTokens>,
    native_price_usd: f64,
    /// In-memory price cache for tokens resolved in this batch
    token_prices: FxHashMap<String, f64>,
}

impl PriceResolver {
    pub fn new(chain_tokens: Arc<ChainTokens>, native_price_usd: f64) -> Self {
        Self {
            chain_tokens,
            native_price_usd,
            token_prices: FxHashMap::default(),
        }
    }

    /// Check if a token is on the whitelist (stablecoin, wrapped native, or major token).
    ///
    /// Uniswap subgraph only tracks USD values for pools with whitelisted tokens.
    /// This prevents garbage prices from scam tokens from polluting volume metrics.
    fn is_whitelisted(&self, token: &str) -> bool {
        self.chain_tokens.is_stable(token)
            || self.chain_tokens.is_wrapped_native(token)
            || self.chain_tokens.is_major_token(token)
    }

    /// Check if pool has sufficient native token liquidity for trusted pricing.
    ///
    /// Returns true if:
    /// - Pool does not contain native token (defers to TVL check)
    /// - Pool contains native token and has >= MIN_NATIVE_LIQUIDITY_USD worth of it
    ///
    /// This is a chain-agnostic check: on Ethereum (ETH ~$3000), needs ~1.7 ETH.
    /// On Monad (token ~$0.02), needs ~250,000 tokens. Works for any native token price.
    fn check_pool_native_liquidity(&self, pool: &Pool) -> bool {
        // Get native token side amount
        let native_amount = if self.chain_tokens.is_wrapped_native(&pool.token0) {
            pool.reserve0_adjusted.unwrap_or(0.0)
        } else if self.chain_tokens.is_wrapped_native(&pool.token1) {
            pool.reserve1_adjusted.unwrap_or(0.0)
        } else {
            // Pool doesn't contain native token - defer to TVL check
            return true;
        };

        has_sufficient_native_liquidity(native_amount, self.native_price_usd)
    }

    /// Calculate implied price from swap amounts.
    ///
    /// implied_price = amount1 / amount0 (token1 per token0)
    ///
    /// This reflects the actual execution price of the swap, which may differ
    /// from sqrtPriceX96-derived price if a V4 hook modifies the swap.
    fn calculate_implied_price(event: &Event) -> Option<f64> {
        let amount0 = event.amount0_adjusted.abs();
        let amount1 = event.amount1_adjusted.abs();

        if amount0 > 1e-18 && amount1 > 0.0 {
            let implied = amount1 / amount0;
            // Validate the implied price is within reasonable bounds
            return validate_price_ratio(implied);
        }

        None
    }

    /// Calculate price divergence between two prices.
    ///
    /// Returns the absolute percentage difference: |price1/price2 - 1|
    fn calculate_divergence(price1: f64, price2: f64) -> f64 {
        if price2 > 0.0 && price1 > 0.0 {
            (price1 / price2 - 1.0).abs()
        } else {
            0.0
        }
    }

    /// Get USD price for a token.
    ///
    /// Resolution priority:
    /// 1. Cache hit → return cached price
    /// 2. Stablecoin → return 1.0
    /// 3. Native token → return native_price_usd
    /// 4. Major/Generic → find path via pools
    ///
    /// Returns 0.0 if no price path can be found.
    pub fn get_token_price_usd(&mut self, token: &str, pools: &FxHashMap<String, Pool>) -> f64 {
        let token_lower = token.to_lowercase();

        // 1. Check cache
        if let Some(&price) = self.token_prices.get(&token_lower) {
            return price;
        }

        // 2. Stablecoin = $1.00
        if self.chain_tokens.is_stable(&token_lower) {
            self.token_prices.insert(token_lower, 1.0);
            return 1.0;
        }

        // 3. Native token = native_price_usd
        if self.chain_tokens.is_wrapped_native(&token_lower) {
            self.token_prices.insert(token_lower, self.native_price_usd);
            return self.native_price_usd;
        }

        // 4. Try to find price via pools (major tokens and generic tokens)
        let price = self.derive_token_price(&token_lower, pools, 0);
        self.token_prices.insert(token_lower, price);
        price
    }

    /// Derive token price by finding a path through pools.
    ///
    /// Attempts to find a pool where:
    /// - The token is paired with a stablecoin → price directly
    /// - The token is paired with native → price via native_price_usd
    /// - The token is paired with another token → recursive lookup (limited depth)
    ///
    /// Uses the new Uniswap-style token0_price/token1_price fields.
    /// `depth` parameter prevents infinite recursion (max 2 hops).
    /// Derive token price by finding a path through pools.
    ///
    /// Attempts to find a pool where:
    /// - The token is paired with a stablecoin → price directly
    /// - The token is paired with native → price via native_price_usd
    /// - The token is paired with another token → recursive lookup (limited depth)
    ///
    /// Uses the new Uniswap-style token0_price/token1_price fields.
    /// `depth` parameter prevents infinite recursion (max 2 hops).
    ///
    /// IMPROVEMENT: Now iterates ALL candidate pools and picks the one with the
    /// highest liquidity (value on the paired/known side) to avoid "fake" or
    /// illiquid pools from setting bad prices.
    fn derive_token_price(
        &mut self,
        token: &str,
        pools: &FxHashMap<String, Pool>,
        depth: u8,
    ) -> f64 {
        // Prevent infinite recursion - max 2 hops
        if depth > 1 {
            return 0.0;
        }

        let mut best_price = 0.0;
        let mut max_liquidity_value = 0.0;

        // Search through pools for one containing this token
        for pool in pools.values() {
            // Use the new token0_price/token1_price fields (Uniswap style)
            // token0_price = how much token0 per 1 token1
            // token1_price = how much token1 per 1 token0
            let (paired_token, token_price_in_paired, is_token0) = if pool.token0 == token {
                // Token is token0, paired with token1
                // We want: how much token1 per 1 token0 = token1_price
                (&pool.token1, pool.token1_price, true)
            } else if pool.token1 == token {
                // Token is token1, paired with token0
                // We want: how much token0 per 1 token1 = token0_price
                (&pool.token0, pool.token0_price, false)
            } else {
                continue;
            };

            // Validate the price ratio from the pool
            let Some(price_in_paired) = token_price_in_paired.and_then(validate_price_ratio) else {
                continue;
            };

            // Try to get paired token's USD price - ONLY for whitelisted tokens
            // This prevents deriving prices through garbage pools
            let paired_price_usd = if self.chain_tokens.is_stable(&paired_token) {
                1.0
            } else if self.chain_tokens.is_wrapped_native(&paired_token) {
                self.native_price_usd
            } else if self.chain_tokens.is_major_token(&paired_token) && depth == 0 {
                // Only allow 1 more hop for major tokens (they have reliable pools)
                self.derive_token_price(&paired_token, pools, depth + 1)
            } else {
                // Paired token is not whitelisted - skip this pool
                continue;
            };

            if paired_price_usd > 0.0 {
                // price_in_paired = how much paired_token per 1 token
                // token_usd = price_in_paired * paired_price_usd
                let token_usd = price_in_paired * paired_price_usd;
                // Validate with absolute bounds first
                let bounded = validate_usd_price(token_usd);
                // Then validate relative to native price (catches inversion errors)
                let validated = validate_usd_price_relative(bounded, self.native_price_usd);

                if validated > 0.0 {
                    // Calculate "Liquidity Value" of the paired side to weight this price
                    // We trust the paired side (Quote) value, so AmountQuote * PriceQuote = LiquidityValue

                    let decimals =
                        if is_token0 { pool.token1_decimals } else { pool.token0_decimals };

                    let paired_balance_adjusted =
                        if let (Some(liquidity_str), Some(sqrt_price_str)) =
                            (&pool.liquidity, &pool.sqrt_price_x96)
                        {
                            // Priority: V3/V4 Liquidity calculation
                            if let (Some(liquidity), Some(sqrt_price_x96)) = (
                                str_to_f64_with_decimals(liquidity_str, 0),
                                str_to_f64_with_decimals(sqrt_price_str, 0),
                            ) {
                                let (r0, r1) = calculate_reserves_from_liquidity_subgraph(
                                    liquidity,
                                    sqrt_price_x96,
                                );
                                let raw_balance = if is_token0 { r1 } else { r0 }; // If token is 0, paired is 1

                                if decimals > 0 {
                                    raw_balance / 10_f64.powi(decimals as i32)
                                } else {
                                    raw_balance
                                }
                            } else {
                                0.0
                            }
                        } else if let (Some(r0), Some(r1)) =
                            (pool.reserve0_adjusted, pool.reserve1_adjusted)
                        {
                            // Fallback: V2 Reserves
                            if is_token0 {
                                r1
                            } else {
                                r0
                            }
                        } else {
                            0.0
                        };

                    let weight = paired_balance_adjusted * paired_price_usd;

                    // If this pool has more liquidity (value) than previous best, use it
                    // Filter out low-liquidity pools (< $5000 paired-side value)
                    if weight > max_liquidity_value && weight > 5000.0 {
                        max_liquidity_value = weight;
                        best_price = validated;
                    }
                }
            }
        }

        best_price
    }

    /// Calculate price_usd and volume_usd for a swap event.
    ///
    /// Uses Uniswap subgraph's whitelist approach:
    /// - If both tokens are whitelisted → sum their USD values
    /// - If only one token is whitelisted → double that token's USD value
    /// - If neither token is whitelisted → volume_usd = 0 (not tracked)
    ///
    /// For V4 pools with hooks, validates sqrtPriceX96-derived prices against
    /// implied prices from swap amounts. If they diverge > 10%, uses the
    /// implied price instead (handles custom curve hooks).
    ///
    /// This prevents scam tokens from generating fake volume.
    pub fn price_swap_event(
        &mut self,
        event: &mut Event,
        pool: &Pool,
        pools: &FxHashMap<String, Pool>,
    ) {
        // Drop illiquid pools (when we have a TVL reading)
        if let Some(tvl_raw) = pool.tvl_usd {
            let tvl = validate_usd_tvl(tvl_raw);
            if tvl <= 0.0 || tvl < MIN_POOL_TVL_USD {
                event.price_usd = 0.0;
                event.volume_usd = 0.0;
                event.is_suspicious = true;
                return;
            }
        } else {
            // If TVL is missing (common for new pools in same batch), try to estimate it
            // from native token liquidity if available. This catches V2 pools.
            let native_amount = if self.chain_tokens.is_wrapped_native(&pool.token0) {
                pool.reserve0_adjusted.unwrap_or(0.0)
            } else if self.chain_tokens.is_wrapped_native(&pool.token1) {
                pool.reserve1_adjusted.unwrap_or(0.0)
            } else {
                0.0
            };

            if native_amount > 0.0 {
                let native_value = native_amount * self.native_price_usd;
                if native_value > 0.0 {
                    // Estimate TVL as 2x native side (standard V2 assumption)
                    let estimated_tvl = native_value * 2.0;

                    // Check if volume is suspicious relative to this estimated TVL
                    // We calculate approximate volume_usd here just for the check
                    // actual volume_usd calculation happens later
                    let quote_token_usd = if self.chain_tokens.is_stable(&pool.quote_token) {
                        1.0
                    } else if self.chain_tokens.is_wrapped_native(&pool.quote_token) {
                        self.native_price_usd
                    } else {
                        0.0
                    };

                    if quote_token_usd > 0.0 {
                        let base_is_token0 = pool.base_token == pool.token0;
                        let quote_amount = if base_is_token0 {
                            event.amount1_adjusted.abs()
                        } else {
                            event.amount0_adjusted.abs()
                        };

                        let approx_volume_usd = quote_amount * quote_token_usd;

                        if is_suspicious_volume_to_tvl(approx_volume_usd, estimated_tvl) {
                            event.price_usd = 0.0;
                            event.volume_usd = 0.0;
                            event.is_suspicious = true;
                            return;
                        }
                    }
                }
            }
        }

        // Check minimum native token liquidity (chain-agnostic $5000 threshold)
        if !self.check_pool_native_liquidity(pool) {
            event.price_usd = 0.0;
            event.volume_usd = 0.0;
            event.is_suspicious = true;
            return;
        }

        // Check whitelist status for both tokens
        let token0_whitelisted = self.is_whitelisted(&pool.token0);
        let token1_whitelisted = self.is_whitelisted(&pool.token1);

        // If neither token is whitelisted, don't track USD values
        // This is how Uniswap subgraph handles scam tokens
        if !token0_whitelisted && !token1_whitelisted {
            event.price_usd = 0.0;
            event.volume_usd = 0.0;
            return;
        }

        // Get quote token's USD price - ONLY for whitelisted tokens
        // This prevents garbage prices from being derived through scam token pools
        let quote_token_usd = if self.chain_tokens.is_stable(&pool.quote_token) {
            1.0
        } else if self.chain_tokens.is_wrapped_native(&pool.quote_token) {
            self.native_price_usd
        } else if self.chain_tokens.is_major_token(&pool.quote_token) {
            // Only derive price for major tokens (they have reliable pools)
            self.get_token_price_usd(&pool.quote_token, pools)
        } else {
            // Quote token is not whitelisted - can't reliably price this pool
            0.0
        };

        // If we can't price the quote token, we can't calculate values
        if quote_token_usd <= 0.0 || !quote_token_usd.is_finite() {
            event.price_usd = 0.0;
            event.volume_usd = 0.0;
            return;
        }

        // Derive base token's USD price from THIS pool's exchange rate (sqrtPriceX96-based)
        let base_is_token0 = pool.base_token == pool.token0;

        // Get price ratio with bounds validation (from pool state)
        let pool_based_rate = if base_is_token0 {
            // base = token0, quote = token1
            // token1_price = how much token1 per 1 token0
            pool.token1_price.and_then(validate_price_ratio)
        } else {
            // base = token1, quote = token0
            // token0_price = how much token0 per 1 token1
            pool.token0_price.and_then(validate_price_ratio)
        };

        // Implied price from swap amounts (token1 per token0)
        let implied_price = Self::calculate_implied_price(event);
        let pool_price_token1_per_token0 = pool.price.and_then(validate_price_ratio);

        // Divergence-aware selection: apply to all pools (not just V4 hooks)
        let final_rate = match (pool_based_rate, implied_price, pool_price_token1_per_token0) {
            // Have both pool price and implied price: use implied if divergence is large
            (Some(pool_rate), Some(implied), Some(pool_price_raw)) => {
                let divergence = Self::calculate_divergence(implied, pool_price_raw);
                if divergence > MAX_PRICE_DIVERGENCE {
                    // Convert implied (token1/token0) to the needed direction
                    if base_is_token0 {
                        Some(implied)
                    } else if implied > 0.0 {
                        validate_price_ratio(1.0 / implied)
                    } else {
                        Some(pool_rate)
                    }
                } else {
                    Some(pool_rate)
                }
            },
            // Have implied but no validated pool price: use implied
            (None, Some(implied), _) => {
                if base_is_token0 {
                    validate_price_ratio(implied)
                } else if implied > 0.0 {
                    validate_price_ratio(1.0 / implied)
                } else {
                    None
                }
            },
            // No implied, but have pool-based rate
            (Some(pool_rate), _, _) => Some(pool_rate),
            // Nothing valid
            _ => None,
        };

        // Calculate base token USD price with multi-layer validation
        let raw_base_token_usd = final_rate.map(|r| r * quote_token_usd).unwrap_or(0.0);

        // Layer 1: Absolute bounds check
        let bounded_price = validate_usd_price(raw_base_token_usd);
        if bounded_price <= 0.0 {
            event.price_usd = 0.0;
            event.volume_usd = 0.0;
            return;
        }

        // Layer 2: Relative to native price check
        // This catches inversion errors where rate is used instead of 1/rate
        let base_token_usd = validate_usd_price_relative(bounded_price, self.native_price_usd);
        if base_token_usd <= 0.0 {
            event.price_usd = 0.0;
            event.volume_usd = 0.0;
            return;
        }

        // Layer 3: Cross-validate with implied price from amounts
        // The implied price from swap amounts should roughly match our calculated price
        if Self::calculate_implied_price(event).is_some() {
            // Get quote amount USD value for comparison
            let quote_amount = if base_is_token0 {
                event.amount1_adjusted.abs()
            } else {
                event.amount0_adjusted.abs()
            };
            let quote_value_usd = quote_amount * quote_token_usd;

            // Get base amount
            let base_amount = if base_is_token0 {
                event.amount0_adjusted.abs()
            } else {
                event.amount1_adjusted.abs()
            };

            // Check: base_amount * base_token_usd should ≈ quote_value_usd
            if base_amount > 1e-10 && quote_value_usd > 1e-10 {
                let implied_base_usd = quote_value_usd / base_amount;
                let ratio = base_token_usd / implied_base_usd;

                // If our calculated price is > 100x or < 0.01x the implied price, something is wrong
                if ratio > 100.0 || ratio < 0.01 {
                    // Use the implied price instead (more reliable for actual swap execution)
                    event.price_usd =
                        validate_usd_price_relative(implied_base_usd, self.native_price_usd);
                    if event.price_usd <= 0.0 {
                        event.volume_usd = 0.0;
                        return;
                    }
                } else {
                    event.price_usd = base_token_usd;
                }
            } else {
                event.price_usd = base_token_usd;
            }
        } else {
            event.price_usd = base_token_usd;
        }

        // Calculate volume using Uniswap's whitelist approach
        // Use the validated price_usd (which may have been corrected above)
        let final_base_usd = event.price_usd;

        // Plausibility check: base side vs quote side volume
        let quote_amount = if base_is_token0 {
            event.amount1_adjusted.abs()
        } else {
            event.amount0_adjusted.abs()
        };
        let base_amount = if base_is_token0 {
            event.amount0_adjusted.abs()
        } else {
            event.amount1_adjusted.abs()
        };
        let quote_value_usd = quote_amount * quote_token_usd;
        if !validate_price_against_volume(final_base_usd, base_amount, quote_value_usd, 0.5) {
            // If the implied USD values disagree by >50%, drop the volume to avoid spikes
            event.price_usd = 0.0;
            event.volume_usd = 0.0;
            return;
        }

        // Get token0 and token1 USD prices
        let (token0_usd, token1_usd) = if base_is_token0 {
            (final_base_usd, quote_token_usd)
        } else {
            (quote_token_usd, final_base_usd)
        };

        // Uniswap whitelist volume calculation:
        // - Both whitelisted: use the whitelisted token amounts
        // - One whitelisted: double that token's USD value
        let volume = if token0_whitelisted && token1_whitelisted {
            // Both tokens whitelisted - sum both sides (standard calculation)
            let amount0_usd = event.amount0_adjusted.abs() * token0_usd;
            let amount1_usd = event.amount1_adjusted.abs() * token1_usd;
            // Use max to avoid double counting (Uniswap style)
            amount0_usd.max(amount1_usd)
        } else if token0_whitelisted {
            // Only token0 is whitelisted - double its value
            event.amount0_adjusted.abs() * token0_usd * 2.0
        } else {
            // Only token1 is whitelisted - double its value
            event.amount1_adjusted.abs() * token1_usd * 2.0
        };

        // Validate volume with bounds
        event.volume_usd = validate_usd_volume(volume);

        // Calculate fees
        // fee is in ppm (parts per million). E.g. 3000 = 0.3%
        // Prioritize event.fee (dynamic fees) over pool.fee (static/initial fees)
        let fee_ppm = if event.fee > 0 { Some(event.fee) } else { pool.fee.map(|f| f as u32) };

        if let Some(fee) = fee_ppm {
            event.fees_usd = event.volume_usd * (fee as f64 / 1_000_000.0);
        }

        // Check for suspicious volume/TVL ratio (potential manipulation)
        if let Some(tvl) = pool.tvl_usd {
            if is_suspicious_volume_to_tvl(event.volume_usd, tvl) {
                event.is_suspicious = true;
            }
        }
    }

    /// Calculate price_usd for mint/burn/collect/modify_liquidity events.
    ///
    /// Liquidity events do NOT generate trading volume.
    /// - price_usd = base token's USD price derived from pool's exchange rate
    /// - volume_usd = 0 (liquidity events are not trades)
    pub fn price_liquidity_event(
        &mut self,
        event: &mut Event,
        pool: &Pool,
        pools: &FxHashMap<String, Pool>,
    ) {
        // Liquidity events don't generate trading volume
        event.volume_usd = 0.0;

        // Check whitelist - only price if at least one token is whitelisted
        if !self.is_whitelisted(&pool.token0) && !self.is_whitelisted(&pool.token1) {
            event.price_usd = 0.0;
            return;
        }

        // price_usd = base token's USD price derived from pool's exchange rate
        event.price_usd = self.derive_base_token_usd(pool, pools);
    }

    /// Derive base token's USD price from a pool's own exchange rate.
    ///
    /// This uses the pool's quote token (stablecoin/native/major) as reference
    /// and the pool's exchange rate to calculate the base token's USD value.
    /// Only works for pools where quote token is whitelisted.
    /// Derive base token's USD price from a pool's own exchange rate.
    ///
    /// This uses the pool's quote token (stablecoin/native/major) as reference
    /// and the pool's exchange rate to calculate the base token's USD value.
    /// Only works for pools where quote token is whitelisted.
    fn derive_base_token_usd(&mut self, pool: &Pool, pools: &FxHashMap<String, Pool>) -> f64 {
        // Get quote token's USD price - ONLY for whitelisted tokens
        let quote_token_usd = if self.chain_tokens.is_stable(&pool.quote_token) {
            1.0
        } else if self.chain_tokens.is_wrapped_native(&pool.quote_token) {
            self.native_price_usd
        } else if self.chain_tokens.is_major_token(&pool.quote_token) {
            // Only derive price for major tokens (they have reliable pools)
            self.get_token_price_usd(&pool.quote_token, pools)
        } else {
            // Quote token is not whitelisted - can't reliably price
            0.0
        };

        if quote_token_usd <= 0.0 {
            return 0.0;
        }

        // Determine which price field gives us "quote per base"
        let exchange_rate = if pool.base_token == pool.token0 {
            // base = token0, quote = token1
            // We need: how much token1 (quote) per 1 token0 (base) = token1_price
            pool.token1_price
        } else {
            // base = token1, quote = token0
            // We need: how much token0 (quote) per 1 token1 (base) = token0_price
            pool.token0_price
        };

        // Validate the exchange rate and calculate USD price with multi-layer bounds
        match exchange_rate.and_then(validate_price_ratio) {
            Some(rate) => {
                let raw_price = rate * quote_token_usd;
                let bounded = validate_usd_price(raw_price);
                validate_usd_price_relative(bounded, self.native_price_usd)
            },
            None => 0.0,
        }
    }

    /// Price an event based on its type.
    pub fn price_event(&mut self, event: &mut Event, pool: &Pool, pools: &FxHashMap<String, Pool>) {
        match event.event_type.as_str() {
            "swap" => self.price_swap_event(event, pool, pools),
            "mint" | "burn" | "collect" | "modify_liquidity" => {
                self.price_liquidity_event(event, pool, pools)
            },
            _ => {},
        }
    }

    /// Calculate pool USD values: price_usd and tvl_usd.
    ///
    /// Uses Uniswap's whitelist approach:
    /// - Only calculates USD values if at least one token is whitelisted
    /// - This prevents garbage values from scam token pools
    ///
    /// Returns (price_usd, tvl_usd)
    pub fn calculate_pool_pricing(
        &mut self,
        pool: &Pool,
        pools: &FxHashMap<String, Pool>,
    ) -> (Option<f64>, Option<f64>) {
        let mut price_usd = None;
        let mut tvl_usd = None;

        // Check whitelist - only price pools with at least one whitelisted token
        let token0_whitelisted = self.is_whitelisted(&pool.token0);
        let token1_whitelisted = self.is_whitelisted(&pool.token1);

        if !token0_whitelisted && !token1_whitelisted {
            // Neither token is whitelisted - don't set USD values
            return (None, None);
        }

        // Get quote token's USD price - ONLY for whitelisted tokens
        let quote_token_usd = if self.chain_tokens.is_stable(&pool.quote_token) {
            1.0
        } else if self.chain_tokens.is_wrapped_native(&pool.quote_token) {
            self.native_price_usd
        } else if self.chain_tokens.is_major_token(&pool.quote_token) {
            // Only derive price for major tokens (they have reliable pools)
            self.get_token_price_usd(&pool.quote_token, pools)
        } else {
            // Quote token is not whitelisted - can't reliably price
            0.0
        };

        // Calculate base token's USD price from THIS pool's exchange rate
        if quote_token_usd > 0.0 {
            let base_token_lower = pool.base_token.to_lowercase();
            let exchange_rate = if base_token_lower == pool.token0.to_lowercase() {
                pool.token1_price
            } else {
                pool.token0_price
            };

            // Validate exchange rate and calculate USD price with multi-layer bounds
            if let Some(rate) = exchange_rate.and_then(validate_price_ratio) {
                let raw_price = rate * quote_token_usd;
                // Layer 1: Absolute bounds
                let bounded_price = validate_usd_price(raw_price);
                // Layer 2: Relative to native (catches inversion errors)
                let base_token_usd =
                    validate_usd_price_relative(bounded_price, self.native_price_usd);
                if base_token_usd > 0.0 {
                    price_usd = Some(base_token_usd);
                }
            }
        }

        // Calculate tvl_usd (total value locked)
        // Only use whitelisted token prices for TVL calculation
        let token0_usd =
            if token0_whitelisted { self.get_token_price_usd(&pool.token0, pools) } else { 0.0 };
        let token1_usd =
            if token1_whitelisted { self.get_token_price_usd(&pool.token1, pools) } else { 0.0 };

        // For V2: use reserves
        // For V3/V4: use virtual reserves from liquidity + sqrtPriceX96
        if let (Some(r0), Some(r1)) = (pool.reserve0_adjusted, pool.reserve1_adjusted) {
            // Check protocol version
            let is_v2 = pool.protocol_version.as_deref() == Some("v2");

            // V2 pools have explicit reserves
            // For V2: Assume 50/50 value split if one token is unpriced (double value)
            // For V3/V4: Do NOT assume 50/50. Only sum known values as they track actual balances.
            let tvl = if token0_whitelisted && token1_whitelisted {
                r0 * token0_usd + r1 * token1_usd
            } else if token0_whitelisted {
                if is_v2 {
                    r0 * token0_usd * 2.0
                } else {
                    r0 * token0_usd
                }
            } else {
                if is_v2 {
                    r1 * token1_usd * 2.0
                } else {
                    r1 * token1_usd
                }
            };

            // Validate TVL with bounds
            let validated_tvl = validate_usd_tvl(tvl);
            if validated_tvl > 0.0 {
                tvl_usd = Some(validated_tvl);
            }
        } else if let (Some(liquidity_str), Some(sqrt_price_str)) =
            (&pool.liquidity, &pool.sqrt_price_x96)
        {
            // V3/V4 pools: estimate TVL from liquidity and sqrtPriceX96
            // Use BigDecimal-based parsing to handle large values without precision loss
            if let (Some(liquidity), Some(sqrt_price_x96)) = (
                str_to_f64_with_decimals(liquidity_str, 0),
                str_to_f64_with_decimals(sqrt_price_str, 0),
            ) {
                if liquidity > 0.0 && sqrt_price_x96 > 0.0 {
                    let (r0_raw, r1_raw) =
                        calculate_reserves_from_liquidity_subgraph(liquidity, sqrt_price_x96);

                    // Convert from raw units to adjusted units by dividing by 10^decimals
                    // The calculate_reserves_from_liquidity_subgraph returns RAW amounts
                    let token0_decimals = pool.token0_decimals as u32;
                    let token1_decimals = pool.token1_decimals as u32;

                    let r0_adjusted = if token0_decimals > 0 {
                        r0_raw / 10_f64.powi(token0_decimals as i32)
                    } else {
                        r0_raw
                    };
                    let r1_adjusted = if token1_decimals > 0 {
                        r1_raw / 10_f64.powi(token1_decimals as i32)
                    } else {
                        r1_raw
                    };

                    // If only one token is whitelisted, double that token's TVL contribution
                    let tvl = if token0_whitelisted && token1_whitelisted {
                        r0_adjusted * token0_usd + r1_adjusted * token1_usd
                    } else if token0_whitelisted {
                        r0_adjusted * token0_usd * 2.0
                    } else {
                        r1_adjusted * token1_usd * 2.0
                    };

                    // Validate TVL with bounds
                    let validated_tvl = validate_usd_tvl(tvl);
                    if validated_tvl > 0.0 {
                        tvl_usd = Some(validated_tvl);
                    }
                }
            }
        }

        (price_usd, tvl_usd)
    }

    /// Calculate TVL-weighted average price for a token across all pools
    /// where it is the base token.
    ///
    /// Formula: SUM(pool.price_usd * pool.tvl_usd) / SUM(pool.tvl_usd)
    ///
    /// Returns None if no valid pools are found (avoids division by zero).
    pub fn calculate_token_price(
        &self,
        token_address: &str,
        pools: &FxHashMap<String, Pool>,
    ) -> Option<f64> {
        let token_lower = token_address.to_lowercase();
        let mut weighted_sum = 0.0;
        let mut total_tvl = 0.0;

        for pool in pools.values() {
            // Only include pools where this token is the base token
            if pool.base_token.to_lowercase() != token_lower {
                continue;
            }

            // Skip pools without price or TVL
            let Some(price_usd) = pool.price_usd else {
                continue;
            };
            let Some(tvl_usd) = pool.tvl_usd else {
                continue;
            };

            // Skip pools with invalid prices (already validated when set, but double-check)
            if price_usd <= 0.0 || tvl_usd <= 0.0 {
                continue;
            }

            // Skip pools with extreme prices that somehow slipped through
            if price_usd > MAX_PRICE_RATIO {
                continue;
            }

            weighted_sum += price_usd * tvl_usd;
            total_tvl += tvl_usd;
        }

        if total_tvl <= 0.0 {
            return None;
        }

        let avg_price = weighted_sum / total_tvl;

        // Multi-layer validation: absolute bounds, then relative to native price (if available)
        let validated = validate_usd_price(avg_price);
        if validated <= 0.0 {
            return None;
        }

        let rel_validated = validate_usd_price_relative(validated, self.native_price_usd);
        if rel_validated > 0.0 {
            Some(rel_validated)
        } else {
            None
        }
    }
}
