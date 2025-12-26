use log::error;

use crate::db::models::{DatabaseChain, NativeTokenPrice, Pool, SyncCheckpoint, Token};
use crate::db::postgres::PostgresClient;

/// Sanitize a string for PostgreSQL by removing null bytes (0x00)
/// which are invalid in UTF-8 text columns
fn sanitize_string(s: &str) -> String {
    s.replace('\0', "")
}

impl PostgresClient {
    // ==================== CHAINS ====================

    /// Get all chains from the database
    pub async fn get_chains(&self) -> anyhow::Result<Vec<DatabaseChain>> {
        let client = self.pool.get().await?;
        let query = r#"
            SELECT 
                chain_id, name, rpc_url, hypersync_url, enabled,
                native_token_address, native_token_decimals, native_token_name, native_token_symbol,
                stable_token_address, stable_token_decimals, stable_pool_address,
                major_tokens, stablecoins, factories, updated_at
            FROM indexer.chains
        "#;

        let rows = client.query(query, &[]).await?;
        let chains = rows
            .iter()
            .map(|row| {
                // Lowercase all address fields for consistent comparisons
                let native_token_address: String = row.get("native_token_address");
                let stable_token_address: String = row.get("stable_token_address");
                let stable_pool_address: String = row.get("stable_pool_address");
                let major_tokens: Vec<String> = row.get("major_tokens");
                let stablecoins: Vec<String> = row.get("stablecoins");
                let factories: Vec<String> = row.get("factories");

                let chain_id: i64 = row.get("chain_id");
                let native_decimals: i16 = row.get("native_token_decimals");
                let stable_decimals: i16 = row.get("stable_token_decimals");

                DatabaseChain {
                    chain_id: chain_id as u64,
                    name: row.get("name"),
                    rpc_url: row.get("rpc_url"),
                    hypersync_url: row.get("hypersync_url"),
                    enabled: row.get("enabled"),
                    native_token_address: native_token_address.to_lowercase(),
                    native_token_decimals: native_decimals as u8,
                    native_token_name: row.get("native_token_name"),
                    native_token_symbol: row.get("native_token_symbol"),
                    stable_token_address: stable_token_address.to_lowercase(),
                    stable_token_decimals: stable_decimals as u8,
                    stable_pool_address: stable_pool_address.to_lowercase(),
                    major_tokens: major_tokens.into_iter().map(|s| s.to_lowercase()).collect(),
                    stablecoins: stablecoins.into_iter().map(|s| s.to_lowercase()).collect(),
                    factories: factories.into_iter().map(|s| s.to_lowercase()).collect(),
                    updated_at: row.get("updated_at"),
                }
            })
            .collect();

        Ok(chains)
    }

    /// Insert or update a single chain
    pub async fn set_chain(&self, chain: &DatabaseChain) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let query = r#"
            INSERT INTO indexer.chains (
                chain_id, name, rpc_url, hypersync_url, enabled,
                native_token_address, native_token_decimals, native_token_name, native_token_symbol,
                stable_token_address, stable_token_decimals, stable_pool_address,
                major_tokens, stablecoins, factories, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (chain_id) DO UPDATE SET
                name = EXCLUDED.name,
                rpc_url = EXCLUDED.rpc_url,
                hypersync_url = EXCLUDED.hypersync_url,
                enabled = EXCLUDED.enabled,
                native_token_address = EXCLUDED.native_token_address,
                native_token_decimals = EXCLUDED.native_token_decimals,
                native_token_name = EXCLUDED.native_token_name,
                native_token_symbol = EXCLUDED.native_token_symbol,
                stable_token_address = EXCLUDED.stable_token_address,
                stable_token_decimals = EXCLUDED.stable_token_decimals,
                stable_pool_address = EXCLUDED.stable_pool_address,
                major_tokens = EXCLUDED.major_tokens,
                stablecoins = EXCLUDED.stablecoins,
                factories = EXCLUDED.factories,
                updated_at = EXCLUDED.updated_at
        "#;

        let chain_id_i64 = chain.chain_id as i64;
        let native_decimals_i16 = chain.native_token_decimals as i16;
        let stable_decimals_i16 = chain.stable_token_decimals as i16;

        client
            .execute(
                query,
                &[
                    &chain_id_i64,
                    &chain.name,
                    &chain.rpc_url,
                    &chain.hypersync_url,
                    &chain.enabled,
                    &chain.native_token_address,
                    &native_decimals_i16,
                    &chain.native_token_name,
                    &chain.native_token_symbol,
                    &chain.stable_token_address,
                    &stable_decimals_i16,
                    &chain.stable_pool_address,
                    &chain.major_tokens,
                    &chain.stablecoins,
                    &chain.factories,
                    &chain.updated_at,
                ],
            )
            .await
            .map_err(|e| {
                error!("Failed to insert chain {}: {:?}", chain.chain_id, e);
                e
            })?;

        Ok(())
    }

    // ==================== TOKENS ====================

    /// Get tokens by chain_id and addresses (batched)
    pub async fn get_tokens(
        &self,
        chain_id: i64,
        addresses: &[String],
    ) -> anyhow::Result<Vec<Token>> {
        if addresses.is_empty() {
            return Ok(vec![]);
        }

        let client = self.pool.get().await?;
        let query = r#"
            SELECT 
                chain_id, address, symbol, name, decimals,
                price_usd, price_updated_at, price_change_24h, price_change_7d,
                logo_url, banner_url, website, twitter, telegram, discord,
                volume_24h, swaps_24h, total_swaps, total_volume_usd, pool_count,
                circulating_supply, market_cap_usd, first_seen_block, last_activity_at, updated_at
            FROM indexer.tokens
            WHERE chain_id = $1 AND address = ANY($2)
        "#;

        let rows = client.query(query, &[&chain_id, &addresses]).await?;
        let tokens = rows.iter().map(|row| row_to_token(row)).collect();

        Ok(tokens)
    }

    /// Batch insert/update multiple tokens (true batch insert with multi-row VALUES)
    pub async fn set_tokens(&self, tokens: &[&Token]) -> anyhow::Result<()> {
        if tokens.is_empty() {
            return Ok(());
        }

        const COLS_PER_ROW: usize = 25;
        const BATCH_SIZE: usize = 300; // Smaller batches due to large number of columns

        let client = self.pool.get().await?;

        for chunk in tokens.chunks(BATCH_SIZE) {
            // Build VALUES placeholders: ($1,$2,...,$25), ($26,$27,...,$50), ...
            let values_clauses: Vec<String> = chunk
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let start = i * COLS_PER_ROW + 1;
                    let placeholders: Vec<String> = (start..start + COLS_PER_ROW)
                        .map(|n| format!("${}", n))
                        .collect();
                    format!("({})", placeholders.join(", "))
                })
                .collect();

            let query = format!(
                r#"
                INSERT INTO indexer.tokens (
                    chain_id, address, symbol, name, decimals,
                    price_usd, price_updated_at, price_change_24h, price_change_7d,
                    logo_url, banner_url, website, twitter, telegram, discord,
                    volume_24h, swaps_24h, total_swaps, total_volume_usd, pool_count,
                    circulating_supply, market_cap_usd, first_seen_block, last_activity_at, updated_at
                ) VALUES {}
                ON CONFLICT (chain_id, address) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    decimals = EXCLUDED.decimals,
                    price_usd = EXCLUDED.price_usd,
                    price_updated_at = EXCLUDED.price_updated_at,
                    price_change_24h = EXCLUDED.price_change_24h,
                    price_change_7d = EXCLUDED.price_change_7d,
                    logo_url = EXCLUDED.logo_url,
                    banner_url = EXCLUDED.banner_url,
                    website = EXCLUDED.website,
                    twitter = EXCLUDED.twitter,
                    telegram = EXCLUDED.telegram,
                    discord = EXCLUDED.discord,
                    volume_24h = EXCLUDED.volume_24h,
                    swaps_24h = EXCLUDED.swaps_24h,
                    total_swaps = EXCLUDED.total_swaps,
                    total_volume_usd = EXCLUDED.total_volume_usd,
                    pool_count = EXCLUDED.pool_count,
                    circulating_supply = EXCLUDED.circulating_supply,
                    market_cap_usd = EXCLUDED.market_cap_usd,
                    first_seen_block = EXCLUDED.first_seen_block,
                    last_activity_at = EXCLUDED.last_activity_at,
                    updated_at = EXCLUDED.updated_at
                "#,
                values_clauses.join(", ")
            );

            // Build params array - need to store sanitized strings
            let mut sanitized: Vec<(String, String)> = Vec::with_capacity(chunk.len());
            for token in chunk {
                sanitized.push((sanitize_string(&token.symbol), sanitize_string(&token.name)));
            }

            // Buffers for casted values to ensure they live long enough
            let mut chain_ids: Vec<i64> = Vec::with_capacity(chunk.len());
            let mut decimals_vec: Vec<i16> = Vec::with_capacity(chunk.len());
            let mut swaps_24h_vec: Vec<Option<i64>> = Vec::with_capacity(chunk.len());
            let mut total_swaps_vec: Vec<Option<i64>> = Vec::with_capacity(chunk.len());
            let mut pool_counts: Vec<Option<i64>> = Vec::with_capacity(chunk.len());
            let mut first_seen_blocks: Vec<Option<i64>> = Vec::with_capacity(chunk.len());

            for token in chunk {
                chain_ids.push(token.chain_id as i64);
                decimals_vec.push(token.decimals as i16);
                swaps_24h_vec.push(token.swaps_24h.map(|v| v as i64));
                total_swaps_vec.push(token.total_swaps.map(|v| v as i64));
                pool_counts.push(token.pool_count.map(|v| v as i64));
                first_seen_blocks.push(token.first_seen_block.map(|v| v as i64));
            }

            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                Vec::with_capacity(chunk.len() * COLS_PER_ROW);

            for (i, token) in chunk.iter().enumerate() {
                params.push(&chain_ids[i]);
                params.push(&token.address);
                params.push(&sanitized[i].0);
                params.push(&sanitized[i].1);
                params.push(&decimals_vec[i]);
                params.push(&token.price_usd);
                params.push(&token.price_updated_at);
                params.push(&token.price_change_24h);
                params.push(&token.price_change_7d);
                params.push(&token.logo_url);
                params.push(&token.banner_url);
                params.push(&token.website);
                params.push(&token.twitter);
                params.push(&token.telegram);
                params.push(&token.discord);
                params.push(&token.volume_24h);
                params.push(&swaps_24h_vec[i]);
                params.push(&total_swaps_vec[i]);
                params.push(&token.total_volume_usd);
                params.push(&pool_counts[i]);
                params.push(&token.circulating_supply);
                params.push(&token.market_cap_usd);
                params.push(&first_seen_blocks[i]);
                params.push(&token.last_activity_at);
                params.push(&token.updated_at);
            }

            client.execute(&query, &params).await.map_err(|e| {
                error!("Failed to batch insert {} tokens: {:?}", chunk.len(), e);
                e
            })?;
        }

        Ok(())
    }

    // ==================== POOLS ====================

    /// Get pools by chain_id and addresses (batched)
    pub async fn get_pools(
        &self,
        chain_id: i64,
        addresses: &[String],
    ) -> anyhow::Result<Vec<Pool>> {
        if addresses.is_empty() {
            return Ok(vec![]);
        }

        let client = self.pool.get().await?;
        let query = r#"
            SELECT 
                chain_id, address, token0, token1, token0_symbol, token1_symbol,
                token0_decimals, token1_decimals, base_token, quote_token, is_inverted,
                quote_token_priority, protocol, protocol_version, factory, fee, initial_fee,
                hook_address, created_at, block_number, tx_hash, reserve0, reserve1,
                reserve0_adjusted, reserve1_adjusted, sqrt_price_x96, tick, tick_spacing,
                liquidity, price, token0_price, token1_price, price_usd, price_change_24h,
                price_change_7d, volume_24h, swaps_24h, total_swaps, total_volume_usd,
                tvl_usd, last_swap_at, updated_at
            FROM indexer.pools
            WHERE chain_id = $1 AND address = ANY($2)
        "#;

        let rows = client.query(query, &[&chain_id, &addresses]).await?;
        let pools = rows.iter().map(|row| row_to_pool(row)).collect();
        Ok(pools)
    }

    /// Batch insert/update multiple pools (true batch insert with multi-row VALUES)
    pub async fn set_pools(&self, pools: &[&Pool]) -> anyhow::Result<()> {
        if pools.is_empty() {
            return Ok(());
        }

        const COLS_PER_ROW: usize = 42;
        const BATCH_SIZE: usize = 300; // Smaller batches to avoid "value too large to transmit"

        let client = self.pool.get().await?;

        for chunk in pools.chunks(BATCH_SIZE) {
            // Build VALUES placeholders
            let values_clauses: Vec<String> = chunk
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let start = i * COLS_PER_ROW + 1;
                    let placeholders: Vec<String> = (start..start + COLS_PER_ROW)
                        .map(|n| format!("${}", n))
                        .collect();
                    format!("({})", placeholders.join(", "))
                })
                .collect();

            let query = format!(
                r#"
                INSERT INTO indexer.pools (
                    chain_id, address, token0, token1, token0_symbol, token1_symbol,
                    token0_decimals, token1_decimals, base_token, quote_token, is_inverted,
                    quote_token_priority, protocol, protocol_version, factory, fee, initial_fee,
                    hook_address, created_at, block_number, tx_hash, reserve0, reserve1,
                    reserve0_adjusted, reserve1_adjusted, sqrt_price_x96, tick, tick_spacing,
                    liquidity, price, token0_price, token1_price, price_usd, price_change_24h,
                    price_change_7d, volume_24h, swaps_24h, total_swaps, total_volume_usd,
                    tvl_usd, last_swap_at, updated_at
                ) VALUES {}
                ON CONFLICT (chain_id, address) DO UPDATE SET
                    -- ANTI-SPOOFING: Immutable fields are NOT updated on conflict
                    -- This prevents fake PoolCreated events from overwriting real pool metadata
                    -- Immutable: token0, token1, token0_symbol, token1_symbol, token0_decimals,
                    --            token1_decimals, base_token, quote_token, is_inverted,
                    --            quote_token_priority, protocol_version, factory, initial_fee,
                    --            created_at, hook_address
                    
                    -- Only update mutable fields (state that changes with swaps/mints/burns)
                    protocol = EXCLUDED.protocol,
                    fee = EXCLUDED.fee,
                    block_number = EXCLUDED.block_number,
                    tx_hash = EXCLUDED.tx_hash,
                    reserve0 = EXCLUDED.reserve0,
                    reserve1 = EXCLUDED.reserve1,
                    reserve0_adjusted = EXCLUDED.reserve0_adjusted,
                    reserve1_adjusted = EXCLUDED.reserve1_adjusted,
                    sqrt_price_x96 = EXCLUDED.sqrt_price_x96,
                    tick = EXCLUDED.tick,
                    tick_spacing = EXCLUDED.tick_spacing,
                    liquidity = EXCLUDED.liquidity,
                    price = EXCLUDED.price,
                    token0_price = EXCLUDED.token0_price,
                    token1_price = EXCLUDED.token1_price,
                    price_usd = EXCLUDED.price_usd,
                    price_change_24h = EXCLUDED.price_change_24h,
                    price_change_7d = EXCLUDED.price_change_7d,
                    volume_24h = EXCLUDED.volume_24h,
                    swaps_24h = EXCLUDED.swaps_24h,
                    total_swaps = EXCLUDED.total_swaps,
                    total_volume_usd = EXCLUDED.total_volume_usd,
                    tvl_usd = EXCLUDED.tvl_usd,
                    last_swap_at = EXCLUDED.last_swap_at,
                    updated_at = EXCLUDED.updated_at
                "#,
                values_clauses.join(", ")
            );

            // Store sanitized strings
            let mut sanitized: Vec<(String, String)> = Vec::with_capacity(chunk.len());
            for pool in chunk {
                sanitized.push((
                    sanitize_string(&pool.token0_symbol),
                    sanitize_string(&pool.token1_symbol),
                ));
            }

            // Buffers for casted values
            let mut chain_ids: Vec<i64> = Vec::with_capacity(chunk.len());
            let mut token0_decimals: Vec<i16> = Vec::with_capacity(chunk.len());
            let mut token1_decimals: Vec<i16> = Vec::with_capacity(chunk.len());
            let mut fees: Vec<Option<i32>> = Vec::with_capacity(chunk.len());
            let mut initial_fees: Vec<Option<i32>> = Vec::with_capacity(chunk.len());
            let mut block_numbers: Vec<Option<i64>> = Vec::with_capacity(chunk.len());
            let mut swaps_24h_vec: Vec<Option<i64>> = Vec::with_capacity(chunk.len());
            let mut total_swaps_vec: Vec<Option<i64>> = Vec::with_capacity(chunk.len());

            for pool in chunk {
                chain_ids.push(pool.chain_id as i64);
                token0_decimals.push(pool.token0_decimals as i16);
                token1_decimals.push(pool.token1_decimals as i16);
                fees.push(pool.fee.map(|v| v as i32));
                initial_fees.push(pool.initial_fee.map(|v| v as i32));
                block_numbers.push(pool.block_number.map(|v| v as i64));
                swaps_24h_vec.push(pool.swaps_24h.map(|v| v as i64));
                total_swaps_vec.push(pool.total_swaps.map(|v| v as i64));
            }

            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                Vec::with_capacity(chunk.len() * COLS_PER_ROW);

            for (i, pool) in chunk.iter().enumerate() {
                params.push(&chain_ids[i]);
                params.push(&pool.address);
                params.push(&pool.token0);
                params.push(&pool.token1);
                params.push(&sanitized[i].0);
                params.push(&sanitized[i].1);
                params.push(&token0_decimals[i]);
                params.push(&token1_decimals[i]);
                params.push(&pool.base_token);
                params.push(&pool.quote_token);
                params.push(&pool.is_inverted);
                params.push(&pool.quote_token_priority);
                params.push(&pool.protocol);
                params.push(&pool.protocol_version);
                params.push(&pool.factory);
                params.push(&fees[i]);
                params.push(&initial_fees[i]);
                params.push(&pool.hook_address);
                params.push(&pool.created_at);
                params.push(&block_numbers[i]);
                params.push(&pool.tx_hash);
                params.push(&pool.reserve0);
                params.push(&pool.reserve1);
                params.push(&pool.reserve0_adjusted);
                params.push(&pool.reserve1_adjusted);
                params.push(&pool.sqrt_price_x96);
                params.push(&pool.tick);
                params.push(&pool.tick_spacing);
                params.push(&pool.liquidity);
                params.push(&pool.price);
                params.push(&pool.token0_price);
                params.push(&pool.token1_price);
                params.push(&pool.price_usd);
                params.push(&pool.price_change_24h);
                params.push(&pool.price_change_7d);
                params.push(&pool.volume_24h);
                params.push(&swaps_24h_vec[i]);
                params.push(&total_swaps_vec[i]);
                params.push(&pool.total_volume_usd);
                params.push(&pool.tvl_usd);
                params.push(&pool.last_swap_at);
                params.push(&pool.updated_at);
            }

            client.execute(&query, &params).await.map_err(|e| {
                error!("Failed to batch insert {} pools: {:?}", chunk.len(), e);
                e
            })?;
        }

        Ok(())
    }

    // ==================== SYNC CHECKPOINT ====================

    /// Get sync checkpoint for a chain
    pub async fn get_sync_checkpoint(
        &self,
        chain_id: u64,
    ) -> anyhow::Result<Option<SyncCheckpoint>> {
        let client = self.pool.get().await?;
        let chain_id = chain_id as i64;
        let query = "SELECT chain_id, last_indexed_block, updated_at FROM indexer.sync_checkpoints WHERE chain_id = $1";

        let row = client.query_opt(query, &[&chain_id]).await?;

        Ok(row.map(|r| SyncCheckpoint {
            chain_id: (r.get::<_, i64>("chain_id")) as u64,
            last_indexed_block: (r.get::<_, i64>("last_indexed_block")) as u64,
            updated_at: r.get("updated_at"),
        }))
    }

    /// Set sync checkpoint for a chain
    pub async fn set_sync_checkpoint(&self, checkpoint: &SyncCheckpoint) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let query = r#"
            INSERT INTO indexer.sync_checkpoints (chain_id, last_indexed_block, updated_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (chain_id) DO UPDATE SET
                last_indexed_block = EXCLUDED.last_indexed_block,
                updated_at = EXCLUDED.updated_at
        "#;

        let chain_id_i64 = checkpoint.chain_id as i64;
        let last_indexed_block_i64 = checkpoint.last_indexed_block as i64;

        client
            .execute(
                query,
                &[&chain_id_i64, &last_indexed_block_i64, &checkpoint.updated_at],
            )
            .await
            .map_err(|e| {
                error!(
                    "Failed to insert sync checkpoint for chain {}: {:?}",
                    checkpoint.chain_id, e
                );
                e
            })?;

        Ok(())
    }

    // ==================== CRON CHECKPOINTS ====================

    /// Get last run timestamp for a cron job
    pub async fn get_cron_checkpoint(
        &self,
        job_name: &str,
    ) -> anyhow::Result<Option<time::OffsetDateTime>> {
        let client = self.pool.get().await?;
        let query = "SELECT last_run_at FROM indexer.cron_checkpoints WHERE job_name = $1";

        let row = client.query_opt(query, &[&job_name]).await?;

        if let Some(row) = row {
            // Convert from chrono::DateTime<Utc> (postgres) to time::OffsetDateTime (application)
            let last_run_at: Option<chrono::DateTime<chrono::Utc>> = row.get("last_run_at");

            if let Some(last_run) = last_run_at {
                let ts = time::OffsetDateTime::from_unix_timestamp(last_run.timestamp())?
                    .replace_nanosecond(last_run.timestamp_subsec_nanos())?;
                return Ok(Some(ts));
            }
        }

        Ok(None)
    }

    /// Set last run timestamp for a cron job
    pub async fn set_cron_checkpoint(
        &self,
        job_name: &str,
        last_run_at: time::OffsetDateTime,
    ) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let query = r#"
            INSERT INTO indexer.cron_checkpoints (job_name, last_run_at, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (job_name) DO UPDATE SET
                last_run_at = EXCLUDED.last_run_at,
                updated_at = NOW()
        "#;

        // Convert from time::OffsetDateTime to chrono::DateTime<Utc>
        let last_run_chrono = chrono::DateTime::<chrono::Utc>::from_timestamp(
            last_run_at.unix_timestamp(),
            last_run_at.nanosecond(),
        )
        .unwrap_or_default();

        client
            .execute(query, &[&job_name, &last_run_chrono])
            .await
            .map_err(|e| {
                error!(
                    "Failed to update checkpoint for cron job {}: {:?}",
                    job_name, e
                );
                e
            })?;

        Ok(())
    }

    // ==================== NATIVE TOKEN PRICE ====================

    /// Get native token price for a chain
    pub async fn get_native_token_price(
        &self,
        chain_id: i64,
    ) -> anyhow::Result<Option<NativeTokenPrice>> {
        let client = self.pool.get().await?;
        let query = "SELECT chain_id, price_usd, updated_at FROM indexer.native_token_prices WHERE chain_id = $1";

        let row = client.query_opt(query, &[&chain_id]).await?;

        Ok(row.map(|r| NativeTokenPrice {
            chain_id: r.get("chain_id"),
            price_usd: r.get("price_usd"),
            updated_at: r.get("updated_at"),
        }))
    }

    /// Set native token price for a chain
    pub async fn set_native_token_price(&self, price: &NativeTokenPrice) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let query = r#"
            INSERT INTO indexer.native_token_prices (chain_id, price_usd, updated_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (chain_id) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                updated_at = EXCLUDED.updated_at
        "#;

        client
            .execute(
                query,
                &[&price.chain_id, &price.price_usd, &price.updated_at],
            )
            .await
            .map_err(|e| {
                error!(
                    "Failed to insert native token price for chain {}: {:?}",
                    price.chain_id, e
                );
                e
            })?;

        Ok(())
    }

    // ==================== POOLS BY TOKEN ====================

    /// Get all pools containing a specific token
    pub async fn get_pools_for_token(
        &self,
        chain_id: i64,
        token_address: &str,
    ) -> anyhow::Result<Vec<Pool>> {
        let client = self.pool.get().await?;
        let query = r#"
            SELECT 
                chain_id, address, token0, token1, token0_symbol, token1_symbol,
                token0_decimals, token1_decimals, base_token, quote_token, is_inverted,
                quote_token_priority, protocol, protocol_version, factory, fee, initial_fee,
                hook_address, created_at, block_number, tx_hash, reserve0, reserve1,
                reserve0_adjusted, reserve1_adjusted, sqrt_price_x96, tick, tick_spacing,
                liquidity, price, token0_price, token1_price, price_usd, price_change_24h,
                price_change_7d, volume_24h, swaps_24h, total_swaps, total_volume_usd,
                tvl_usd, last_swap_at, updated_at
            FROM indexer.pools
            WHERE chain_id = $1 AND (token0 = $2 OR token1 = $2)
        "#;

        let rows = client.query(query, &[&chain_id, &token_address]).await?;
        let pools = rows.iter().map(|row| row_to_pool(row)).collect();

        Ok(pools)
    }
}

// ==================== HELPER FUNCTIONS ====================

fn row_to_token(row: &tokio_postgres::Row) -> Token {
    let chain_id: i64 = row.get("chain_id");
    let address: String = row.get("address");
    let symbol: String = row.get("symbol");
    let name: String = row.get("name");
    let decimals: i16 = row.get("decimals");

    Token {
        chain_id: chain_id as u64,
        address: address.to_lowercase(), // Lowercase addresses for consistent comparisons
        symbol,
        name,
        decimals: decimals as u8,
        price_usd: row.get("price_usd"),
        price_updated_at: row.get("price_updated_at"),
        price_change_24h: row.get("price_change_24h"),
        price_change_7d: row.get("price_change_7d"),
        logo_url: row.get("logo_url"),
        banner_url: row.get("banner_url"),
        website: row.get("website"),
        twitter: row.get("twitter"),
        telegram: row.get("telegram"),
        discord: row.get("discord"),
        volume_24h: row.get("volume_24h"),
        swaps_24h: row.get::<_, Option<i64>>("swaps_24h").map(|v| v as u64),
        total_swaps: row.get::<_, Option<i64>>("total_swaps").map(|v| v as u64),
        total_volume_usd: row.get("total_volume_usd"),
        pool_count: row.get::<_, Option<i64>>("pool_count").map(|v| v as u64),
        circulating_supply: row.get("circulating_supply"),
        market_cap_usd: row.get("market_cap_usd"),
        first_seen_block: row
            .get::<_, Option<i64>>("first_seen_block")
            .map(|v| v as u64),
        last_activity_at: row.get("last_activity_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_pool(row: &tokio_postgres::Row) -> Pool {
    // Helper to get string and lowercase it for address normalization
    fn get_lowercased(row: &tokio_postgres::Row, col: &str) -> String {
        let val: String = row.get(col);
        val.to_lowercase()
    }

    let chain_id: i64 = row.get("chain_id");
    let address: String = get_lowercased(row, "address");
    let token0: String = get_lowercased(row, "token0");
    let token1: String = get_lowercased(row, "token1");
    let token0_symbol: String = row.get("token0_symbol");
    let token1_symbol: String = row.get("token1_symbol");
    let token0_decimals: i16 = row.get("token0_decimals");
    let token1_decimals: i16 = row.get("token1_decimals");

    Pool {
        chain_id: chain_id as u64,
        address,
        token0,
        token1,
        token0_symbol,
        token1_symbol,
        token0_decimals: token0_decimals as u8,
        token1_decimals: token1_decimals as u8,
        base_token: get_lowercased(row, "base_token"),
        quote_token: get_lowercased(row, "quote_token"),
        is_inverted: row.get("is_inverted"),
        quote_token_priority: row.get("quote_token_priority"),
        protocol: row.get("protocol"),
        protocol_version: row.get("protocol_version"),
        factory: row.get("factory"),
        fee: row.get::<_, Option<i32>>("fee").map(|v| v as u32),
        initial_fee: row.get::<_, Option<i32>>("initial_fee").map(|v| v as u32),
        hook_address: row.get("hook_address"),
        created_at: row.get("created_at"),
        block_number: row.get::<_, Option<i64>>("block_number").map(|v| v as u64),
        tx_hash: row.get("tx_hash"),
        reserve0: row.get("reserve0"),
        reserve1: row.get("reserve1"),
        reserve0_adjusted: row.get("reserve0_adjusted"),
        reserve1_adjusted: row.get("reserve1_adjusted"),
        sqrt_price_x96: row.get("sqrt_price_x96"),
        tick: row.get("tick"),
        tick_spacing: row.get("tick_spacing"),
        liquidity: row.get("liquidity"),
        price: row.get("price"),
        token0_price: row.get("token0_price"),
        token1_price: row.get("token1_price"),
        price_usd: row.get("price_usd"),
        price_change_24h: row.get("price_change_24h"),
        price_change_7d: row.get("price_change_7d"),
        volume_24h: row.get("volume_24h"),
        swaps_24h: row.get::<_, Option<i64>>("swaps_24h").map(|v| v as u64),
        total_swaps: row.get::<_, Option<i64>>("total_swaps").map(|v| v as u64),
        total_volume_usd: row.get("total_volume_usd"),
        tvl_usd: row.get("tvl_usd"),
        last_swap_at: row.get("last_swap_at"),
        updated_at: row.get("updated_at"),
    }
}
