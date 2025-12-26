//! Job to update price changes (24h, 7d) in PostgreSQL.
//!
//! Calculates price changes from ClickHouse candles and updates PostgreSQL pools/tokens.
//! Only updates pools that have candle data.

use anyhow::Result;
use log::info;

use crate::db::Database;

/// Updates price change percentages for pools and tokens.
///
/// Calculates from ClickHouse candles:
/// - price_change_24h: percentage change from 24 hours ago
/// - price_change_7d: percentage change from 7 days ago
///
/// Only updates pools/tokens that have price history.
pub async fn run(db: &Database) -> Result<()> {
    info!("Starting update_price_changes job...");

    let start = std::time::Instant::now();

    // Update pool price changes
    let pool_count = update_pool_price_changes(db).await?;

    // Update token price changes
    let token_count = update_token_price_changes(db).await?;

    info!(
        "Completed update_price_changes job in {:?} ({} pools, {} tokens)",
        start.elapsed(),
        pool_count,
        token_count
    );
    Ok(())
}

async fn update_pool_price_changes(db: &Database) -> Result<usize> {
    // Query ClickHouse for price changes using candles
    // Only returns pools that have candle data (had trades)
    let query = r#"
        WITH current_prices AS (
            SELECT 
                chain_id,
                pool_address,
                close as current_price
            FROM indexer.candles_1h
            WHERE time = (
                SELECT max(time) FROM indexer.candles_1h
            )
        ),
        prices_24h AS (
            SELECT 
                chain_id,
                pool_address,
                close as price_24h_ago
            FROM indexer.candles_1h
            WHERE time >= now() - INTERVAL 25 HOUR 
              AND time <= now() - INTERVAL 23 HOUR
        ),
        prices_7d AS (
            SELECT 
                chain_id,
                pool_address,
                close as price_7d_ago
            FROM indexer.candles_1h
            WHERE time >= now() - INTERVAL 169 HOUR 
              AND time <= now() - INTERVAL 167 HOUR
        )
        SELECT 
            c.chain_id,
            c.pool_address,
            c.current_price,
            if(p24.price_24h_ago > 0, (c.current_price - p24.price_24h_ago) / p24.price_24h_ago * 100, 0) as price_change_24h,
            if(p7d.price_7d_ago > 0, (c.current_price - p7d.price_7d_ago) / p7d.price_7d_ago * 100, 0) as price_change_7d
        FROM current_prices c
        LEFT JOIN prices_24h p24 ON c.chain_id = p24.chain_id AND c.pool_address = p24.pool_address
        LEFT JOIN prices_7d p7d ON c.chain_id = p7d.chain_id AND c.pool_address = p7d.pool_address
        WHERE c.current_price > 0
    "#;

    let rows = db
        .clickhouse
        .client
        .query(query)
        .fetch_all::<PoolPriceChange>()
        .await?;

    if rows.is_empty() {
        info!("No pool price changes to update (no candle data)");
        return Ok(0);
    }

    // Batch update PostgreSQL
    let pg = db.postgres.pool.get().await?;

    let stmt = pg
        .prepare(
            "UPDATE indexer.pools 
         SET price_change_24h = $3, price_change_7d = $4, updated_at = NOW()
         WHERE chain_id = $1 AND address = $2",
        )
        .await?;

    let mut updated = 0;
    for row in &rows {
        let result = pg
            .execute(
                &stmt,
                &[
                    &(row.chain_id as i64),
                    &row.pool_address,
                    &row.price_change_24h,
                    &row.price_change_7d,
                ],
            )
            .await;

        if result.is_ok() {
            updated += 1;
        }
    }

    info!(
        "Updated price changes for {} pools (have candle data)",
        updated
    );
    Ok(updated)
}

async fn update_token_price_changes(db: &Database) -> Result<usize> {
    // Query ClickHouse for token price changes and market cap
    // Calculates circulating_supply from token_supplies (mints - burns)
    let query = r#"
        WITH current_prices AS (
            SELECT 
                chain_id,
                token_address,
                price_usd as current_price
            FROM indexer.token_snapshots
            WHERE time = (SELECT max(time) FROM indexer.token_snapshots)
        ),
        prices_24h AS (
            SELECT 
                chain_id,
                token_address,
                price_usd as price_24h_ago
            FROM indexer.token_snapshots
            WHERE time >= now() - INTERVAL 25 HOUR 
              AND time <= now() - INTERVAL 23 HOUR
            ORDER BY time DESC LIMIT 1 BY chain_id, token_address
        ),
        prices_7d AS (
            SELECT 
                chain_id,
                token_address,
                price_usd as price_7d_ago
            FROM indexer.token_snapshots
            WHERE time >= now() - INTERVAL 169 HOUR 
              AND time <= now() - INTERVAL 167 HOUR
            ORDER BY time DESC LIMIT 1 BY chain_id, token_address
        ),
        supplies AS (
             SELECT
                chain_id,
                token_address,
                sum(total_supply) as circulating_supply
             FROM indexer.token_supplies
             GROUP BY chain_id, token_address
        )
        SELECT 
            c.chain_id,
            c.token_address,
            if(p24.price_24h_ago > 0, (c.current_price - p24.price_24h_ago) / p24.price_24h_ago * 100, 0) as price_change_24h,
            if(p7d.price_7d_ago > 0, (c.current_price - p7d.price_7d_ago) / p7d.price_7d_ago * 100, 0) as price_change_7d,
            COALESCE(s.circulating_supply, 0) as circulating_supply,
            (circulating_supply * c.current_price) as market_cap_usd
        FROM current_prices c
        LEFT JOIN prices_24h p24 ON c.chain_id = p24.chain_id AND c.token_address = p24.token_address
        LEFT JOIN prices_7d p7d ON c.chain_id = p7d.chain_id AND c.token_address = p7d.token_address
        LEFT JOIN supplies s ON c.chain_id = s.chain_id AND c.token_address = s.token_address
        WHERE c.current_price > 0
    "#;

    let rows = db
        .clickhouse
        .client
        .query(query)
        .fetch_all::<TokenPriceChange>()
        .await?;

    if rows.is_empty() {
        info!("No token price changes to update (no snapshot history)");
        return Ok(0);
    }

    // Batch update PostgreSQL
    let pg = db.postgres.pool.get().await?;

    let stmt = pg
        .prepare(
            "UPDATE indexer.tokens 
         SET 
            price_change_24h = $3, 
            price_change_7d = $4, 
            circulating_supply = $5,
            market_cap_usd = $6,
            updated_at = NOW()
         WHERE chain_id = $1 AND address = $2",
        )
        .await?;

    let mut updated = 0;
    for row in &rows {
        let result = pg
            .execute(
                &stmt,
                &[
                    &(row.chain_id as i64),
                    &row.token_address,
                    &row.price_change_24h,
                    &row.price_change_7d,
                    &row.circulating_supply,
                    &row.market_cap_usd,
                ],
            )
            .await;

        if result.is_ok() {
            updated += 1;
        }
    }

    info!("Updated price/market caps for {} tokens", updated);
    Ok(updated)
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct PoolPriceChange {
    chain_id: u64,
    pool_address: String,
    #[allow(dead_code)]
    current_price: f64,
    price_change_24h: f64,
    price_change_7d: f64,
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct TokenPriceChange {
    chain_id: u64,
    token_address: String,
    price_change_24h: f64,
    price_change_7d: f64,
    circulating_supply: f64,
    market_cap_usd: f64,
}
