//! Job to update 24h statistics (volume, swaps, last_swap_at) in PostgreSQL.
//!
//! Aggregates data from ClickHouse events table and updates PostgreSQL pools/tokens tables.
//! Only updates pools that have had activity in the last 24 hours.

use anyhow::Result;
use chrono::{DateTime, Utc};
use log::info;

use crate::db::Database;

/// Updates 24h statistics for pools and tokens.
///
/// Aggregates from ClickHouse:
/// - volume_24h: sum(volume_usd) for last 24 hours
/// - swaps_24h: count() of swaps for last 24 hours
/// - last_swap_at: max(timestamp) for swaps
///
/// Only updates pools/tokens that had activity in the window.
pub async fn run(db: &Database) -> Result<()> {
    info!("Starting update_24h_stats job...");

    let start = std::time::Instant::now();

    // Update pool 24h stats
    let pool_count = update_pool_stats(db).await?;

    // Update token 24h stats
    let token_count = update_token_stats(db).await?;

    info!(
        "Completed update_24h_stats job in {:?} ({} pools, {} tokens)",
        start.elapsed(),
        pool_count,
        token_count
    );
    Ok(())
}

async fn update_pool_stats(db: &Database) -> Result<usize> {
    // Query ClickHouse for 24h pool aggregates
    // Only returns pools that had swap activity in the last 24 hours
    let query = r#"
        SELECT 
            chain_id,
            pool_address,
            sum(volume_usd) as volume_24h,
            count() as swaps_24h,
            max(timestamp) as last_swap_at
        FROM indexer.events
        WHERE timestamp >= now() - INTERVAL 24 HOUR 
          AND event_type = 'swap'
        GROUP BY chain_id, pool_address
    "#;

    let rows = db
        .clickhouse
        .client
        .query(query)
        .fetch_all::<PoolStats24h>()
        .await?;

    if rows.is_empty() {
        info!("No pool stats to update (no swaps in last 24h)");
        return Ok(0);
    }

    // Batch update PostgreSQL using UNNEST
    let pg = db.postgres.pool.get().await?;

    // Prepare vectors for batch update
    let mut chain_ids = Vec::with_capacity(rows.len());
    let mut addresses = Vec::with_capacity(rows.len());
    let mut volumes = Vec::with_capacity(rows.len());
    let mut swaps = Vec::with_capacity(rows.len());
    let mut last_swaps = Vec::with_capacity(rows.len());

    for row in &rows {
        chain_ids.push(row.chain_id as i64);
        addresses.push(row.pool_address.clone());
        volumes.push(row.volume_24h);
        swaps.push(row.swaps_24h as i64);

        let last_swap_chrono: DateTime<Utc> = DateTime::from_timestamp(
            row.last_swap_at.unix_timestamp(),
            row.last_swap_at.nanosecond(),
        )
        .unwrap_or_default();
        last_swaps.push(last_swap_chrono);
    }

    let stmt = "
        UPDATE indexer.pools p
        SET 
            volume_24h = data.volume_24h,
            swaps_24h = data.swaps_24h,
            last_swap_at = data.last_swap_at,
            updated_at = NOW()
        FROM (
            SELECT * FROM UNNEST(
                $1::bigint[], 
                $2::text[], 
                $3::float8[], 
                $4::bigint[], 
                $5::timestamptz[]
            ) AS t(chain_id, address, volume_24h, swaps_24h, last_swap_at)
        ) AS data
        WHERE p.chain_id = data.chain_id AND p.address = data.address
    ";

    pg.execute(
        stmt,
        &[&chain_ids, &addresses, &volumes, &swaps, &last_swaps],
    )
    .await?;

    info!("Updated 24h stats for {} pools (had activity)", rows.len());
    Ok(rows.len())
}

async fn update_token_stats(db: &Database) -> Result<usize> {
    // Query ClickHouse for 24h token aggregates (by token0 and token1)
    // Only returns tokens that had swap activity in the last 24 hours
    let query = r#"
        SELECT 
            chain_id,
            token_address,
            sum(volume_usd) as volume_24h,
            count() as swaps_24h
        FROM (
            SELECT chain_id, token0 as token_address, volume_usd / 2 as volume_usd
            FROM indexer.events
            WHERE timestamp >= now() - INTERVAL 24 HOUR AND event_type = 'swap'
            UNION ALL
            SELECT chain_id, token1 as token_address, volume_usd / 2 as volume_usd
            FROM indexer.events
            WHERE timestamp >= now() - INTERVAL 24 HOUR AND event_type = 'swap'
        )
        GROUP BY chain_id, token_address
    "#;

    let rows = db
        .clickhouse
        .client
        .query(query)
        .fetch_all::<TokenStats24h>()
        .await?;

    if rows.is_empty() {
        info!("No token stats to update (no swaps in last 24h)");
        return Ok(0);
    }

    // Batch update PostgreSQL using UNNEST
    let pg = db.postgres.pool.get().await?;

    // Prepare vectors
    let mut chain_ids = Vec::with_capacity(rows.len());
    let mut addresses = Vec::with_capacity(rows.len());
    let mut volumes = Vec::with_capacity(rows.len());
    let mut swaps = Vec::with_capacity(rows.len());

    for row in &rows {
        chain_ids.push(row.chain_id as i64);
        addresses.push(row.token_address.clone());
        volumes.push(row.volume_24h);
        swaps.push(row.swaps_24h as i64);
    }

    let stmt = "
        UPDATE indexer.tokens t
        SET 
            volume_24h = data.volume_24h,
            swaps_24h = data.swaps_24h,
            updated_at = NOW()
        FROM (
            SELECT * FROM UNNEST(
                $1::bigint[], 
                $2::text[], 
                $3::float8[], 
                $4::bigint[]
            ) AS t(chain_id, address, volume_24h, swaps_24h)
        ) AS data
        WHERE t.chain_id = data.chain_id AND t.address = data.address
    ";

    pg.execute(stmt, &[&chain_ids, &addresses, &volumes, &swaps])
        .await?;

    info!("Updated 24h stats for {} tokens (had activity)", rows.len());
    Ok(rows.len())
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct PoolStats24h {
    chain_id: u64,
    pool_address: String,
    volume_24h: f64,
    swaps_24h: u64,
    #[serde(with = "clickhouse::serde::time::datetime")]
    last_swap_at: time::OffsetDateTime,
}

#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct TokenStats24h {
    chain_id: u64,
    token_address: String,
    volume_24h: f64,
    swaps_24h: u64,
}
