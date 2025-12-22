//! Job to take token snapshots for historical tracking.
//!
//! Reads current token state from PostgreSQL and sends snapshots to the batch inserter.
//! Only snapshots tokens that have been updated since the last run.

use anyhow::Result;
use log::info;
use tokio::sync::mpsc;

use crate::db::{clickhouse::SnapshotMessage, models::TokenSnapshot, Database, IngestMessage};

const JOB_NAME: &str = "token_snapshots";

/// Takes snapshots of tokens that have been updated since the last run.
///
/// Snapshot data includes:
/// - price_usd, market_cap_usd, circulating_supply
/// - volume_usd, swap_count, pool_count
pub async fn run(db: &Database, live_tx: &mpsc::Sender<IngestMessage>) -> Result<()> {
    info!("Starting token_snapshots job...");

    let start = std::time::Instant::now();
    let now = time::OffsetDateTime::now_utc();

    // Get last run time from DB, default to 1 day ago for first run
    let last_run = db
        .postgres
        .get_cron_checkpoint(JOB_NAME)
        .await?
        .unwrap_or_else(|| now - time::Duration::days(1));

    // Convert to chrono for PostgreSQL query
    let since_chrono =
        chrono::DateTime::<chrono::Utc>::from_timestamp(last_run.unix_timestamp(), 0)
            .unwrap_or_default();

    // Read only tokens updated since last run
    let pg = db.postgres.pool.get().await?;

    let rows = pg
        .query(
            r#"
        SELECT 
            chain_id, address, price_usd, market_cap_usd,
            circulating_supply, volume_24h, swaps_24h, pool_count
        FROM indexer.tokens
        WHERE updated_at > $1 AND (price_usd > 0 OR volume_24h > 0)
        "#,
            &[&since_chrono],
        )
        .await?;

    if rows.is_empty() {
        info!(
            "No tokens to snapshot (none updated since {:?})",
            since_chrono
        );
        // Update last run time even if no rows
        db.postgres.set_cron_checkpoint(JOB_NAME, now).await?;
        return Ok(());
    }

    // Build snapshot records using the existing model
    let mut snapshots = Vec::with_capacity(rows.len());

    for row in &rows {
        let chain_id: i64 = row.get("chain_id");
        let address: String = row.get("address");
        let price_usd: Option<f64> = row.get("price_usd");
        let market_cap_usd: Option<f64> = row.get("market_cap_usd");
        let circulating_supply: Option<f64> = row.get("circulating_supply");
        let volume_24h: Option<f64> = row.get("volume_24h");
        let swaps_24h: Option<i64> = row.get("swaps_24h");
        let pool_count: Option<i64> = row.get("pool_count");

        snapshots.push(TokenSnapshot::new(
            chain_id as u64,
            address,
            now,
            price_usd.unwrap_or(0.0),
            price_usd.unwrap_or(0.0), // price_open - same as current for daily snapshot
            price_usd.unwrap_or(0.0), // price_high
            price_usd.unwrap_or(0.0), // price_low
            market_cap_usd.unwrap_or(0.0),
            circulating_supply.unwrap_or(0.0),
            volume_24h.unwrap_or(0.0),
            swaps_24h.unwrap_or(0) as u64,
            pool_count.unwrap_or(0) as u32,
        ));
    }

    // Send to batch inserter
    live_tx
        .send(IngestMessage::Snapshots(SnapshotMessage {
            pool_snapshots: vec![],
            token_snapshots: snapshots.clone(),
        }))
        .await?;

    // Update last run time in DB
    db.postgres.set_cron_checkpoint(JOB_NAME, now).await?;

    info!(
        "Sent {} token snapshots to batch inserter in {:?}",
        snapshots.len(),
        start.elapsed()
    );
    Ok(())
}
