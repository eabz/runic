//! Job to take pool snapshots for historical tracking.
//!
//! Reads current pool state from PostgreSQL and sends snapshots to the batch inserter.
//! Only snapshots pools that have been updated since the last run.

use anyhow::Result;
use clickhouse::types::UInt256;
use log::info;
use tokio::sync::mpsc;

use crate::db::{clickhouse::SnapshotMessage, models::PoolSnapshot, Database, IngestMessage};

const JOB_NAME: &str = "pool_snapshots";

/// Takes snapshots of pools that have been updated since the last run.
///
/// Snapshot data includes:
/// - price, price_usd, tvl_usd
/// - reserve0, reserve1, liquidity
/// - volume_24h, swaps_24h, fees_24h
pub async fn run(db: &Database, live_tx: &mpsc::Sender<IngestMessage>) -> Result<()> {
    info!("Starting pool_snapshots job...");

    let start = std::time::Instant::now();
    let now = time::OffsetDateTime::now_utc();

    // Get last run time from DB, default to 1 hour ago for first run
    let last_run = db
        .postgres
        .get_cron_checkpoint(JOB_NAME)
        .await?
        .unwrap_or_else(|| now - time::Duration::hours(1));

    // Convert to chrono for PostgreSQL query
    let since_chrono =
        chrono::DateTime::<chrono::Utc>::from_timestamp(last_run.unix_timestamp(), 0)
            .unwrap_or_default();

    // Read only pools updated since last run
    let pg = db.postgres.pool.get().await?;

    let rows = pg
        .query(
            r#"
        SELECT 
            chain_id, address, price, price_usd, tvl_usd,
            reserve0_adjusted, reserve1_adjusted, liquidity,
            volume_24h, swaps_24h, fee
        FROM indexer.pools
        WHERE updated_at > $1 AND (tvl_usd > 0 OR volume_24h > 0)
        "#,
            &[&since_chrono],
        )
        .await?;

    if rows.is_empty() {
        info!(
            "No pools to snapshot (none updated since {:?})",
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
        let price: Option<f64> = row.get("price");
        let price_usd: Option<f64> = row.get("price_usd");
        let tvl_usd: Option<f64> = row.get("tvl_usd");
        let reserve0: Option<f64> = row.get("reserve0_adjusted");
        let reserve1: Option<f64> = row.get("reserve1_adjusted");
        let liquidity_str: Option<String> = row.get("liquidity");
        let volume_24h: Option<f64> = row.get("volume_24h");
        let swaps_24h: Option<i64> = row.get("swaps_24h");
        let fee: Option<i32> = row.get("fee");

        // Parse liquidity string to UInt256
        let liquidity = liquidity_str
            .and_then(|s| s.parse::<u128>().ok())
            .map(UInt256::from)
            .unwrap_or(UInt256::from(0u128));

        // fees_24h calculation
        // fee is in ppm (e.g. 3000 = 0.3%)
        let fees_24h = if let (Some(vol), Some(f)) = (volume_24h, fee) {
            vol * (f as f64 / 1_000_000.0)
        } else {
            0.0
        };

        snapshots.push(PoolSnapshot::new(
            chain_id as u64,
            address,
            now,
            price.unwrap_or(0.0),
            price_usd.unwrap_or(0.0),
            tvl_usd.unwrap_or(0.0),
            reserve0.unwrap_or(0.0),
            reserve1.unwrap_or(0.0),
            liquidity,
            volume_24h.unwrap_or(0.0),
            swaps_24h.unwrap_or(0) as u64,
            fees_24h,
        ));
    }

    // Send to batch inserter
    live_tx
        .send(IngestMessage::Snapshots(SnapshotMessage {
            pool_snapshots: snapshots.clone(),
            token_snapshots: vec![],
        }))
        .await?;

    // Update last run time in DB
    db.postgres.set_cron_checkpoint(JOB_NAME, now).await?;

    info!(
        "Sent {} pool snapshots to batch inserter in {:?}",
        snapshots.len(),
        start.elapsed()
    );
    Ok(())
}
