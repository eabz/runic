//! Job to refresh PostgreSQL materialized views.
//!
//! Calls the refresh_summary_views() function to update mv_pool_summary and mv_token_summary.

use anyhow::Result;
use log::info;

use crate::db::Database;

/// Refreshes PostgreSQL materialized views for dashboard performance.
///
/// Calls:
/// - indexer.refresh_summary_views() which refreshes:
///   - mv_pool_summary (pool counts, TVL aggregates)
///   - mv_token_summary (token counts, market cap aggregates)
pub async fn run(db: &Database) -> Result<()> {
    info!("Starting refresh_materialized_views job...");

    let start = std::time::Instant::now();

    let pg = db.postgres.pool.get().await?;

    // Call the refresh function we created in the schema
    pg.execute("SELECT indexer.refresh_summary_views()", &[])
        .await?;

    info!(
        "Completed refresh_materialized_views job in {:?}",
        start.elapsed()
    );
    Ok(())
}
