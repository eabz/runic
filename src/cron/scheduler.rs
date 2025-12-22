//! Cron scheduler for periodic background tasks.
//!
//! Runs jobs like:
//! - Updating 24h stats (volume, swaps) from ClickHouse to PostgreSQL
//! - Updating price changes from ClickHouse candles
//! - Refreshing PostgreSQL materialized views
//! - Taking pool and token snapshots to ClickHouse

use std::sync::Arc;

use anyhow::Result;
use log::{error, info};
use tokio::sync::mpsc;
use tokio_cron_scheduler::{Job, JobScheduler};
use tokio_util::sync::CancellationToken;

use crate::db::{Database, IngestMessage};

use super::jobs;

/// Cron scheduler that manages periodic background jobs.
pub struct CronScheduler {
    db: Arc<Database>,
    live_tx: mpsc::Sender<IngestMessage>,
    settings: Arc<CronSettings>,
}

/// Configuration for cron job intervals
#[derive(Debug, Clone)]
pub struct CronSettings {
    /// Interval for updating 24h stats (volume, swaps) - default 5 minutes
    pub update_stats_interval_secs: u64,
    /// Interval for refreshing PostgreSQL materialized views - default 5 minutes
    pub refresh_mv_interval_secs: u64,
    /// Interval for taking pool snapshots - default 1 hour
    pub pool_snapshot_interval_secs: u64,
    /// Interval for taking token snapshots - default 1 day
    pub token_snapshot_interval_secs: u64,
}

impl Default for CronSettings {
    fn default() -> Self {
        Self {
            update_stats_interval_secs: 900,    // 15 minutes
            refresh_mv_interval_secs: 300,      // 5 minutes
            pool_snapshot_interval_secs: 3600,  // 1 hour
            token_snapshot_interval_secs: 3600, // 1 hour
        }
    }
}

impl CronScheduler {
    pub fn new(
        db: Arc<Database>,
        live_tx: mpsc::Sender<IngestMessage>,
        settings: CronSettings,
    ) -> Self {
        Self {
            db,
            live_tx,
            settings: Arc::new(settings),
        }
    }

    /// Starts the cron scheduler and runs until cancellation.
    pub async fn run(&self, cancellation_token: CancellationToken) -> Result<()> {
        let mut scheduler = JobScheduler::new().await?;

        // Register all jobs
        self.register_update_24h_stats_job(&scheduler).await?;
        self.register_update_price_changes_job(&scheduler).await?;
        self.register_refresh_mv_job(&scheduler).await?;
        self.register_pool_snapshots_job(&scheduler).await?;
        self.register_token_snapshots_job(&scheduler).await?;

        // Start the scheduler
        scheduler.start().await?;
        info!("Cron scheduler started with {} jobs", 5);

        // Wait for cancellation
        cancellation_token.cancelled().await;
        info!("Cron scheduler shutting down...");

        scheduler.shutdown().await?;
        Ok(())
    }

    async fn register_update_24h_stats_job(&self, scheduler: &JobScheduler) -> Result<()> {
        let db = self.db.clone();
        let interval = self.settings.update_stats_interval_secs;

        let job = Job::new_repeated_async(
            std::time::Duration::from_secs(interval),
            move |_uuid, _lock| {
                let db = db.clone();
                Box::pin(async move {
                    if let Err(e) = jobs::update_24h_stats::run(&db).await {
                        error!("Failed to update 24h stats: {:#}", e);
                    }
                })
            },
        )?;

        scheduler.add(job).await?;
        info!("Registered update_24h_stats job (every {}s)", interval);
        Ok(())
    }

    async fn register_update_price_changes_job(&self, scheduler: &JobScheduler) -> Result<()> {
        let db = self.db.clone();
        let interval = self.settings.update_stats_interval_secs;

        let job = Job::new_repeated_async(
            std::time::Duration::from_secs(interval),
            move |_uuid, _lock| {
                let db = db.clone();
                Box::pin(async move {
                    if let Err(e) = jobs::update_price_changes::run(&db).await {
                        error!("Failed to update price changes: {:#}", e);
                    }
                })
            },
        )?;

        scheduler.add(job).await?;
        info!("Registered update_price_changes job (every {}s)", interval);
        Ok(())
    }

    async fn register_refresh_mv_job(&self, scheduler: &JobScheduler) -> Result<()> {
        let db = self.db.clone();
        let interval = self.settings.refresh_mv_interval_secs;

        let job = Job::new_repeated_async(
            std::time::Duration::from_secs(interval),
            move |_uuid, _lock| {
                let db = db.clone();
                Box::pin(async move {
                    if let Err(e) = jobs::refresh_materialized_views::run(&db).await {
                        error!("Failed to refresh materialized views: {:#}", e);
                    }
                })
            },
        )?;

        scheduler.add(job).await?;
        info!(
            "Registered refresh_materialized_views job (every {}s)",
            interval
        );
        Ok(())
    }

    async fn register_pool_snapshots_job(&self, scheduler: &JobScheduler) -> Result<()> {
        let db = self.db.clone();
        let live_tx = self.live_tx.clone();
        let interval = self.settings.pool_snapshot_interval_secs;

        let job = Job::new_repeated_async(
            std::time::Duration::from_secs(interval),
            move |_uuid, _lock| {
                let db = db.clone();
                let live_tx = live_tx.clone();
                Box::pin(async move {
                    if let Err(e) = jobs::pool_snapshots::run(&db, &live_tx).await {
                        error!("Failed to take pool snapshots: {:#}", e);
                    }
                })
            },
        )?;

        scheduler.add(job).await?;
        info!("Registered pool_snapshots job (every {}s)", interval);
        Ok(())
    }

    async fn register_token_snapshots_job(&self, scheduler: &JobScheduler) -> Result<()> {
        let db = self.db.clone();
        let live_tx = self.live_tx.clone();
        let interval = self.settings.token_snapshot_interval_secs;

        let job = Job::new_repeated_async(
            std::time::Duration::from_secs(interval),
            move |_uuid, _lock| {
                let db = db.clone();
                let live_tx = live_tx.clone();
                Box::pin(async move {
                    if let Err(e) = jobs::token_snapshots::run(&db, &live_tx).await {
                        error!("Failed to take token snapshots: {:#}", e);
                    }
                })
            },
        )?;

        scheduler.add(job).await?;
        info!("Registered token_snapshots job (every {}s)", interval);
        Ok(())
    }
}
