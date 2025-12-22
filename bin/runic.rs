use std::sync::Arc;

use anyhow::Context;
use jemallocator::Jemalloc;
use log::{error, info, LevelFilter};
use simple_logger::SimpleLogger;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use runic::{db::IngestMessage, ChainManager, CronScheduler, CronSettings, Database, Settings};

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    // Load configuration
    let settings = Arc::new(
        Settings::new()
            .context("Failed to load config.yaml. Please ensure it exists and is valid")?,
    );

    // Dual channel architecture for ingestion
    // Historical: high-throughput for chain sync (large batches, long wait)
    // Live: low-latency for real-time data (small batches, fast flush)
    let (historical_tx, historical_rx) = mpsc::channel::<IngestMessage>(128);
    let (live_tx, live_rx) = mpsc::channel::<IngestMessage>(128);

    let cancellation_token = CancellationToken::new();

    let (db, historical_handle, live_handle) = Database::new(
        settings.clone(),
        historical_rx,
        live_rx,
        cancellation_token.clone(),
    )
    .await
    .context("Failed to initialize database connection")?;

    let db = Arc::new(db);

    return run_indexer(
        settings,
        db,
        cancellation_token,
        historical_tx,
        live_tx,
        historical_handle,
        live_handle,
    )
    .await;
}

async fn run_indexer(
    settings: Arc<Settings>,
    db: Arc<Database>,
    cancellation_token: CancellationToken,
    historical_tx: mpsc::Sender<IngestMessage>,
    live_tx: mpsc::Sender<IngestMessage>,
    historical_handle: tokio::task::JoinHandle<()>,
    live_handle: tokio::task::JoinHandle<()>,
) -> anyhow::Result<()> {
    // Create chain manager - it will load chains from database and spawn workers
    // Workers route to historical_tx or live_tx based on tip detection
    let chain_manager = ChainManager::new(
        db.clone(),
        settings.indexer.hypersync_bearer_token.clone(),
        settings.indexer.tip_poll_interval_milliseconds,
        historical_tx.clone(),
        live_tx.clone(),
    );

    let chain_manager_token = cancellation_token.child_token();
    let chain_manager_handle = tokio::spawn(async move {
        if let Err(e) = chain_manager.run(chain_manager_token).await {
            error!("Chain manager failed: {:#}", e);
        }
    });

    info!("Chain manager started - chains will be loaded from database");

    // Create and spawn cron scheduler for background jobs
    // (24h stats updates, price changes, MV refresh, snapshots)
    let cron_scheduler = CronScheduler::new(db.clone(), live_tx.clone(), CronSettings::default());

    let cron_token = cancellation_token.child_token();
    let cron_handle = tokio::spawn(async move {
        if let Err(e) = cron_scheduler.run(cron_token).await {
            error!("Cron scheduler failed: {:#}", e);
        }
    });

    info!("Cron scheduler started - background jobs will run periodically");

    #[cfg(unix)]
    let mut sigterm_stream = {
        use tokio::signal::unix::{signal, SignalKind};
        signal(SignalKind::terminate()).context("Failed to install SIGTERM handler")?
    };

    // Set up graceful shutdown signal handler
    info!("Indexer running. Press Ctrl+C to stop.");

    #[cfg(unix)]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal (Ctrl+C), exiting gracefully...");
            },
            _ = sigterm_stream.recv() => {
                info!("Received SIGTERM, exiting gracefully...");
            },
        };
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal (Ctrl+C), exiting gracefully...");
            },
        };
    }

    // Cancel all running tasks
    info!("Finishing all tasks...");

    cancellation_token.cancel();

    // Wait for chain manager to stop all indexers
    info!("Waiting for chain manager to stop...");
    let _ = chain_manager_handle.await;

    // Wait for cron scheduler to stop
    info!("Waiting for cron scheduler to stop...");
    let _ = cron_handle.await;

    // Shutdown both ingestors
    info!("Shutting down batch ingestors...");
    let _ = historical_tx.send(IngestMessage::Shutdown).await;
    let _ = live_tx.send(IngestMessage::Shutdown).await;

    // Gracefully await both ingestors
    let _ = historical_handle.await;
    let _ = live_handle.await;

    info!("All ingestors stopped");
    Ok(())
}
