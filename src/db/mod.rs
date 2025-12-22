use std::sync::Arc;

use log::{error, info};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::config::Settings;

pub mod clickhouse;
pub mod models;
pub mod postgres;

pub use clickhouse::{BatchDataMessage, ClickhouseClient, IngestMessage, SnapshotMessage};
pub use postgres::PostgresClient;

/// Combined database client managing ClickHouse and PostgreSQL connections.
///
/// ClickHouse is used for high-volume time-series data (events, transfers, snapshots).
/// PostgreSQL is used for relational data (chains, tokens, pools, checkpoints).
#[derive(Clone)]
pub struct Database {
    pub clickhouse: Arc<ClickhouseClient>,
    pub postgres: Arc<PostgresClient>,
}

impl Database {
    pub async fn new(
        settings: Arc<Settings>,
        historical_rx: mpsc::Receiver<IngestMessage>,
        live_rx: mpsc::Receiver<IngestMessage>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<(Self, JoinHandle<()>, JoinHandle<()>)> {
        let (clickhouse, historical_ingestor, live_ingestor) =
            ClickhouseClient::new(settings.clickhouse.clone(), historical_rx, live_rx).await?;

        let postgres = PostgresClient::new(settings.postgres.clone()).await?;

        // Run migrations
        clickhouse.migrate().await?;
        postgres.migrate().await?;

        let clickhouse = Arc::new(clickhouse);

        // Spawn historical ingestor (high-throughput for chain sync)
        let historical_token = cancellation_token.child_token();
        let historical_handle = tokio::spawn(async move {
            if let Err(e) = historical_ingestor.run(historical_token).await {
                error!("[HISTORICAL] Batch inserter failed: {:#}", e);
            }
        });

        // Spawn live ingestor (low-latency for real-time)
        let live_token = cancellation_token.child_token();
        let live_handle = tokio::spawn(async move {
            if let Err(e) = live_ingestor.run(live_token).await {
                error!("[LIVE] Batch inserter failed: {:#}", e);
            }
        });

        info!("Dual batch ingestors spawned (historical + live)");

        Ok((
            Self {
                clickhouse,
                postgres: Arc::new(postgres),
            },
            historical_handle,
            live_handle,
        ))
    }
}
