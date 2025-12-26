use std::time::Duration;

use anyhow::Context;
use clickhouse::{inserter::Inserter, Client};
use log::info;
use tokio::sync::mpsc;

use crate::{
    config::ClickHouseSettings,
    db::{
        clickhouse::ops::IngestMessage,
        models::{Event, NewPool, PoolSnapshot, SupplyEvent, TokenSnapshot},
    },
};

pub struct ClickhouseClient {
    pub client: Client,
}

/// Configuration for creating inserters with specific thresholds
#[derive(Clone)]
pub struct InserterConfig {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub period: Duration,
}

pub struct BatchIngestor {
    pub label: &'static str,
    pub client: Client,
    pub receiver: mpsc::Receiver<IngestMessage>,
    pub config: InserterConfig,

    // Inserters for each data type - use clickhouse-rs built-in batching
    pub event_inserter: Inserter<Event>,
    pub new_pool_inserter: Inserter<NewPool>,
    pub pool_snapshot_inserter: Inserter<PoolSnapshot>,
    pub token_snapshot_inserter: Inserter<TokenSnapshot>,
    pub supply_event_inserter: Inserter<SupplyEvent>,

    // Optional Redpanda publisher for live streaming (only used by LIVE ingestor)
    pub redpanda_publisher: Option<crate::pubsub::RedpandaPublisher>,
}

impl BatchIngestor {
    /// Create a new inserter with the given configuration
    fn create_inserter<T: clickhouse::Row>(
        client: &Client,
        table: &str,
        config: &InserterConfig,
    ) -> Inserter<T> {
        client
            .inserter::<T>(table)
            .with_max_rows(config.max_rows)
            .with_max_bytes(config.max_bytes)
            .with_period(Some(config.period))
            .with_period_bias(0.1) // 10% bias to avoid synchronized flushes
    }

    /// Recreate an inserter (used after errors or forced commits)
    pub fn recreate_event_inserter(&mut self) {
        self.event_inserter = Self::create_inserter(&self.client, "events", &self.config);
    }

    pub fn recreate_new_pool_inserter(&mut self) {
        self.new_pool_inserter = Self::create_inserter(&self.client, "new_pools", &self.config);
    }

    pub fn recreate_pool_snapshot_inserter(&mut self) {
        self.pool_snapshot_inserter =
            Self::create_inserter(&self.client, "pool_snapshots", &self.config);
    }

    pub fn recreate_token_snapshot_inserter(&mut self) {
        self.token_snapshot_inserter =
            Self::create_inserter(&self.client, "token_snapshots", &self.config);
    }

    pub fn recreate_supply_event_inserter(&mut self) {
        self.supply_event_inserter =
            Self::create_inserter(&self.client, "supply_events", &self.config);
    }
}

impl ClickhouseClient {
    pub async fn new(
        settings: ClickHouseSettings,
        historical_rx: mpsc::Receiver<IngestMessage>,
        live_rx: mpsc::Receiver<IngestMessage>,
    ) -> anyhow::Result<(Self, BatchIngestor, BatchIngestor)> {
        info!("Connecting to ClickHouse");

        let client = Client::default()
            .with_url(settings.url.clone())
            .with_user(settings.user.clone())
            .with_password(settings.password.clone())
            .with_database(settings.database.clone())
            .with_validation(false); // Disable schema validation for 5-10% performance boost

        // Test connection with retry logic
        let mut retries = 0;
        let max_retries = 3;
        #[allow(unused_assignments)]
        let mut last_error: Option<String> = None;

        loop {
            match client.query("SELECT 1").fetch_one::<u8>().await {
                Ok(_) => {
                    info!("Successfully connected to ClickHouse");
                    break;
                },
                Err(e) => {
                    let error_msg = e.to_string();
                    last_error = Some(error_msg.clone());
                    retries += 1;

                    if retries >= max_retries {
                        return Err(anyhow::anyhow!(
                            "Failed to connect to ClickHouse after {} attempts: {}",
                            max_retries,
                            last_error.unwrap_or_else(|| "Unknown error".to_string())
                        ));
                    }

                    let delay = std::time::Duration::from_millis(100 * 2_u64.pow(retries));
                    log::warn!(
                        "Failed to connect to ClickHouse (attempt {}/{}), retrying in {:?}... Error: {}",
                        retries,
                        max_retries,
                        delay,
                        error_msg
                    );
                    tokio::time::sleep(delay).await;
                },
            }
        }

        // Historical ingestor: high-throughput for chain sync
        // Use byte-based limits for memory-aware batching
        let historical_config = InserterConfig {
            max_rows: settings.historical_batch_size as u64,
            max_bytes: 500_000_000, // 500MB max per batch for high throughput
            period: Duration::from_secs(settings.historical_max_wait_secs as u64),
        };

        let historical_ingestor = BatchIngestor {
            label: "HISTORICAL",
            client: client.clone(),
            receiver: historical_rx,
            event_inserter: BatchIngestor::create_inserter(&client, "events", &historical_config),
            new_pool_inserter: BatchIngestor::create_inserter(
                &client,
                "new_pools",
                &historical_config,
            ),
            pool_snapshot_inserter: BatchIngestor::create_inserter(
                &client,
                "pool_snapshots",
                &historical_config,
            ),
            token_snapshot_inserter: BatchIngestor::create_inserter(
                &client,
                "token_snapshots",
                &historical_config,
            ),
            supply_event_inserter: BatchIngestor::create_inserter(
                &client,
                "supply_events",
                &historical_config,
            ),
            config: historical_config,
            redpanda_publisher: None, // Historical ingestor doesn't publish to Redpanda
        };

        // Live ingestor: low-latency for real-time data
        // Use unlimited bytes so time (100ms) or row count drives flushes
        let live_config = InserterConfig {
            max_rows: settings.live_batch_size as u64,
            max_bytes: u64::MAX, // Unlimited - let time/row count trigger flushes
            period: Duration::from_millis(settings.live_max_wait_ms as u64),
        };

        let live_ingestor = BatchIngestor {
            label: "LIVE",
            client: client.clone(),
            receiver: live_rx,
            event_inserter: BatchIngestor::create_inserter(&client, "events", &live_config),
            new_pool_inserter: BatchIngestor::create_inserter(&client, "new_pools", &live_config),
            pool_snapshot_inserter: BatchIngestor::create_inserter(
                &client,
                "pool_snapshots",
                &live_config,
            ),
            token_snapshot_inserter: BatchIngestor::create_inserter(
                &client,
                "token_snapshots",
                &live_config,
            ),
            supply_event_inserter: BatchIngestor::create_inserter(
                &client,
                "supply_events",
                &live_config,
            ),
            config: live_config,
            redpanda_publisher: None, // Will be set by caller if Redpanda is enabled
        };

        info!(
            "Created dual ingestors - Historical: {}rows/{}s, Live: {}rows/{}ms",
            settings.historical_batch_size,
            settings.historical_max_wait_secs,
            settings.live_batch_size,
            settings.live_max_wait_ms
        );

        Ok((
            Self {
                client,
            },
            historical_ingestor,
            live_ingestor,
        ))
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        info!("Running ClickHouse migrations");
        let schema = tokio::fs::read_to_string("schema/clickhouse.sql")
            .await
            .context("Failed to read schema/clickhouse.sql")?;

        for statement in schema.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }
            self.client
                .query(stmt)
                .execute()
                .await
                .with_context(|| format!("Failed to execute migration statement: {}", stmt))?;
        }

        info!("ClickHouse migrations completed successfully");
        Ok(())
    }

    /// Health check - verify connection is still alive
    pub async fn health_check(&self) -> anyhow::Result<()> {
        self.client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .context("ClickHouse health check failed")?;
        Ok(())
    }
}
