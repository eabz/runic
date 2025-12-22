use config::{Config, ConfigError, File};
use serde::Deserialize;

/// ClickHouse database connection and batching configuration.
///
/// Controls connection settings and dual-channel batch ingestion:
/// - Historical: High-throughput batching for chain sync (large batches, longer waits)
/// - Live: Low-latency batching for real-time data (small batches, short waits)
#[derive(Debug, Deserialize, Clone)]
pub struct ClickHouseSettings {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    // Historical ingestor (high-throughput sync)
    #[serde(default = "default_historical_batch_size")]
    pub historical_batch_size: usize,
    #[serde(default = "default_historical_max_wait_secs")]
    pub historical_max_wait_secs: usize,
    // Live ingestor (low-latency real-time)
    #[serde(default = "default_live_batch_size")]
    pub live_batch_size: usize,
    #[serde(default = "default_live_max_wait_ms")]
    pub live_max_wait_ms: usize,
}

fn default_historical_batch_size() -> usize {
    5_000_000 // Increased to 5M for high throughput
}

fn default_historical_max_wait_secs() -> usize {
    10
}

fn default_live_batch_size() -> usize {
    1_000
}

fn default_live_max_wait_ms() -> usize {
    100
}

/// PostgreSQL database connection configuration.
///
/// Used for storing:
/// - Chain configurations
/// - Token and pool metadata
/// - Sync checkpoints
/// - Aggregated statistics
#[derive(Debug, Deserialize, Clone)]
pub struct PostgresSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
}

fn default_pool_size() -> usize {
    16
}

/// HyperSync indexer configuration.
///
/// HyperSync provides high-performance blockchain data streaming
/// with sub-second latency for real-time indexing.
#[derive(Debug, Deserialize, Clone)]
pub struct IndexerSettings {
    pub hypersync_bearer_token: String,
    #[serde(default = "default_tip_poll_interval")]
    pub tip_poll_interval_milliseconds: u64,
}

/// Redpanda (Kafka-compatible) pub/sub configuration.
///
/// When enabled, streams real-time blockchain events to Redpanda topics
/// for external consumers. Only publishes data when indexer is at chain tip.
#[derive(Debug, Deserialize, Clone)]
pub struct RedpandaSettings {
    /// Enable/disable Redpanda publishing
    #[serde(default)]
    pub enabled: bool,
    /// Comma-separated list of broker addresses (e.g., "localhost:9092")
    #[serde(default = "default_redpanda_brokers")]
    pub brokers: String,
    /// Topic name prefix (topics: {prefix}.events.{chain_id}, etc.)
    #[serde(default = "default_redpanda_topic_prefix")]
    pub topic_prefix: String,
}

fn default_redpanda_brokers() -> String {
    "localhost:9092".to_string()
}

fn default_redpanda_topic_prefix() -> String {
    "runic".to_string()
}

/// Root application configuration.
///
/// Loaded from `config.yaml` at startup.
/// Contains all subsystem configurations for databases and indexer.
#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub clickhouse: ClickHouseSettings,
    pub postgres: PostgresSettings,
    pub indexer: IndexerSettings,
    #[serde(default)]
    pub redpanda: Option<RedpandaSettings>,
}

fn default_tip_poll_interval() -> u64 {
    200
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(File::with_name("config"))
            .build()?;

        let settings: Settings = s.try_deserialize()?;

        Ok(settings)
    }
}
