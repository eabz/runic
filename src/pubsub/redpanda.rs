//! Redpanda (Kafka-compatible) publisher implementation.
//!
//! Publishes blockchain events to Redpanda topics for external consumers.
//! Uses fire-and-forget semantics to avoid blocking the indexer.

use std::time::Duration;

use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::Serialize;

use crate::config::RedpandaSettings;
use crate::db::clickhouse::ops::BatchDataMessage;

/// Redpanda publisher for streaming blockchain events.
///
/// Publishes events, transfers, and new pools to separate topics
/// with chain_id-based partitioning for parallel consumption.
pub struct RedpandaPublisher {
    producer: FutureProducer,
    topic_prefix: String,
}

impl RedpandaPublisher {
    /// Create a new Redpanda publisher.
    ///
    /// Returns None if Redpanda is disabled in settings or connection fails.
    pub fn new(settings: &RedpandaSettings) -> Option<Self> {
        if !settings.enabled {
            info!("Redpanda publishing is disabled");
            return None;
        }

        info!("Connecting to Redpanda brokers: {}", settings.brokers);

        let producer: FutureProducer = match ClientConfig::new()
            .set("bootstrap.servers", &settings.brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.kbytes", "1048576") // 1GB buffer
            .set("batch.num.messages", "10000")
            .set("linger.ms", "5") // Small delay to batch messages
            .create()
        {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to create Redpanda producer: {}", e);
                return None;
            },
        };

        info!(
            "Redpanda publisher initialized with topic prefix: {}",
            settings.topic_prefix
        );

        Some(Self {
            producer,
            topic_prefix: settings.topic_prefix.clone(),
        })
    }

    /// Publish a batch of data to Redpanda topics.
    ///
    /// This is fire-and-forget: errors are logged but don't stop the indexer.
    /// Each data type goes to its own topic with chain_id suffix.
    pub async fn publish_batch(&self, chain_id: u64, batch: &BatchDataMessage) {
        let events_topic = format!("{}.events.{}", self.topic_prefix, chain_id);
        let new_pools_topic = format!("{}.new_pools.{}", self.topic_prefix, chain_id);
        let pool_states_topic = format!("{}.pool_states.{}", self.topic_prefix, chain_id);
        let token_states_topic = format!("{}.token_states.{}", self.topic_prefix, chain_id);

        // Publish events
        for event in &batch.events {
            self.publish_message(&events_topic, &event.pool_address, event)
                .await;
        }

        // Publish new pools (pool creation announcements)
        for pool in &batch.new_pools {
            self.publish_message(&new_pools_topic, &pool.pool_address, pool)
                .await;
        }

        // Publish updated pool states (reserves, prices, TVL)
        for pool in &batch.pools {
            self.publish_message(&pool_states_topic, &pool.address, pool)
                .await;
        }

        // Publish updated token states (prices)
        for token in &batch.tokens {
            self.publish_message(&token_states_topic, &token.address, token)
                .await;
        }
    }

    /// Publish a single message to a topic.
    async fn publish_message<T: Serialize>(&self, topic: &str, key: &str, value: &T) {
        let payload = match serde_json::to_string(value) {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to serialize message for {}: {}", topic, e);
                return;
            },
        };

        let record = FutureRecord::to(topic).key(key).payload(&payload);

        // Fire-and-forget with short timeout
        match self.producer.send(record, Duration::from_millis(100)).await {
            Ok(_) => {},
            Err((e, _)) => {
                // Log but don't fail - this is best-effort streaming
                warn!("Failed to send message to {}: {}", topic, e);
            },
        }
    }

    /// Flush any pending messages (call on shutdown).
    pub fn flush(&self) {
        self.producer.flush(Duration::from_secs(5)).ok();
    }
}

impl Drop for RedpandaPublisher {
    fn drop(&mut self) {
        self.flush();
    }
}
