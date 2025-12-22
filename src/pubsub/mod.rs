//! Pub/Sub messaging module for real-time event streaming.
//!
//! Provides Redpanda (Kafka-compatible) integration for publishing
//! blockchain events when the indexer is at chain tip.

mod redpanda;

pub use redpanda::RedpandaPublisher;
