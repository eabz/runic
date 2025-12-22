use anyhow::Context;
use log::info;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::db::{
    clickhouse::client::BatchIngestor,
    models::{Event, NewPool, Pool, PoolSnapshot, Token, TokenSearch, TokenSnapshot, Transfer},
};

/// Batch of data from the indexer to be inserted into ClickHouse
/// Also used for Redpanda pub/sub streaming of live data at chain tip
#[derive(Debug, Clone)]
pub struct BatchDataMessage {
    /// Chain ID for this batch (used for Redpanda topic partitioning)
    pub chain_id: i64,
    pub events: Vec<Event>,
    pub transfers: Vec<Transfer>,
    pub new_pools: Vec<NewPool>,
    /// Updated pool states (for Redpanda, not stored in ClickHouse)
    pub pools: Vec<Pool>,
    /// Updated token states (for Redpanda, not stored in ClickHouse)
    pub tokens: Vec<Token>,
}

/// Snapshot data populated by background jobs (not real-time indexing)
#[derive(Debug, Clone)]
pub struct SnapshotMessage {
    pub pool_snapshots: Vec<PoolSnapshot>,
    pub token_snapshots: Vec<TokenSnapshot>,
}

/// Token search entries (populated when new tokens are discovered)
#[derive(Debug, Clone)]
pub struct TokenSearchMessage {
    pub tokens: Vec<TokenSearch>,
}

pub enum IngestMessage {
    /// Real-time batch data from indexer
    BatchData(BatchDataMessage),
    /// Periodic snapshots from background jobs
    Snapshots(SnapshotMessage),
    /// Token search index updates
    TokenSearch(TokenSearchMessage),
    /// Shutdown signal
    Shutdown,
}

impl BatchIngestor {
    pub async fn run(mut self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        loop {
            // Calculate time until next commit check based on inserter periods
            // Use the minimum time_left across all inserters
            let sleep_duration = self.min_time_left().unwrap_or(Duration::from_secs(1));

            tokio::select! {
                biased; // Check cancellation first

                _ = cancellation_token.cancelled() => {
                    info!("[{}] Batch inserter received cancellation signal", self.label);
                    self.end_all().await?;
                    break;
                }

                msg = self.receiver.recv() => {
                    match msg {
                        Some(IngestMessage::BatchData(batch)) => {
                            // Write events to inserter
                            for event in &batch.events {
                                self.event_inserter.write(event).await
                                    .context("Failed to write event")?;
                            }

                            // Write transfers to inserter
                            for transfer in &batch.transfers {
                                self.transfer_inserter.write(transfer).await
                                    .context("Failed to write transfer")?;
                            }

                            // Write new pools to inserter
                            for pool in &batch.new_pools {
                                self.new_pool_inserter.write(pool).await
                                    .context("Failed to write new pool")?;
                            }

                            // Publish to Redpanda if enabled (fire-and-forget)
                            if let Some(ref publisher) = self.redpanda_publisher {
                                publisher.publish_batch(batch.chain_id, &batch).await;
                            }

                            // Commit checks thresholds and flushes if needed
                            self.commit_all().await?;
                        }
                        Some(IngestMessage::Snapshots(snapshots)) => {
                            for snapshot in &snapshots.pool_snapshots {
                                self.pool_snapshot_inserter.write(snapshot).await
                                    .context("Failed to write pool snapshot")?;
                            }

                            for snapshot in &snapshots.token_snapshots {
                                self.token_snapshot_inserter.write(snapshot).await
                                    .context("Failed to write token snapshot")?;
                            }

                            self.commit_all().await?;
                        }
                        Some(IngestMessage::TokenSearch(search)) => {
                            for token in &search.tokens {
                                self.token_search_inserter.write(token).await
                                    .context("Failed to write token search")?;
                            }

                            self.commit_all().await?;
                        }
                        Some(IngestMessage::Shutdown) => {
                            info!("[{}] Batch inserter received shutdown signal", self.label);
                            self.end_all().await?;
                            break;
                        }
                        None => {
                            info!("[{}] Batch inserter channel closed", self.label);
                            self.end_all().await?;
                            break;
                        }
                    }
                }

                // Periodic commit check - the inserters handle time-based flushing internally
                _ = tokio::time::sleep(sleep_duration) => {
                    self.commit_all().await?;
                }
            }
        }

        info!("[{}] Batch inserter stopped", self.label);
        Ok(())
    }

    /// Get the minimum time_left across all inserters
    fn min_time_left(&mut self) -> Option<Duration> {
        [
            self.event_inserter.time_left(),
            self.transfer_inserter.time_left(),
            self.new_pool_inserter.time_left(),
            self.pool_snapshot_inserter.time_left(),
            self.token_snapshot_inserter.time_left(),
            self.token_search_inserter.time_left(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Commit all inserters - checks thresholds and flushes if needed
    async fn commit_all(&mut self) -> anyhow::Result<()> {
        let event_stats = self.event_inserter.commit().await?;
        let transfer_stats = self.transfer_inserter.commit().await?;
        let new_pool_stats = self.new_pool_inserter.commit().await?;
        let pool_snapshot_stats = self.pool_snapshot_inserter.commit().await?;
        let token_snapshot_stats = self.token_snapshot_inserter.commit().await?;
        let token_search_stats = self.token_search_inserter.commit().await?;

        // Log only if any data was actually committed (transactions > 0)
        let total_rows = event_stats.rows
            + transfer_stats.rows
            + new_pool_stats.rows
            + pool_snapshot_stats.rows
            + token_snapshot_stats.rows
            + token_search_stats.rows;

        let total_transactions = event_stats.transactions
            + transfer_stats.transactions
            + new_pool_stats.transactions
            + pool_snapshot_stats.transactions
            + token_snapshot_stats.transactions
            + token_search_stats.transactions;

        if total_transactions > 0 {
            let mut parts = Vec::new();
            if event_stats.rows > 0 {
                parts.push(format!("Events:{}", event_stats.rows));
            }
            if transfer_stats.rows > 0 {
                parts.push(format!("Transfers:{}", transfer_stats.rows));
            }
            if new_pool_stats.rows > 0 {
                parts.push(format!("NewPools:{}", new_pool_stats.rows));
            }
            if pool_snapshot_stats.rows > 0 {
                parts.push(format!("PoolSnaps:{}", pool_snapshot_stats.rows));
            }
            if token_snapshot_stats.rows > 0 {
                parts.push(format!("TokenSnaps:{}", token_snapshot_stats.rows));
            }
            if token_search_stats.rows > 0 {
                parts.push(format!("TokenSearch:{}", token_search_stats.rows));
            }

            info!(
                "[{}] Committed {} rows in {} txns [{}]",
                self.label,
                total_rows,
                total_transactions,
                parts.join(" ")
            );
        }

        Ok(())
    }

    /// Force end all inserters - used on shutdown
    async fn end_all(&mut self) -> anyhow::Result<()> {
        // Force commit any remaining data
        let _ = self.event_inserter.force_commit().await;
        let _ = self.transfer_inserter.force_commit().await;
        let _ = self.new_pool_inserter.force_commit().await;
        let _ = self.pool_snapshot_inserter.force_commit().await;
        let _ = self.token_snapshot_inserter.force_commit().await;
        let _ = self.token_search_inserter.force_commit().await;

        info!("[{}] All inserters flushed", self.label);
        Ok(())
    }
}
