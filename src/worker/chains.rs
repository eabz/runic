use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use log::{error, info, warn};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    db::{models::DatabaseChain, IngestMessage},
    worker::ChainWorker,
    Database,
};

/// Represents a running chain indexer
struct RunningChain {
    name: String,
    handle: JoinHandle<()>,
    cancel_token: CancellationToken,
    config: DatabaseChain,
}

/// Manages chain indexers dynamically based on database configuration
///
/// Features:
/// - Loads chains from database at startup
/// - Refreshes chain list every 5 minutes
/// - Starts indexers for newly enabled chains
/// - Gracefully stops indexers for disabled chains
pub struct ChainManager {
    running_chains: HashMap<u64, RunningChain>,
    historical_sender: mpsc::Sender<IngestMessage>,
    live_sender: mpsc::Sender<IngestMessage>,
    db: Arc<Database>,
    hypersync_token: String,
    tip_poll_interval_milliseconds: u64,
}

impl ChainManager {
    /// Create a new ChainManager
    pub fn new(
        db: Arc<Database>,
        hypersync_token: String,
        tip_poll_interval_milliseconds: u64,
        historical_sender: mpsc::Sender<IngestMessage>,
        live_sender: mpsc::Sender<IngestMessage>,
    ) -> Self {
        Self {
            running_chains: HashMap::new(),
            historical_sender,
            live_sender,
            db,
            hypersync_token,
            tip_poll_interval_milliseconds,
        }
    }

    /// Start a chain indexer
    async fn start_chain(&mut self, config: DatabaseChain) -> Result<()> {
        if self.running_chains.contains_key(&config.chain_id) {
            warn!(
                "Chain {} ({}) is already running, skipping",
                config.name, config.chain_id
            );
            return Ok(());
        }

        info!(
            "Starting indexer for chain {} ({})",
            config.name, config.chain_id
        );

        let worker = ChainWorker::new(
            &config,
            self.historical_sender.clone(),
            self.live_sender.clone(),
            self.hypersync_token.clone(),
            self.db.clone(),
            self.tip_poll_interval_milliseconds,
        )
        .await
        .context(format!(
            "Failed to initialize worker for chain {} ({}). Check RPC and Hypersync URLs.",
            config.name, config.chain_id
        ))?;

        info!(
            "Worker initialized successfully for chain {} ({})",
            config.name, config.chain_id
        );

        let cancel_token = CancellationToken::new();
        let worker_token = cancel_token.clone();
        let chain_name = config.name.clone();
        let chain_id = config.chain_id;

        let handle = tokio::spawn(async move {
            if let Err(e) = worker.run(worker_token).await {
                error!("Worker for {} failed: {:#}", chain_name, e);
            }
        });

        self.running_chains.insert(
            chain_id,
            RunningChain {
                name: config.name.clone(),
                handle,
                cancel_token,
                config: config.clone(),
            },
        );

        info!(
            "Chain {} ({}) indexer started successfully and is now running",
            config.name, chain_id
        );

        Ok(())
    }

    /// Stop a chain indexer gracefully
    async fn stop_chain(&mut self, chain_id: u64) {
        if let Some(running) = self.running_chains.remove(&chain_id) {
            info!("Stopping indexer for chain {} ({})", running.name, chain_id);

            // Cancel the token to signal graceful shutdown
            running.cancel_token.cancel();

            // Wait for the handle to complete (with timeout)
            match tokio::time::timeout(Duration::from_secs(10), running.handle).await {
                Ok(_) => {
                    info!(
                        "Indexer for chain {} ({}) stopped gracefully",
                        running.name, chain_id
                    );
                },
                Err(_) => {
                    warn!(
                        "Indexer for chain {} ({}) did not stop within timeout, continuing...",
                        running.name, chain_id
                    );
                },
            }
        } else {
            warn!(
                "Attempted to stop chain {} but it was not running",
                chain_id
            );
        }
    }

    /// Refresh chain configuration from database
    /// Starts new chains, stops disabled chains, restarts chains with changed config
    async fn refresh_chains(&mut self) -> Result<()> {
        let all_chains = self.db.postgres.get_chains().await?;

        // Build sets of what should be running vs what is running
        let enabled_chain_ids: HashMap<u64, DatabaseChain> = all_chains
            .into_iter()
            .filter(|c| c.enabled)
            .map(|c| (c.chain_id, c))
            .collect();

        let running_chain_ids: Vec<u64> = self.running_chains.keys().cloned().collect();

        // Stop chains that are no longer enabled or have changed config
        for chain_id in running_chain_ids {
            if let Some(new_config) = enabled_chain_ids.get(&chain_id) {
                // Chain is enabled, check if config changed
                if let Some(running) = self.running_chains.get(&chain_id) {
                    if running.config != *new_config {
                        info!(
                            "Configuration changed for chain {} ({}), restarting...",
                            new_config.name, chain_id
                        );
                        // Stop the chain - it will be restarted below with new config
                        self.stop_chain(chain_id).await;
                    }
                }
            } else {
                // Chain is no longer enabled
                info!("Chain {} is now disabled, stopping indexer...", chain_id);
                self.stop_chain(chain_id).await;
            }
        }

        // Start chains that should be running but aren't
        // This includes: newly created chains, newly enabled chains, and chains that were restarted due to config changes
        for (chain_id, config) in enabled_chain_ids {
            if !self.running_chains.contains_key(&chain_id) {
                if let Err(e) = self.start_chain(config).await {
                    error!("Failed to start chain {}: {:#}", chain_id, e);
                }
            }
        }

        Ok(())
    }

    /// Run the chain manager main loop
    ///
    /// This will:
    /// 1. Load enabled chains from database and start them
    /// 2. Periodically refresh (every 30 seconds) to detect changes
    /// 3. Stop when cancellation token is triggered
    pub async fn run(mut self, cancellation_token: CancellationToken) -> Result<()> {
        let refresh_interval = Duration::from_secs(30); // 30 seconds - faster response to changes
        let mut last_refresh = std::time::Instant::now();

        // Initial load
        info!("ChainManager: Loading chains from database...");
        self.refresh_chains().await?;

        if self.running_chains.is_empty() {
            warn!("ChainManager: No enabled chains found in database!");
        } else {
            info!(
                "ChainManager: Started {} chain indexer(s)",
                self.running_chains.len()
            );
            for (chain_id, running) in &self.running_chains {
                info!("  - Chain {} ({})", running.name, chain_id);
            }
        }

        // Main loop - refresh periodically
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("ChainManager: Received cancellation signal");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    // Check if it's time to refresh
                    if last_refresh.elapsed() >= refresh_interval {
                        info!("ChainManager: Refreshing chain configuration...");
                        match self.refresh_chains().await {
                            Ok(_) => {
                                info!(
                                    "ChainManager: Refresh complete. {} chain indexer(s) running",
                                    self.running_chains.len()
                                );
                            },
                            Err(e) => {
                                error!("ChainManager: Failed to refresh chains: {:#}", e);
                            }
                        }
                        last_refresh = std::time::Instant::now();
                    }
                }
            }
        }

        // Stop all running chains
        info!("ChainManager: Stopping all chain indexers...");
        let chain_ids: Vec<u64> = self.running_chains.keys().cloned().collect();
        for chain_id in chain_ids {
            self.stop_chain(chain_id).await;
        }

        info!("ChainManager: Shutdown complete");
        Ok(())
    }
}
