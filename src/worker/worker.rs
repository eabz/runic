use alloy::{primitives::U256, sol_types::SolEvent};
use anyhow::Context;
use chrono::Utc;
use hypersync_client::{
    net_types::{BlockField, LogField, LogFilter, Query},
    Client, ClientConfig, SerializationFormat, StreamConfig,
};
use log::{info, warn};
use rustc_hash::FxHashMap;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    abis::{erc20, v2, v3, v4},
    db::{
        clickhouse::ops::BatchDataMessage,
        models::{
            ChainTokens, DatabaseChain, Event, NativeTokenPrice, NewPool, Pool, SupplyEvent,
            SyncCheckpoint,
        },
        IngestMessage,
    },
    utils::{compute_v4_pool_id, compute_v4_pool_id_from_stored, hex_encode},
    worker::{
        parser::{self, ParsedLog},
        price_resolver::PriceResolver,
        token_fetcher::TokenFetcher,
    },
    Database,
};

/// Interval for logging progress updates (10 seconds)
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Timeout for receiving data from HyperSync stream (5 minutes)
/// If no data is received within this time, reconnect the stream
const STREAM_RECV_TIMEOUT: Duration = Duration::from_secs(300);

/// Main blockchain indexer worker for a single chain.
///
/// Streams blockchain events from HyperSync and processes them in batches:
/// - Parses logs for pool creation, swaps, mints, burns, and transfers
/// - Fetches token metadata via multicall
/// - Updates pool states and calculates USD prices
/// - Sends processed data to ClickHouse and PostgreSQL via channels
pub struct ChainWorker {
    historical_sender: mpsc::Sender<IngestMessage>,
    live_sender: mpsc::Sender<IngestMessage>,
    chain_id: u64,
    client: Arc<Client>,
    db: Arc<Database>,
    filters: LogFilter,
    chain_tokens: Arc<ChainTokens>,
    token_fetcher: TokenFetcher,
    tip_poll_interval: Duration,
    factories: Vec<String>,
}

/// Mutable state tracked during batch processing.
///
/// Maintains cross-batch state like native token price that persists
/// across multiple event batches within a single indexer run.
struct BatchState {
    native_token_price: NativeTokenPrice,
}

impl ChainWorker {
    pub async fn new(
        config: &DatabaseChain,
        historical_sender: mpsc::Sender<IngestMessage>,
        live_sender: mpsc::Sender<IngestMessage>,
        hypersync_token: String,
        db: Arc<Database>,
        tip_poll_interval_milliseconds: u64,
    ) -> anyhow::Result<Self> {
        let url = config
            .hypersync_url
            .parse()
            .context("Invalid HyperSync URL")?;

        let client_config = ClientConfig {
            serialization_format: SerializationFormat::CapnProto {
                should_cache_queries: false,
            },
            http_req_timeout_millis: 120_000,
            url,
            api_token: hypersync_token,
            max_num_retries: 5,
            ..Default::default()
        };

        let client =
            Arc::new(Client::new(client_config).context("Failed to create HyperSync client")?);

        let token_fetcher =
            TokenFetcher::new(config.rpc_url.clone(), config.chain_id as i64, db.clone());

        let chain_tokens = ChainTokens::new(
            config.native_token_address.clone(),
            config.stable_token_address.clone(),
            config.major_tokens.clone(),
            config.stablecoins.clone(),
            config.stable_pool_address.clone(),
        );

        let worker = Self {
            historical_sender,
            live_sender,
            chain_id: config.chain_id,
            client,
            db: db.clone(),
            filters: LogFilter::all().and_topic0([
                erc20::Transfer::SIGNATURE_HASH.0,
                erc20::Deposit::SIGNATURE_HASH.0,
                erc20::Withdrawal::SIGNATURE_HASH.0,
                v2::PairCreated::SIGNATURE_HASH.0,
                v3::PoolCreated::SIGNATURE_HASH.0,
                v3::Initialize::SIGNATURE_HASH.0,
                v4::Initialize::SIGNATURE_HASH.0,
                v2::Mint::SIGNATURE_HASH.0,
                v3::Mint::SIGNATURE_HASH.0,
                v2::Burn::SIGNATURE_HASH.0,
                v3::Burn::SIGNATURE_HASH.0,
                v2::Sync::SIGNATURE_HASH.0,
                v3::Collect::SIGNATURE_HASH.0,
                v4::ModifyLiquidity::SIGNATURE_HASH.0,
                v2::Swap::SIGNATURE_HASH.0,
                v3::Swap::SIGNATURE_HASH.0,
                v4::Swap::SIGNATURE_HASH.0,
            ])?,
            chain_tokens: Arc::new(chain_tokens),
            token_fetcher,
            tip_poll_interval: Duration::from_millis(tip_poll_interval_milliseconds),
            factories: config.factories.clone(),
        };

        // Pre-seed the wrapped native token to ensure it exists before any batches run.
        // This is critical because pools with zero addresses (native token) get normalized to wrapped native,
        // and if the wrapped native token doesn't exist, pool creation fails silently.
        worker
            .token_fetcher
            .ensure_wrapped_native_token(&config)
            .await?;

        Ok(worker)
    }

    pub async fn run(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut last_progress_log = Instant::now();

        // Initialize batch state with native token price from DB or default
        let native_token_price = self
            .db
            .postgres
            .get_native_token_price(self.chain_id as i64)
            .await?
            .unwrap_or_else(|| NativeTokenPrice::new(self.chain_id as i64, 0.0));

        let mut batch_state = BatchState {
            native_token_price,
        };

        loop {
            // Check cancellation at the start of each loop
            if cancellation_token.is_cancelled() {
                info!(
                    "Indexer for chain {} received cancellation signal",
                    self.chain_id
                );
                break;
            }

            let mut last_synced_block: u64 =
                match self.db.postgres.get_sync_checkpoint(self.chain_id).await {
                    Ok(block) => {
                        if block.is_some() {
                            block.unwrap().last_indexed_block
                        } else {
                            0
                        }
                    },
                    Err(e) => {
                        warn!(
                        "Failed to fetch last block from postgres: {:?}. Starting from block 0.",
                        e
                    );
                        0
                    },
                };

            let config = StreamConfig {
                ..Default::default()
            };

            let query = Query::new()
                .from_block(last_synced_block)
                .where_logs(self.filters.clone())
                .select_block_fields([BlockField::Number, BlockField::Timestamp])
                .select_log_fields([
                    LogField::BlockNumber,
                    LogField::TransactionHash,
                    LogField::LogIndex,
                    LogField::Address,
                    LogField::Data,
                    LogField::Topic0,
                    LogField::Topic1,
                    LogField::Topic2,
                    LogField::Topic3,
                ]);

            let mut stream = self.client.stream(query, config).await?;

            // Start the log stream
            while let Some(res) = tokio::time::timeout(STREAM_RECV_TIMEOUT, stream.recv())
                .await
                .map_err(|_| {
                    anyhow::anyhow!("Stream recv timeout after {:?}", STREAM_RECV_TIMEOUT)
                })?
            {
                let res = res.context("Stream error")?;

                // Get block timestamps for the log batch
                let block_timestamps: FxHashMap<u64, u64> = res
                    .data
                    .blocks
                    .iter()
                    .flatten()
                    .filter_map(|b| {
                        let n = b.number?;
                        let t = U256::from_be_slice(b.timestamp.as_ref()?).to::<u64>();
                        Some((n, t))
                    })
                    .collect();

                // Phase 1 -> Pre-parse all logs in a SINGLE PASS
                // This eliminates repeated parsing in phases 3 and 4.
                // We collect token addresses and parsed logs simultaneously.

                // Estimate log count for capacity hints (avoid reallocations)
                let log_count_estimate = res.data.logs.iter().flatten().count();

                // Phase 1: Parse all logs using the parser module (single-pass)
                // Returns parsed logs in sequential order + token/pool addresses
                let parse_result = parser::parse_logs(
                    res.data.logs.into_iter().flatten(),
                    &block_timestamps,
                    &self.chain_tokens,
                    log_count_estimate,
                );

                let parsed_logs = parse_result.parsed_logs;
                let mut token_addresses = parse_result.token_addresses;
                let mut modified_pools_addresses = parse_result.modified_pools_addresses;

                // Phase 1.5 -> Fetch existing pools that will be modified in this batch
                // We need to do this BEFORE fetching tokens so we can include their tokens
                modified_pools_addresses.sort();
                modified_pools_addresses.dedup();

                let mut updated_pools: FxHashMap<String, Pool> = FxHashMap::default();

                // Batch fetch all modified pools from PostgreSQL
                match self
                    .db
                    .postgres
                    .get_pools(self.chain_id as i64, &modified_pools_addresses)
                    .await
                {
                    Ok(pools) => {
                        for pool in pools {
                            // Add this pool's tokens to the token_addresses list
                            token_addresses.push(pool.token0.clone());
                            token_addresses.push(pool.token1.clone());
                            updated_pools.insert(pool.address.clone(), pool);
                        }
                    },
                    Err(e) => {
                        warn!("Failed to fetch pools from DB: {:?}", e);
                    },
                }

                // Deduplicate token addresses before fetching
                token_addresses.sort();
                token_addresses.dedup();

                // Phase 2 -> We fetch all the tokens required through the token_fetcher and create new ones.
                let mut tokens = self.token_fetcher.get_tokens(&token_addresses).await?;

                // Phase 3 -> Process Pool creation events from pre-parsed logs
                // (no re-parsing needed - we use the ParsedLog enum)

                // Create pools map for pools created during this batch
                let mut new_pools: FxHashMap<String, Pool> = FxHashMap::default();
                // NewPool records for ClickHouse (tracking new pool discoveries)
                let mut new_pool_records: Vec<NewPool> =
                    Vec::with_capacity(log_count_estimate / 50);

                for parsed_log in &parsed_logs {
                    match parsed_log {
                        // V2 Mint
                        ParsedLog::V2PairCreated {
                            event,
                            log_address,
                            block_number,
                            tx_hash,
                            block_timestamp,
                        } => {
                            // ANTI-SPOOFING: Validate pool address is not zero
                            let event_pool_address = hex_encode(event.pair.as_slice());
                            if event_pool_address == "0x0000000000000000000000000000000000000000" {
                                continue;
                            }

                            // FACTORY FILTER: Only index pools from allowed factories
                            if !self.factories.is_empty()
                                && !self.factories.contains(&log_address.to_lowercase())
                            {
                                continue;
                            }

                            if let (Some(token0), Some(token1)) = (
                                tokens.get(&hex_encode(event.token0.as_slice())),
                                tokens.get(&hex_encode(event.token1.as_slice())),
                            ) {
                                let pool = Pool::from_v2_pool_created(
                                    self.chain_id,
                                    log_address.clone(),
                                    event.clone(),
                                    token0,
                                    token1,
                                    *block_number,
                                    tx_hash.clone(),
                                    *block_timestamp,
                                    &self.chain_tokens,
                                );

                                let new_pool = NewPool::from_pool_created(
                                    self.chain_id as u64,
                                    pool.address.clone(),
                                    *block_number,
                                    tx_hash.clone(),
                                    *block_timestamp,
                                    pool.token0.clone(),
                                    pool.token1.clone(),
                                    pool.token0_symbol.clone(),
                                    pool.token1_symbol.clone(),
                                    pool.protocol.clone().unwrap_or_default(),
                                    pool.protocol_version.clone().unwrap_or_default(),
                                    pool.fee.unwrap_or(0),
                                );

                                new_pool_records.push(new_pool);
                                new_pools.insert(pool.address.clone(), pool);
                            }
                        },
                        ParsedLog::V3PoolCreated {
                            event,
                            log_address,
                            block_number,
                            tx_hash,
                            block_timestamp,
                        } => {
                            // ANTI-SPOOFING: Validate pool address is not zero
                            let event_pool_address = hex_encode(event.pool.as_slice());
                            if event_pool_address == "0x0000000000000000000000000000000000000000" {
                                continue;
                            }

                            // FACTORY FILTER: Only index pools from allowed factories
                            if !self.factories.is_empty()
                                && !self.factories.contains(&log_address.to_lowercase())
                            {
                                continue;
                            }

                            if let (Some(token0), Some(token1)) = (
                                tokens.get(&hex_encode(event.token0.as_slice())),
                                tokens.get(&hex_encode(event.token1.as_slice())),
                            ) {
                                let pool = Pool::from_v3_pool_created(
                                    self.chain_id,
                                    log_address.clone(),
                                    event.clone(),
                                    token0,
                                    token1,
                                    *block_number,
                                    tx_hash.clone(),
                                    *block_timestamp,
                                    &self.chain_tokens,
                                );

                                let new_pool = NewPool::from_pool_created(
                                    self.chain_id as u64,
                                    pool.address.clone(),
                                    *block_number,
                                    tx_hash.clone(),
                                    *block_timestamp,
                                    pool.token0.clone(),
                                    pool.token1.clone(),
                                    pool.token0_symbol.clone(),
                                    pool.token1_symbol.clone(),
                                    pool.protocol.clone().unwrap_or_default(),
                                    pool.protocol_version.clone().unwrap_or_default(),
                                    pool.fee.unwrap_or(0),
                                );

                                new_pool_records.push(new_pool);
                                new_pools.insert(pool.address.clone(), pool);
                            }
                        },
                        ParsedLog::V4Initialize {
                            event,
                            log_address,
                            block_number,
                            tx_hash,
                            block_timestamp,
                        } => {
                            // ANTI-SPOOFING: Validate pool ID by computing it from event fields
                            let event_pool_id = hex_encode(event.id.as_slice());
                            let computed_pool_id = compute_v4_pool_id(
                                &hex_encode(event.currency0.as_slice()),
                                &hex_encode(event.currency1.as_slice()),
                                event.fee.to::<u32>(),
                                event.tickSpacing.as_i32(),
                                &event.hooks.to_string(),
                            );

                            if event_pool_id != computed_pool_id {
                                warn!(
                                    "V4 Initialize: Pool ID mismatch! Event ID: {}, Computed: {}",
                                    event_pool_id, computed_pool_id
                                );
                                continue;
                            }

                            // FACTORY FILTER: Only index pools from allowed factories
                            if !self.factories.is_empty()
                                && !self.factories.contains(&log_address.to_lowercase())
                            {
                                continue;
                            }

                            // FACTORY FILTER: Only index pools from allowed factories
                            if !self.factories.is_empty()
                                && !self.factories.contains(&log_address.to_lowercase())
                            {
                                continue;
                            }

                            if let (Some(token0), Some(token1)) = (
                                tokens.get(&hex_encode(event.currency0.as_slice())),
                                tokens.get(&hex_encode(event.currency1.as_slice())),
                            ) {
                                let pool = Pool::from_v4_pool_created(
                                    self.chain_id,
                                    log_address.clone(),
                                    event.clone(),
                                    token0,
                                    token1,
                                    *block_number,
                                    tx_hash.clone(),
                                    *block_timestamp,
                                    &self.chain_tokens,
                                );

                                let new_pool = NewPool::from_pool_created(
                                    self.chain_id as u64,
                                    pool.address.clone(),
                                    *block_number,
                                    tx_hash.clone(),
                                    *block_timestamp,
                                    pool.token0.clone(),
                                    pool.token1.clone(),
                                    pool.token0_symbol.clone(),
                                    pool.token1_symbol.clone(),
                                    pool.protocol.clone().unwrap_or_default(),
                                    pool.protocol_version.clone().unwrap_or_default(),
                                    pool.fee.unwrap_or(0),
                                );

                                new_pool_records.push(new_pool);
                                new_pools.insert(pool.address.clone(), pool);
                            }
                        },
                        _ => {}, // Other event types handled in Phase 4
                    }
                }

                // Collect all pools in a single hashmap for easy access.
                // new_pools takes precedence (comes second in chain) to handle pools created earlier in the same batch
                let mut pools: FxHashMap<String, Pool> = updated_pools
                    .into_iter()
                    .chain(new_pools.into_iter())
                    .collect();

                // Phase 4 -> Process all swap/liquidity/transfer events from pre-parsed logs
                // IMPORTANT: parsed_logs maintains sequential order from original log stream
                // This is critical for correct pool state updates and native token price tracking

                // Initialize storage vectors with capacity hints
                let mut events: Vec<Event> = Vec::with_capacity(log_count_estimate / 2);
                let mut supply_events: Vec<SupplyEvent> =
                    Vec::with_capacity(log_count_estimate / 10);

                for parsed_log in parsed_logs {
                    match parsed_log {
                        // V2 Mint
                        ParsedLog::V2Mint {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v2_mint(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    if self.chain_tokens.is_stable_pool(&log_address) {
                                        batch_state.native_token_price.update_from_pool(
                                            pool,
                                            &self.chain_tokens.wrapped_native_token,
                                        );
                                    }
                                    events.push(ev);
                                }
                            }
                        },
                        // V3 Mint
                        ParsedLog::V3Mint {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v3_mint(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    events.push(ev);
                                }
                            }
                        },
                        // V2 Burn
                        ParsedLog::V2Burn {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v2_burn(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    if self.chain_tokens.is_stable_pool(&log_address) {
                                        batch_state.native_token_price.update_from_pool(
                                            pool,
                                            &self.chain_tokens.wrapped_native_token,
                                        );
                                    }
                                    events.push(ev);
                                }
                            }
                        },
                        // V3 Burn
                        ParsedLog::V3Burn {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v3_burn(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    events.push(ev);
                                }
                            }
                        },
                        // V2 Sync
                        ParsedLog::V2Sync {
                            event,
                            log_address,
                            block_number,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                pool.update_v2_sync(&event, block_number, block_timestamp);
                                if self.chain_tokens.is_stable_pool(&log_address) {
                                    batch_state.native_token_price.update_from_pool(
                                        pool,
                                        &self.chain_tokens.wrapped_native_token,
                                    );
                                }
                            }
                        },
                        // V3 Initialize
                        ParsedLog::V3Initialize {
                            event,
                            log_address,
                            block_number,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                pool.update_v3_initialize(&event, block_number, block_timestamp);
                                if self.chain_tokens.is_stable_pool(&log_address) {
                                    batch_state.native_token_price.update_from_pool(
                                        pool,
                                        &self.chain_tokens.wrapped_native_token,
                                    );
                                }
                            }
                        },
                        // V3 Collect
                        ParsedLog::V3Collect {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v3_collect(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    events.push(ev);
                                }
                            }
                        },
                        // V4 ModifyLiquidity
                        ParsedLog::V4ModifyLiquidity {
                            event,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            let event_pool_id = hex_encode(event.id.as_slice());
                            if let Some(pool) = pools.get_mut(&event_pool_id) {
                                // ANTI-SPOOFING: Validate pool ID
                                let computed_pool_id = compute_v4_pool_id_from_stored(
                                    &pool.token0,
                                    &pool.token1,
                                    pool.initial_fee.unwrap_or(0) as u32,
                                    pool.tick_spacing.unwrap_or(0),
                                    pool.hook_address
                                        .as_deref()
                                        .unwrap_or("0x0000000000000000000000000000000000000000"),
                                );
                                if event_pool_id != computed_pool_id {
                                    warn!("V4 ModifyLiquidity: Pool ID mismatch! Event ID: {}, Computed: {}", event_pool_id, computed_pool_id);
                                    continue;
                                }
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v4_modify_liquidity(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                        pool,
                                    );
                                    pool.update_from_event(&ev);
                                    events.push(ev);
                                }
                            }
                        },
                        // V2 Swap
                        ParsedLog::V2Swap {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v2_swap(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    if self.chain_tokens.is_stable_pool(&log_address) {
                                        batch_state.native_token_price.update_from_pool(
                                            pool,
                                            &self.chain_tokens.wrapped_native_token,
                                        );
                                    }
                                    events.push(ev);
                                }
                            }
                        },
                        // V3 Swap
                        ParsedLog::V3Swap {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(pool) = pools.get_mut(&log_address) {
                                // ANTI-SPOOFING: Require initialization
                                if pool.sqrt_price_x96.is_none() {
                                    continue;
                                }
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v3_swap(
                                        self.chain_id,
                                        event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    if self.chain_tokens.is_stable_pool(&log_address) {
                                        batch_state.native_token_price.update_from_pool(
                                            pool,
                                            &self.chain_tokens.wrapped_native_token,
                                        );
                                    }
                                    events.push(ev);
                                }
                            }
                        },
                        // V4 Swap
                        ParsedLog::V4Swap {
                            event: swap_event,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            let event_pool_id = hex_encode(swap_event.id.as_slice());
                            if let Some(pool) = pools.get_mut(&event_pool_id) {
                                // ANTI-SPOOFING: Validate pool ID
                                let computed_pool_id = compute_v4_pool_id_from_stored(
                                    &pool.token0,
                                    &pool.token1,
                                    pool.initial_fee.unwrap_or(0) as u32,
                                    pool.tick_spacing.unwrap_or(0),
                                    pool.hook_address
                                        .as_deref()
                                        .unwrap_or("0x0000000000000000000000000000000000000000"),
                                );
                                if event_pool_id != computed_pool_id {
                                    warn!(
                                        "V4 Swap: Pool ID mismatch! Event ID: {}, Computed: {}",
                                        event_pool_id, computed_pool_id
                                    );
                                    continue;
                                }
                                // ANTI-SPOOFING: Require initialization
                                if pool.sqrt_price_x96.is_none() {
                                    continue;
                                }
                                pool.update_v4_fee(&swap_event);
                                if let (Some(token0), Some(token1)) =
                                    (tokens.get(&pool.token0), tokens.get(&pool.token1))
                                {
                                    let ev = Event::from_v4_swap(
                                        self.chain_id as u64,
                                        swap_event,
                                        token0,
                                        token1,
                                        block_number,
                                        tx_hash,
                                        log_index,
                                        pool.address.clone(),
                                        block_timestamp,
                                    );
                                    pool.update_from_event(&ev);
                                    if self.chain_tokens.is_stable_pool(&event_pool_id) {
                                        batch_state.native_token_price.update_from_pool(
                                            pool,
                                            &self.chain_tokens.wrapped_native_token,
                                        );
                                    }
                                    events.push(ev);
                                }
                            }
                        },

                        // Supply: Transfer (Mint/Burn)
                        ParsedLog::SupplyTransfer {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                            is_mint,
                        } => {
                            if let Some(token) = tokens.get(&log_address) {
                                let event_type = if is_mint { "mint" } else { "burn" }.to_string();

                                let event = SupplyEvent::new(
                                    self.chain_id as u64,
                                    block_number,
                                    block_timestamp,
                                    tx_hash,
                                    log_index,
                                    log_address,
                                    event_type,
                                    event.value,
                                    token.decimals as u8,
                                );

                                supply_events.push(event);
                            }
                        },
                        // Supply: Deposit (Mint for Wrapped)
                        ParsedLog::SupplyDeposit {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(token) = tokens.get(&log_address) {
                                let event = SupplyEvent::new(
                                    self.chain_id as u64,
                                    block_number,
                                    block_timestamp,
                                    tx_hash,
                                    log_index,
                                    log_address,
                                    "mint".to_string(),
                                    event.amount,
                                    token.decimals as u8,
                                );

                                supply_events.push(event);
                            }
                        },
                        // Supply: Withdrawal (Burn for Wrapped)
                        ParsedLog::SupplyWithdrawal {
                            event,
                            log_address,
                            block_number,
                            log_index,
                            tx_hash,
                            block_timestamp,
                        } => {
                            if let Some(token) = tokens.get(&log_address) {
                                let event = SupplyEvent::new(
                                    self.chain_id as u64,
                                    block_number,
                                    block_timestamp,
                                    tx_hash,
                                    log_index,
                                    log_address,
                                    "burn".to_string(),
                                    event.amount,
                                    token.decimals as u8,
                                );

                                supply_events.push(event);
                            }
                        },
                        _ => {},
                    }
                }

                // Phase 4.5 -> Price all events and pools in USD
                // This happens after all events are created and pool states are updated
                //
                // IMPORTANT: Create price resolver HERE, after all events processed,
                // so it has the latest native token price from Sync/Swap events on the stable pool.
                let mut price_resolver = PriceResolver::new(
                    self.chain_tokens.clone(),
                    batch_state.native_token_price.price_usd,
                );

                // Price all events
                for event in &mut events {
                    if let Some(pool) = pools.get(&event.pool_address) {
                        price_resolver.price_event(event, pool, &pools);
                    }
                }

                // Price all pools (calculate price_usd and tvl_usd)
                // Use 2-pass approach to avoid cloning the entire pools map
                // 1. Calculate updates (immutable access to pools)
                let pool_updates: Vec<(String, Option<f64>, Option<f64>)> = pools
                    .values()
                    .map(|pool| {
                        let (price, tvl) = price_resolver.calculate_pool_pricing(pool, &pools);
                        (pool.address.clone(), price, tvl)
                    })
                    .collect();

                // 2. Apply updates (mutable access to pools)
                for (address, price, tvl) in pool_updates {
                    if let Some(pool) = pools.get_mut(&address) {
                        pool.price_usd = price;
                        pool.tvl_usd = tvl;
                    }
                }

                // Update initial_tvl_usd for newly created pools
                for new_pool in &mut new_pool_records {
                    if let Some(pool) = pools.get(&new_pool.pool_address) {
                        if let Some(tvl) = pool.tvl_usd {
                            new_pool.set_initial_tvl(tvl);
                        }
                    }
                }

                // Price all tokens using TVL-weighted average from pools where token is base
                for token in tokens.values_mut() {
                    if let Some(price) =
                        price_resolver.calculate_token_price(&token.address, &pools)
                    {
                        token.price_usd = Some(price);
                        token.price_updated_at = Some(Utc::now());
                    }
                }

                // Phase 5 -> Flush pool states to PostgreSQL
                // IMPORTANT: We must await these AND check for errors before updating checkpoint
                // to prevent race conditions and data loss
                // NOTE: Tokens are already saved in TokenFetcher when first discovered

                let pools_to_flush: Vec<&Pool> = pools.values().collect();

                // Batch flush tokens with updated prices
                let tokens_to_flush: Vec<&crate::db::models::Token> = tokens.values().collect();

                // Execute DB writes in parallel to reduce latency
                let (pools_res, _) = tokio::join!(
                    self.db.postgres.set_pools(&pools_to_flush),
                    self.db.postgres.set_tokens(&tokens_to_flush)
                );

                // Check critical results
                if let Err(e) = pools_res {
                    warn!(
                        "Chain {}: Failed to batch write pools: {:?}",
                        self.chain_id, e
                    );
                }

                // Phase 6 -> Send events to ClickHouse
                // NOTE: This only queues the data. For full consistency, we should wait
                // for ClickHouse confirmation, but that would require a different architecture
                // (e.g., two-phase commit or acknowledgment channel)
                let batch = BatchDataMessage {
                    chain_id: self.chain_id,
                    events,
                    supply_events,
                    new_pools: new_pool_records,
                    pools: pools.values().cloned().collect(),
                    tokens: tokens.values().cloned().collect(),
                };

                // Tip detection: use timestamp-based approach for chain-agnostic detection
                // If the latest block is within 60 seconds of current time, we're at the tip
                // This works correctly regardless of chain block time (Ethereum ~12s, Arbitrum ~0.25s)
                let current_timestamp = Utc::now().timestamp() as u64;
                let latest_block_timestamp = block_timestamps.values().max().copied().unwrap_or(0);
                let seconds_behind = current_timestamp.saturating_sub(latest_block_timestamp);
                let is_at_tip = seconds_behind < 60;

                if is_at_tip {
                    // If the data is from tip, we also send to the pub/sub channels.
                    self.live_sender
                        .send(IngestMessage::BatchData(batch))
                        .await?;
                } else {
                    self.historical_sender
                        .send(IngestMessage::BatchData(batch))
                        .await?;
                }

                // Update checkpoint ONLY after PostgreSQL writes complete
                // NOTE: ClickHouse data is still in the channel buffer at this point.
                // On crash, we may re-process some blocks (causing duplicates in ClickHouse).
                // This is acceptable: duplicates are better than data loss,
                // and ClickHouse's ReplacingMergeTree can handle them.
                let next_block = res.next_block;
                last_synced_block = next_block;
                let checkpoint = SyncCheckpoint::new(self.chain_id, next_block);

                // Synchronously update checkpoint - errors are critical
                if let Err(e) = self.db.postgres.set_sync_checkpoint(&checkpoint).await {
                    // Don't continue if checkpoint update fails - this could cause
                    // the indexer to skip blocks on restart
                    return Err(anyhow::anyhow!(
                        "Critical: Failed to update checkpoint for chain {}: {:?}. Stopping to prevent data loss.",
                        self.chain_id, e
                    ));
                }

                // Save native token price to database (fire-and-forget, non-critical)
                let _ = self
                    .db
                    .postgres
                    .set_native_token_price(&batch_state.native_token_price)
                    .await;

                // Log progress every PROGRESS_LOG_INTERVAL seconds to reduce noise
                if last_progress_log.elapsed() >= PROGRESS_LOG_INTERVAL {
                    info!(
                        "Chain {} synced to block {} (native price: ${:.5} USD)",
                        self.chain_id, next_block, batch_state.native_token_price.price_usd
                    );
                    last_progress_log = Instant::now();
                }
            }

            // HEARTBEAT: Update checkpoint timestamp even if no new blocks/logs were processed
            // This ensures Grafana "lag" monitor doesn't trigger false positives during quiet periods.
            // Only update if we are not shutting down (loop finished naturally).
            let checkpoint = SyncCheckpoint::new(self.chain_id, last_synced_block);
            if let Err(e) = self.db.postgres.set_sync_checkpoint(&checkpoint).await {
                warn!(
                    "Failed to update heartbeat checkpoint for chain {}: {:?}",
                    self.chain_id, e
                );
            }

            // Sleep before next poll
            tokio::time::sleep(self.tip_poll_interval).await;
        }

        Ok(())
    }
}
