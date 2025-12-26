use crate::abis::erc20::IERC20;
use crate::abis::multicall::Call3;
use crate::db::models::DatabaseChain;
use crate::Database;
use crate::{abis::multicall::IMulticall3, db::models::Token};
use alloy::providers::MULTICALL3_ADDRESS;
use alloy::{
    providers::{DynProvider, ProviderBuilder},
    sol_types::SolCall,
};
use anyhow::{Context, Result};
use log::info;
use moka::future::Cache;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Token metadata fetcher using multicall3
#[derive(Clone)]
pub struct TokenFetcher {
    db: Arc<Database>,
    chain_id: i64,
    provider: DynProvider,
    /// Cache of token addresses that failed to fetch (invalid contracts, no decimals, etc.)
    /// Prevents repeatedly trying to fetch tokens that will never succeed
    invalid_tokens: Cache<String, ()>,
}

/// Maximum retries for multicall
const MAX_RETRIES: u32 = 3;

/// Delay between retries (exponential backoff base)
const RETRY_DELAY_MS: u64 = 100;

/// Timeout for individual RPC calls (30 seconds)
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(30);

impl TokenFetcher {
    pub fn new(rpc_url: String, chain_id: i64, db: Arc<Database>) -> Self {
        let url = Url::parse(&rpc_url).expect("Invalid RPC URL");

        let client = ProviderBuilder::new().connect_http(url);

        let provider = DynProvider::new(client.clone());

        // Create cache with 10,000 capacity and 1 hour TTL
        // frequent lookups for known invalid tokens will hit cache
        // TTL ensures we retry eventually in case contract was deployed/fixed
        let invalid_tokens = Cache::builder()
            .max_capacity(10_000)
            .time_to_live(Duration::from_secs(3600))
            .build();

        Self {
            db,
            chain_id,
            provider,
            invalid_tokens,
        }
    }

    pub async fn get_tokens(&self, addresses: &[String]) -> Result<HashMap<String, Token>> {
        let mut result = HashMap::new();

        // Filter out known invalid tokens before any lookups
        let valid_addresses: Vec<String> = addresses
            .iter()
            .filter(|addr| !self.invalid_tokens.contains_key(*addr))
            .cloned()
            .collect();

        if valid_addresses.is_empty() {
            return Ok(result);
        }

        // Batch fetch existing tokens from PostgreSQL
        let existing_tokens = self
            .db
            .postgres
            .get_tokens(self.chain_id, &valid_addresses)
            .await?;

        // Add existing tokens to result and track which ones we found
        let found_addresses: HashSet<String> = existing_tokens
            .into_iter()
            .map(|token| {
                let addr = token.address.clone();
                result.insert(addr.clone(), token);
                addr
            })
            .collect();

        // Find addresses that weren't in the database
        let missing_addresses: Vec<String> = valid_addresses
            .into_iter()
            .filter(|addr| !found_addresses.contains(addr))
            .collect();

        // Fetch missing tokens via multicall
        if !missing_addresses.is_empty() {
            let fetched = self.fetch_metadata_batch(&missing_addresses).await;

            // Collect new tokens for batch save and search index
            let mut new_tokens_for_save = Vec::new();

            // Zip the requested addresses with the fetched results
            for (requested_addr, maybe_token) in missing_addresses.iter().zip(fetched.into_iter()) {
                if let Some(token) = maybe_token {
                    new_tokens_for_save.push(token.clone());
                    result.insert(requested_addr.clone(), token);
                } else {
                    // Token fetch failed - add to invalid cache
                    self.invalid_tokens.insert(requested_addr.clone(), ()).await;
                }
            }

            // Batch save new tokens to PostgreSQL
            if !new_tokens_for_save.is_empty() {
                let refs: Vec<&Token> = new_tokens_for_save.iter().collect();
                let _ = self.db.postgres.set_tokens(&refs).await;
            }
        }

        Ok(result)
    }

    /// Batch size for multicall requests to avoid RPC congestion/timeouts
    const MULTICALL_BATCH_SIZE: usize = 20;

    async fn fetch_metadata_batch(&self, addresses: &[String]) -> Vec<Option<Token>> {
        let mut all_tokens: Vec<Option<Token>> = Vec::with_capacity(addresses.len());

        // Process addresses in batches to avoid RPC congestion
        for chunk in addresses.chunks(Self::MULTICALL_BATCH_SIZE) {
            let batch_tokens = self.fetch_metadata_chunk_with_retry(chunk).await;
            all_tokens.extend(batch_tokens);
        }

        all_tokens
    }

    /// Fetch metadata with retry logic
    async fn fetch_metadata_chunk_with_retry(&self, addresses: &[String]) -> Vec<Option<Token>> {
        for attempt in 0..MAX_RETRIES {
            match self.fetch_metadata_chunk(addresses).await {
                Ok(tokens) => return tokens,
                Err(_) => {
                    if attempt < MAX_RETRIES - 1 {
                        let delay = Duration::from_millis(RETRY_DELAY_MS * 2_u64.pow(attempt));
                        tokio::time::sleep(delay).await;
                    }
                },
            }
        }

        // All retries failed - try individual fetches as fallback
        self.fetch_tokens_individually(addresses).await
    }

    /// Fallback: fetch tokens one by one when multicall fails
    async fn fetch_tokens_individually(&self, addresses: &[String]) -> Vec<Option<Token>> {
        // Execute fetches concurrently
        let tasks = addresses.iter().map(|addr| self.fetch_single_token(addr));
        futures::future::join_all(tasks).await
    }

    /// Fetch a single token's metadata
    async fn fetch_single_token(&self, addr: &str) -> Option<Token> {
        let address = match addr.parse() {
            Ok(a) => a,
            Err(_) => return None,
        };

        let token_contract = IERC20::new(address, &self.provider);

        // Decimals is required - skip token if it fails (with timeout)
        let decimals =
            match tokio::time::timeout(RPC_CALL_TIMEOUT, token_contract.decimals().call()).await {
                Ok(Ok(d)) => d,
                _ => return None,
            };

        if decimals > 24 {
            return None;
        }

        // Try to get name (optional, with timeout)
        let name = tokio::time::timeout(RPC_CALL_TIMEOUT, token_contract.name().call())
            .await
            .ok()
            .and_then(|r| r.ok())
            .map(|n| n.to_string())
            .unwrap_or_default();

        // Try to get symbol (optional, with timeout)
        let symbol = tokio::time::timeout(RPC_CALL_TIMEOUT, token_contract.symbol().call())
            .await
            .ok()
            .and_then(|r| r.ok())
            .map(|s| s.to_string())
            .unwrap_or_default();

        Some(Token::new(
            self.chain_id as u64,
            addr.to_string(),
            symbol,
            name,
            decimals,
        ))
    }

    async fn fetch_metadata_chunk(&self, addresses: &[String]) -> Result<Vec<Option<Token>>> {
        let multicall = IMulticall3::new(MULTICALL3_ADDRESS, &self.provider);
        let mut calls = Vec::with_capacity(addresses.len() * 3);

        for addr in addresses {
            let address = addr.parse().context("Invalid address")?;
            let token = IERC20::new(address, &self.provider);

            // name()
            calls.push(Call3 {
                target: address,
                allowFailure: true,
                callData: token.name().calldata().to_vec().into(),
            });
            // symbol()
            calls.push(Call3 {
                target: address,
                allowFailure: true,
                callData: token.symbol().calldata().to_vec().into(),
            });
            // decimals()
            calls.push(Call3 {
                target: address,
                allowFailure: true,
                callData: token.decimals().calldata().to_vec().into(),
            });
        }

        let results = tokio::time::timeout(RPC_CALL_TIMEOUT, multicall.aggregate3(calls).call())
            .await
            .context("Multicall timeout")?
            .context("Multicall aggregate3 failed")?;

        // Use Vec<Option<Token>> to maintain index alignment with input addresses
        let mut tokens: Vec<Option<Token>> = Vec::with_capacity(addresses.len());

        for (i, addr) in addresses.iter().enumerate() {
            let base_idx = i * 3;
            if base_idx + 2 >= results.len() {
                // Not enough results, push None for remaining addresses
                tokens.push(None);
                continue;
            }

            let name_res = &results[base_idx];
            let symbol_res = &results[base_idx + 1];
            let decimals_res = &results[base_idx + 2];

            // Decimals is required - skip token if it fails
            let decimals = if decimals_res.success {
                match IERC20::decimalsCall::abi_decode_returns(&decimals_res.returnData) {
                    Ok(d) => d,
                    Err(_) => {
                        tokens.push(None);
                        continue;
                    },
                }
            } else {
                tokens.push(None);
                continue;
            };

            if decimals > 24 {
                tokens.push(None);
                continue;
            }

            let name = if name_res.success {
                IERC20::nameCall::abi_decode_returns(&name_res.returnData).unwrap_or_default()
            } else {
                String::new()
            };

            let symbol = if symbol_res.success {
                IERC20::symbolCall::abi_decode_returns(&symbol_res.returnData).unwrap_or_default()
            } else {
                String::new()
            };

            tokens.push(Some(Token::new(
                self.chain_id as u64,
                addr.clone(),
                symbol,
                name,
                decimals,
            )));
        }

        Ok(tokens)
    }

    /// Ensures the wrapped native token exists in the database.
    /// This should be called once at worker startup to guarantee the token exists
    /// before any batches run, since pools with zero addresses get normalized to wrapped native.
    pub async fn ensure_wrapped_native_token(&self, chain: &DatabaseChain) -> Result<()> {
        let addr = chain.native_token_address.to_lowercase();

        // Check if already exists
        let existing = self
            .db
            .postgres
            .get_tokens(self.chain_id, &[addr.clone()])
            .await?;

        if existing
            .iter()
            .any(|token| token.address.to_lowercase() == addr)
        {
            return Ok(());
        }

        // Try to fetch real metadata from RPC
        let fetched = self.get_tokens(&[addr.clone()]).await?;
        if fetched.contains_key(&addr) {
            return Ok(()); // Successfully fetched and saved by get_tokens
        }

        // RPC fetch failed, create fallback
        let fallback_token = Token::new(
            self.chain_id as u64,
            addr,
            chain.native_token_symbol.clone(),
            chain.native_token_name.clone(),
            chain.native_token_decimals as u8,
        );
        self.db.postgres.set_tokens(&[&fallback_token]).await?;

        info!(
            "Pre-seeded wrapped native token for chain {}",
            self.chain_id
        );

        Ok(())
    }
}
