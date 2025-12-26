use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Simple struct with chain tokens for synchronous access
/// Used to calculate pool token priorities
#[derive(Debug, Clone)]
pub struct ChainTokens {
    pub wrapped_native_token: String,
    pub stable_token: String,
    pub major_tokens: Vec<String>,
    pub stablecoins: Vec<String>,
    pub stable_pool_address: String,
}

impl ChainTokens {
    pub fn new(
        wrapped_native_token: String,
        stable_token: String,
        major_tokens: Vec<String>,
        stablecoins: Vec<String>,
        stable_pool_address: String,
    ) -> Self {
        Self {
            wrapped_native_token,
            stable_token,
            major_tokens,
            stablecoins,
            stable_pool_address,
        }
    }

    pub fn is_wrapped_native(&self, token: &str) -> bool {
        self.wrapped_native_token.to_lowercase() == token.to_lowercase()
    }

    pub fn is_stable(&self, token: &str) -> bool {
        let token_lower = token.to_lowercase();

        // IMPORTANT: Wrapped native token is NEVER a stablecoin, even if misconfigured.
        // This prevents configuration errors where native token is added to stablecoins array.
        if self.is_wrapped_native(&token_lower) {
            return false;
        }

        // Check single stable token (for backward compatibility)
        if self.stable_token.to_lowercase() == token_lower {
            return true;
        }
        // Check stablecoins array
        self.stablecoins
            .iter()
            .any(|s| s.to_lowercase() == token_lower)
    }

    pub fn is_major_token(&self, token: &str) -> bool {
        let token_lower = token.to_lowercase();
        self.major_tokens
            .iter()
            .any(|s| s.to_lowercase() == token_lower)
    }

    pub fn is_stable_pool(&self, address: &str) -> bool {
        self.stable_pool_address.to_lowercase() == address.to_lowercase()
    }
}

/// Blockchain configuration stored in PostgreSQL.
///
/// Contains connection URLs, native token metadata, and token classification
/// arrays (stablecoins, major tokens) used for price resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatabaseChain {
    pub chain_id: u64,
    pub name: String,
    pub rpc_url: String,
    pub hypersync_url: String,
    pub enabled: bool,
    pub native_token_address: String,
    pub native_token_decimals: u8,
    pub native_token_name: String,
    pub native_token_symbol: String,
    pub stable_token_address: String,
    pub stable_token_decimals: u8,
    pub stable_pool_address: String,
    pub major_tokens: Vec<String>,
    pub stablecoins: Vec<String>,
    #[serde(default)]
    pub factories: Vec<String>,
    pub updated_at: Option<DateTime<Utc>>,
}
