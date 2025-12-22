use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Indexer sync progress checkpoint (PostgreSQL).
///
/// Tracks the last successfully indexed block for each chain.
/// Used to resume indexing after restarts without missing or duplicating data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCheckpoint {
    pub chain_id: i64,
    pub last_indexed_block: i64,
    pub updated_at: DateTime<Utc>,
}

impl SyncCheckpoint {
    pub fn new(chain_id: i64, last_indexed_block: i64) -> Self {
        Self {
            chain_id,
            last_indexed_block,
            updated_at: Utc::now(),
        }
    }
}
