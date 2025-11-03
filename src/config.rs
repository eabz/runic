use clap::ValueEnum;
use serde::Serialize;
use std::{fmt, fs, path::Path};

use crate::errors::RunicError;

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, ValueEnum,
)]
/// Supported APIs that the generated indexer can expose.
pub enum API {
    Graphql,
    /// Expose services via gRPC.
    Grpc,
}

impl fmt::Display for API {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            API::Graphql => "graphql",
            API::Grpc => "grpc",
        })
    }
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, ValueEnum,
)]
/// Supported database backends for generated projects.
pub enum Database {
    /// Use a PostgreSQL database.
    Postgres,
    /// Use an embedded SQLite database.
    Sqlite,
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Database::Postgres => "postgres",
            Database::Sqlite => "sqlite",
        })
    }
}

#[derive(Serialize)]
pub struct RunicConfig {
    pub contract: ContractConfig,
    pub network: NetworkConfig,
    pub database: DatabaseConfig,
    pub engines: EngineConfig,
}

impl RunicConfig {
    pub fn new(
        address: String,
        start_block: i64,
        api: API,
        db: Database,
        db_uri: String,
        child_contract: Option<ChildContractConfig>,
    ) -> Self {
        Self {
            contract: ContractConfig {
                address,
                start_block,
                child_contract,
            },
            network: NetworkConfig { rpc_endpoint: String::new() },
            database: DatabaseConfig { uri: db_uri },
            engines: EngineConfig {
                api: api.to_string(),
                db: db.to_string(),
            },
        }
    }
}

#[derive(Serialize)]
pub struct ContractConfig {
    pub address: String,
    pub start_block: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_contract: Option<ChildContractConfig>,
}

#[derive(Serialize)]
pub struct NetworkConfig {
    pub rpc_endpoint: String,
}

#[derive(Serialize)]
pub struct DatabaseConfig {
    pub uri: String,
}

#[derive(Serialize)]
pub struct EngineConfig {
    pub api: String,
    pub db: String,
}

#[derive(Serialize, Clone)]
pub struct ChildContractConfig {
    pub event_signature: String,
    pub abi_path: String,
}

pub fn write_config(
    config_path: &Path,
    config: &RunicConfig,
) -> Result<(), RunicError> {
    let config_contents = toml::to_string_pretty(config)?;
    fs::write(config_path, config_contents)?;
    Ok(())
}
