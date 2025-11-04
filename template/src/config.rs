use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RunicConfig {
    pub contract: ContractConfig,
    pub network: NetworkConfig,
    pub database: DatabaseConfig,
    pub engines: EngineConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContractConfig {
    pub address: String,
    pub start_block: i64,
    pub child_contract: Option<ChildContractConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChildContractConfig {
    pub event_signature: String,
    pub abi_path: String,
    pub address_param_index: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    pub rpc_endpoint: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub uri: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub api: String,
    pub db: String,
}
