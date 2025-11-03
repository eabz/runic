use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RunicConfig {
    pub contract: ContractConfig,
    pub network: NetworkConfig,
    pub engines: EngineConfig,
}

#[derive(Debug, Deserialize)]
pub struct ContractConfig {
    pub address: String,
    pub start_block: i64,
    pub child_contract: Option<ChildContractConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ChildContractConfig {
    pub event_signature: String,
    pub abi_path: String,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub rpc_endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    pub api: String,
    pub db: String,
}
