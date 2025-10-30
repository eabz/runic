use std::{fs, path::Path};

use tera::Context;

use crate::{errors::ScaffoldError, templates::render_template};

pub const RUNIC_RPC_TEMPLATE: &str = r#"use serde::Deserialize;

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

"#;

pub fn write_runic_rpc(project_root: &Path) -> Result<(), ScaffoldError> {
    let bin_dir = project_root.join("src");
    let runic_rpc_path = bin_dir.join("config.rs");
    let context = Context::new();
    let runic_rpc_contents =
        render_template(RUNIC_RPC_TEMPLATE, &context)?;

    fs::write(runic_rpc_path, runic_rpc_contents)?;
    Ok(())
}
