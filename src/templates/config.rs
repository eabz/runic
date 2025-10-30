use std::{fs, path::Path};

use tera::Context;

use crate::{errors::ScaffoldError, templates::render_template};

pub const RUNIC_CONFIG_TEMPLATE: &str = r#"use serde::Deserialize;

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

pub fn write_runic_config(
    project_root: &Path,
) -> Result<(), ScaffoldError> {
    let bin_dir = project_root.join("src");
    let runic_config_path = bin_dir.join("config.rs");
    let context = Context::new();
    let runic_config_contents =
        render_template(RUNIC_CONFIG_TEMPLATE, &context)?;

    fs::write(runic_config_path, runic_config_contents)?;
    Ok(())
}
