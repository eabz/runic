use std::{fs, path::Path};

use tera::Context;

use crate::{errors::ScaffoldError, templates::render_template};

pub const RUNIC_INDEXER_TEMPLATE: &str = r#"use std::fs;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RunicConfig {
    contract: ContractConfig,
    network: NetworkConfig,
    engines: EngineConfig,
}

#[derive(Debug, Deserialize)]
struct ContractConfig {
    address: String,
    start_block: i64,
}

#[derive(Debug, Deserialize)]
struct NetworkConfig {
    rpc_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct EngineConfig {
    api: String,
    db: String,
}

fn main() {
    let config = load_config("Config.toml");
    println!(
        "Indexer configured for contract {} starting at block {}",
        config.contract.address, config.contract.start_block
    );
}

fn load_config(path: &str) -> RunicConfig {
    let contents =
        fs::read_to_string(path).unwrap_or_else(|err| panic!("Failed to read {}: {err}", path));
    toml::from_str(&contents)
        .unwrap_or_else(|err| panic!("Failed to parse {}: {err}", path))
}
"#;

pub fn write_runic_indexer(
    project_root: &Path,
) -> Result<(), ScaffoldError> {
    let bin_dir = project_root.join("bin");
    let runic_indexer_path = bin_dir.join("runic-indexer.rs");
    let context = Context::new();
    let runic_indexer_contents =
        render_template(RUNIC_INDEXER_TEMPLATE, &context)?;

    fs::write(runic_indexer_path, runic_indexer_contents)?;
    Ok(())
}
