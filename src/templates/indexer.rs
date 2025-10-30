use std::{fs, path::Path};

use tera::Context;

use crate::{errors::RunicError, templates::render_template};

pub const RUNIC_INDEXER_TEMPLATE: &str = r#"use std::{fs, process, sync::Arc};

use alloy::json_abi::JsonAbi;
use runic_indexer::{config::RunicConfig, rpc::Rpc};
use simple_logger::SimpleLogger;

fn load_config(path: &str) -> RunicConfig {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("Failed to read {}: {err}", path));
    toml::from_str(&contents)
        .unwrap_or_else(|err| panic!("Failed to parse {}: {err}", path))
}

#[tokio::main()]
async fn main() {
    let log = SimpleLogger::new();

    if let Err(err) = log.init() {
        eprintln!("Logger setup failed: {err}");
    }

    let config = load_config("Config.toml");
    println!(
        "Stating indexer configured for contract {} starting at block {}",
        config.contract.address, config.contract.start_block
    );

    if config.network.rpc_endpoint.trim().is_empty() {
        eprintln!(
            "Config error: network.rpc_endpoint is empty. Please add it to Config.toml."
        );
        process::exit(1);
    }

    let abi_path = "src/abi/abi.json";
    let abi_txt = std::fs::read_to_string(abi_path)
        .expect("unable to decode abi file");

    let abi: JsonAbi =
        serde_json::from_str(&abi_txt).expect("unable to parse abi json");

    let rpc = Rpc::new(Arc::new(config), Arc::new(abi)).await;

    rpc.listen_events().await;
}
"#;

pub fn write_runic_indexer(project_root: &Path) -> Result<(), RunicError> {
    let bin_dir = project_root.join("bin");
    let runic_indexer_path = bin_dir.join("runic-indexer.rs");
    let context = Context::new();
    let runic_indexer_contents =
        render_template(RUNIC_INDEXER_TEMPLATE, &context)?;

    fs::write(runic_indexer_path, runic_indexer_contents)?;
    Ok(())
}
