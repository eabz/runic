use std::{fs, path::Path};

use crate::errors::RunicError;

const CARGO_TOML_TEMPLATE: &str = r#"[package]
name = "runic-indexer"
version = "0.1.0"
edition = "2024"

[dependencies]
alloy = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
toml = "0.8"
ethers = { version = "2", features = ["abigen"] }
log = "0.4"
tokio = { version = "1", features = ["full"] }
simple_logger = { version = "5", features = ["colors"] }
serde_json = "1"

[[bin]]
path = "bin/runic-indexer.rs"
name = "runic-indexer"
"#;

const CONFIG_RS_TEMPLATE: &str = include_str!("../template/src/config.rs");
const LIB_RS_TEMPLATE: &str = include_str!("../template/src/lib.rs");
const RPC_RS_TEMPLATE: &str = include_str!("../template/src/rpc.rs");
const RUNIC_INDEXER_TEMPLATE: &str =
    include_str!("../template/bin/runic-indexer.rs");

pub fn write_cargo_toml(project_root: &Path) -> Result<(), RunicError> {
    let destination = project_root.join("Cargo.toml");
    write_embedded_template(&destination, CARGO_TOML_TEMPLATE)
}

pub fn write_runic_config(project_root: &Path) -> Result<(), RunicError> {
    let destination = project_root.join("src/config.rs");
    write_embedded_template(&destination, CONFIG_RS_TEMPLATE)
}

pub fn write_runic_lib(project_root: &Path) -> Result<(), RunicError> {
    let destination = project_root.join("src/lib.rs");
    write_embedded_template(&destination, LIB_RS_TEMPLATE)
}

pub fn write_runic_rpc(project_root: &Path) -> Result<(), RunicError> {
    let destination = project_root.join("src/rpc.rs");
    write_embedded_template(&destination, RPC_RS_TEMPLATE)
}

pub fn write_runic_indexer(project_root: &Path) -> Result<(), RunicError> {
    let destination = project_root.join("bin/runic-indexer.rs");
    write_embedded_template(&destination, RUNIC_INDEXER_TEMPLATE)
}

fn write_embedded_template(
    destination: &Path,
    contents: &str,
) -> Result<(), RunicError> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(destination, contents)?;
    Ok(())
}
