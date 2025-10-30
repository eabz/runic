use std::{fs, path::Path};

use tera::Context;

use crate::{errors::RunicError, templates::render_template};

pub const CARGO_TOML_TEMPLATE: &str = r#"[package]
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

pub fn write_cargo_toml(project_root: &Path) -> Result<(), RunicError> {
    let cargo_toml_path = project_root.join("Cargo.toml");
    let context = Context::new();
    let cargo_toml_contents =
        render_template(CARGO_TOML_TEMPLATE, &context)?;

    fs::write(cargo_toml_path, cargo_toml_contents)?;
    Ok(())
}
