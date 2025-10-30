use std::{fs, path::Path};

use tera::Context;

use crate::{errors::ScaffoldError, templates::render_template};

pub const CARGO_TOML_TEMPLATE: &str = r#"[package]
name = "{{ package_name }}"
version = "0.1.0"
edition = "2024"
[dependencies]
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"
ethers = { version = "2", features = ["abigen"] }

[[bin]]
path = "bin/runic-indexer.rs"
name = "runic-indexer"
"#;

pub fn write_cargo_toml(
    project_root: &Path,
    crate_name: &str,
) -> Result<(), ScaffoldError> {
    let cargo_toml_path = project_root.join("Cargo.toml");
    let mut context = Context::new();
    context.insert("package_name", crate_name);

    let cargo_toml_contents =
        render_template(CARGO_TOML_TEMPLATE, &context)?;

    fs::write(cargo_toml_path, cargo_toml_contents)?;
    Ok(())
}
