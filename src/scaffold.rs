use crate::config::{API, Database, RunicConfig};
use log::info;
use std::{
    fmt, fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

pub struct ScaffoldSettings {
    pub abi: String,
    pub api: API,
    pub db: Database,
    pub start_block: i64,
}

#[derive(Debug)]
pub enum ScaffoldError {
    Io(io::Error),
    Serialization(toml::ser::Error),
}

impl fmt::Display for ScaffoldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScaffoldError::Io(err) => write!(f, "io error: {err}"),
            ScaffoldError::Serialization(err) => {
                write!(f, "failed to serialize configuration: {err}")
            }
        }
    }
}

impl std::error::Error for ScaffoldError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ScaffoldError::Io(err) => Some(err),
            ScaffoldError::Serialization(err) => Some(err),
        }
    }
}

impl From<io::Error> for ScaffoldError {
    fn from(err: io::Error) -> Self {
        ScaffoldError::Io(err)
    }
}

impl From<toml::ser::Error> for ScaffoldError {
    fn from(err: toml::ser::Error) -> Self {
        ScaffoldError::Serialization(err)
    }
}

pub fn run(settings: ScaffoldSettings) -> Result<(), ScaffoldError> {
    println!();
    println!("Project configuration");
    println!("---------------------");

    let (folder_name, project_root) = prompt_project_folder()?;
    let contract_address = prompt_contract_address()?;

    info!(
        "Scaffolding indexer for ABI {} into `{}`",
        settings.abi, folder_name
    );

    let config_path = project_root.join("Config.toml");
    let config = RunicConfig::new(
        contract_address.clone(),
        settings.start_block,
        settings.api,
        settings.db,
    );

    write_config(&config_path, &config)?;

    create_project_layout(&project_root)?;

    let crate_name = crate_name_from_folder(&folder_name);

    write_cargo_toml(&project_root, &crate_name)?;
    write_runic_indexer(&project_root)?;
    write_library_files(&project_root)?;

    info!(
        "Project created at `{}`. You can now build and run your indexer.",
        project_root.display()
    );

    Ok(())
}

fn prompt_project_folder() -> Result<(String, PathBuf), ScaffoldError> {
    loop {
        print!("Project folder name: ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim();

        if trimmed.is_empty() {
            println!("Folder name cannot be empty. Please try again.");
            continue;
        }

        let folder_name = trimmed.to_owned();
        let project_root = PathBuf::from(&folder_name);

        if project_root.exists() {
            println!(
                "Folder `{folder_name}` already exists and cannot be used. Please choose a different name."
            );
            continue;
        }

        fs::create_dir_all(&project_root)?;

        return Ok((folder_name, project_root));
    }
}

fn prompt_contract_address() -> Result<String, ScaffoldError> {
    const DEFAULT_CONTRACT: &str =
        "0x0000000000000000000000000000000000000000";

    print!(
        "Contract address (press Enter for default 0x000…0000, can edit later): "
    );
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let trimmed = input.trim();

    if trimmed.is_empty() {
        Ok(DEFAULT_CONTRACT.to_owned())
    } else {
        Ok(trimmed.to_owned())
    }
}

fn write_config(
    config_path: &Path,
    config: &RunicConfig,
) -> Result<(), ScaffoldError> {
    let config_contents = toml::to_string_pretty(config)?;
    fs::write(config_path, config_contents)?;
    Ok(())
}

fn create_project_layout(
    project_root: &Path,
) -> Result<(), ScaffoldError> {
    let bin_dir = project_root.join("bin");
    let src_dir = project_root.join("src");
    let abi_dir = src_dir.join("abi");

    for dir in [&bin_dir, &src_dir, &abi_dir] {
        fs::create_dir_all(dir)?;
    }

    Ok(())
}

fn crate_name_from_folder(folder_name: &str) -> String {
    let sanitized: String = folder_name
        .chars()
        .map(|c| {
            let lower = c.to_ascii_lowercase();
            if lower.is_ascii_alphanumeric()
                || lower == '-'
                || lower == '_'
            {
                lower
            } else {
                '-'
            }
        })
        .collect();

    let trimmed = sanitized.trim_matches('-');

    let mut crate_name = if trimmed.is_empty() {
        "runic-indexer".to_owned()
    } else {
        trimmed.to_owned()
    };

    if crate_name
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        crate_name = format!("runic-{crate_name}");
    }

    crate_name
}

fn write_cargo_toml(
    project_root: &Path,
    crate_name: &str,
) -> Result<(), ScaffoldError> {
    let cargo_toml_path = project_root.join("Cargo.toml");
    let cargo_toml_contents = format!(
        "[package]\nname = \"{crate_name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[dependencies]\nserde = {{ version = \"1.0\", features = [\"derive\"] }}\ntoml = \"0.8\"\n"
    );

    fs::write(cargo_toml_path, cargo_toml_contents)?;
    Ok(())
}

fn write_runic_indexer(project_root: &Path) -> Result<(), ScaffoldError> {
    let bin_dir = project_root.join("bin");
    let runic_indexer_path = bin_dir.join("runic-indexer.rs");
    let runic_indexer_contents = r#"use std::fs;

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

    fs::write(runic_indexer_path, runic_indexer_contents)?;
    Ok(())
}

fn write_library_files(project_root: &Path) -> Result<(), ScaffoldError> {
    let src_dir = project_root.join("src");
    let lib_rs_path = src_dir.join("lib.rs");
    let abi_dir = src_dir.join("abi");
    let abi_mod_path = abi_dir.join("mod.rs");

    fs::write(lib_rs_path, "pub mod abi;\n")?;
    fs::write(
        abi_mod_path,
        "// ABI bindings can be added to this module.\n",
    )?;

    Ok(())
}
