use crate::{
    config::{API, Database, RunicConfig},
    errors::ScaffoldError,
    generate::generate_abi_bindings,
    templates::{
        cargo::write_cargo_toml, config::write_runic_config,
        indexer::write_runic_indexer, lib::write_runic_lib,
        rpc::write_runic_rpc,
    },
};
use dialoguer::Input;
use ethers_core::{types::Address, utils::to_checksum};
use log::info;
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

pub struct ScaffoldSettings {
    pub abi: String,
    pub api: API,
    pub db: Database,
    pub start_block: i64,
}

pub fn scaffold(settings: ScaffoldSettings) -> Result<(), ScaffoldError> {
    println!();
    println!("Project configuration");
    println!("---------------------");

    let (folder_name, project_root) = prompt_project_folder()?;
    let contract_address = prompt_contract_address()?;

    info!(
        "Scaffolding indexer for ABI {} into `{}`",
        settings.abi, folder_name
    );

    fs::create_dir_all(&project_root)?;

    let config_path = project_root.join("Config.toml");

    let config = RunicConfig::new(
        contract_address.clone(),
        settings.start_block,
        settings.api,
        settings.db,
    );

    write_config(&config_path, &config)?;

    create_project_layout(&project_root)?;

    write_cargo_toml(&project_root)?;
    write_runic_indexer(&project_root)?;
    write_runic_config(&project_root)?;
    write_runic_lib(&project_root)?;
    write_runic_rpc(&project_root)?;

    generate_abi_bindings(&project_root, &settings.abi)?;

    info!(
        "Project created at `{}`. You can now build and run your indexer.",
        project_root.display()
    );

    Ok(())
}

fn prompt_project_folder() -> Result<(String, PathBuf), ScaffoldError> {
    loop {
        let folder_name: String = Input::new()
            .with_prompt("Project folder name")
            .validate_with(|input: &String| -> Result<(), &str> {
                if input.trim().is_empty() {
                    Err("Folder name cannot be empty.")
                } else {
                    Ok(())
                }
            })
            .interact_text()?;

        let folder_name = folder_name.trim().to_owned();
        let project_root = PathBuf::from(&folder_name);

        if project_root.exists() {
            println!(
                "Folder `{folder_name}` already exists and cannot be used. Please choose a different name."
            );
            continue;
        }

        return Ok((folder_name, project_root));
    }
}

fn prompt_contract_address() -> Result<String, ScaffoldError> {
    const DEFAULT_CONTRACT: &str =
        "0x0000000000000000000000000000000000000000";

    let prompt = format!("Contract address:");

    let input: String = Input::new()
        .with_prompt(prompt)
        .allow_empty(true)
        .default(DEFAULT_CONTRACT.to_owned())
        .show_default(true)
        .validate_with(|value: &String| -> Result<(), String> {
            let trimmed = value.trim();

            if trimmed.is_empty() || trimmed == DEFAULT_CONTRACT {
                return Ok(());
            }

            Address::from_str(trimmed).map(|_| ()).map_err(|_| {
                "Please enter a valid Ethereum address.".to_owned()
            })
        })
        .interact_text()?;

    let trimmed = input.trim();

    if trimmed.is_empty() || trimmed == DEFAULT_CONTRACT {
        Ok(DEFAULT_CONTRACT.to_owned())
    } else {
        let address = Address::from_str(trimmed)
            .expect("address validated by dialoguer");
        Ok(to_checksum(&address, None))
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

    for dir in [&bin_dir, &src_dir] {
        fs::create_dir_all(dir)?;
    }

    Ok(())
}
