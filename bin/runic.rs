use alloy::{json_abi::JsonAbi, primitives::Address};
use clap::Parser;
use dialoguer::{Confirm, Input, Select};
use runic::{
    config::{
        API, ChildContractConfig, Database, RunicConfig, write_config,
    },
    errors::RunicError,
    templates::{
        write_cargo_toml, write_runic_config, write_runic_indexer,
        write_runic_lib, write_runic_rpc,
    },
    utils::{
        SimplePathCompletion, load_json_abi, print_banner, print_section,
    },
};
use std::{
    fs,
    path::{Path, PathBuf},
    process,
    str::FromStr,
};

#[derive(Parser)]
#[command(
    name = "runic",
    author,
    version,
    about = "Scaffold an indexer project from a contract ABI.",
    long_about = "Generate boilerplate code and configuration for a Runic indexer using interactive prompts."
)]
struct RunicArgs;

pub struct RunicDefaults {
    pub default_api: API,
    pub default_db: Database,
    pub default_start_block: i64,
    pub default_contract: String,
}

impl Default for RunicDefaults {
    fn default() -> Self {
        Self {
            default_api: API::Graphql,
            default_db: Database::Redb,
            default_start_block: 0,
            default_contract: String::from(
                "0x0000000000000000000000000000000000000000",
            ),
        }
    }
}

fn main() {
    RunicArgs::parse();

    let defaults = RunicDefaults::default();

    if let Err(err) = scaffold(defaults) {
        eprintln!("Scaffolding failed: {err}");
        process::exit(1);
    }
}

pub fn scaffold(defaults: RunicDefaults) -> Result<(), RunicError> {
    print_banner("Runic Indexer Scaffolder");

    let (folder_name, project_root) = prompt_project_folder()?;

    let contract_address =
        prompt_contract_address(defaults.default_contract)?;

    let abi_path =
        prompt_existing_json_path("Path to the contract ABI JSON")?;
    let parsed_abi = load_json_abi(&abi_path)?;

    println!("[ok] loaded ABI from {}", abi_path.display());

    let start_block = prompt_start_block(defaults.default_start_block)?;

    let selected_db = prompt_database(defaults.default_db)?;

    let selected_api = prompt_api(defaults.default_api)?;

    let (child_contract, child_abi_source) =
        prompt_child_contract_tracking(&parsed_abi)?;

    let child_contract_for_summary = child_contract.clone();

    fs::create_dir_all(&project_root)?;

    let config_path = project_root.join("Config.toml");

    let config = RunicConfig::new(
        contract_address.clone(),
        start_block,
        selected_api,
        selected_db,
        child_contract,
    );

    write_config(&config_path, &config)?;

    create_project_layout(&project_root)?;

    write_cargo_toml(&project_root)?;
    write_runic_indexer(&project_root)?;
    write_runic_config(&project_root)?;
    write_runic_lib(&project_root)?;
    write_runic_rpc(&project_root)?;

    let abi_dir = project_root.join("src").join("abi");

    let primary_abi_target = abi_dir.join("abi.json");
    fs::copy(&abi_path, &primary_abi_target)?;

    let child_target = abi_dir.join("child-abi.json");
    if let Some(child_source) = &child_abi_source {
        fs::copy(child_source, &child_target)?;
        println!("[ok] Copied child ABI to {}", child_target.display());
    }

    print_section("Summary");
    println!("- Project folder: {}", folder_name);
    println!("- Output path: {}", project_root.display());
    println!("- Contract address: {}", contract_address);
    println!("- Start block: {}", start_block);
    println!("- Database engine: {}", selected_db);
    println!("- API surface: {}", selected_api);
    println!("- ABI source: {}", abi_path.display());

    if let Some(child_contract) = &child_contract_for_summary {
        println!("- Child event: {}", child_contract.event_signature);
    }

    println!();
    println!(
        "Project created at `{}`. You can now build and run your indexer.",
        project_root.display()
    );

    Ok(())
}

fn create_project_layout(project_root: &Path) -> Result<(), RunicError> {
    let bin_dir = project_root.join("bin");
    let src_dir = project_root.join("src");
    let abi_dir = src_dir.join("abi");

    for dir in [&bin_dir, &src_dir, &abi_dir] {
        fs::create_dir_all(dir)?;
    }

    Ok(())
}

fn prompt_project_folder() -> Result<(String, PathBuf), RunicError> {
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

fn prompt_contract_address(default: String) -> Result<String, RunicError> {
    let prompt = format!("Contract address:");

    let input: String = Input::new()
        .with_prompt(prompt)
        .allow_empty(true)
        .default(default.to_owned())
        .show_default(true)
        .validate_with(|value: &String| -> Result<(), String> {
            let trimmed = value.trim();

            if trimmed.is_empty() || trimmed == default {
                return Ok(());
            }

            Address::from_str(trimmed).map(|_| ()).map_err(|_| {
                "Please enter a valid Ethereum address.".to_owned()
            })
        })
        .interact_text()?;

    let trimmed = input.trim();

    if trimmed.is_empty() || trimmed == default {
        Ok(default.to_owned())
    } else {
        let address = Address::from_str(trimmed)
            .expect("address validated by dialoguer");
        Ok(address.to_checksum(None))
    }
}

fn prompt_existing_json_path(prompt: &str) -> Result<PathBuf, RunicError> {
    let input: String = Input::new()
        .with_prompt(prompt)
        .completion_with(&SimplePathCompletion::default())
        .validate_with(|value: &String| -> Result<(), String> {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err("Path cannot be empty.".to_owned());
            }

            let path = PathBuf::from(trimmed);
            if !path.exists() {
                return Err(format!("File `{trimmed}` does not exist."));
            }
            if path.is_dir() {
                return Err(
                    "Expected a file path, but found a directory."
                        .to_owned(),
                );
            }
            let is_json = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("json"))
                .unwrap_or(false);
            if !is_json {
                return Err("Path must point to a .json file.".to_owned());
            }
            Ok(())
        })
        .interact_text()?;

    Ok(PathBuf::from(input.trim()))
}

fn prompt_start_block(default: i64) -> Result<i64, RunicError> {
    let block: i64 = Input::new()
        .with_prompt("Starting block")
        .default(default)
        .show_default(true)
        .validate_with(|value: &i64| -> Result<(), String> {
            if *value < 0 {
                Err("Start block cannot be negative.".to_owned())
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    Ok(block)
}

fn prompt_database(default: Database) -> Result<Database, RunicError> {
    let options = [Database::Redb];
    let labels: Vec<String> =
        options.iter().map(|db| db.to_string()).collect();
    let default_index =
        options.iter().position(|&db| db == default).unwrap_or(0);
    let selected = Select::new()
        .with_prompt("Database engine")
        .items(&labels)
        .default(default_index)
        .interact()?;

    Ok(options[selected])
}

fn prompt_api(default: API) -> Result<API, RunicError> {
    let options = [API::Graphql];
    let labels: Vec<String> =
        options.iter().map(|api| api.to_string()).collect();
    let default_index =
        options.iter().position(|&api| api == default).unwrap_or(0);
    let selected = Select::new()
        .with_prompt("API surface")
        .items(&labels)
        .default(default_index)
        .interact()?;

    Ok(options[selected])
}

fn prompt_child_contract_tracking(
    abi: &JsonAbi,
) -> Result<(Option<ChildContractConfig>, Option<PathBuf>), RunicError> {
    let track_children = Confirm::new()
        .with_prompt(
            "Does this contract create child contracts that need tracking?",
        )
        .default(false)
        .interact()?;

    if !track_children {
        return Ok((None, None));
    }

    let mut event_options: Vec<String> = abi
        .events
        .values()
        .flat_map(|events| {
            events.iter().map(|event| {
                let params = event
                    .inputs
                    .iter()
                    .map(|input| input.ty.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                if params.is_empty() {
                    format!("{}()", event.name)
                } else {
                    format!("{}({})", event.name, params)
                }
            })
        })
        .collect();

    event_options.sort();
    event_options.dedup();

    if event_options.is_empty() {
        return Err(RunicError::Abi(
            "The provided ABI does not contain any events to monitor for child contracts."
                .to_owned(),
        ));
    }

    let selected_index = Select::new()
        .with_prompt("Select the event to track for child contracts")
        .items(&event_options)
        .max_length(8)
        .interact()?;

    let selected_event = event_options
        .get(selected_index)
        .cloned()
        .expect("selection index should be valid");

    let child_source =
        prompt_existing_json_path("Path to the child contract ABI JSON")?;

    let _ = load_json_abi(&child_source)?;

    let child_config = ChildContractConfig {
        event_signature: selected_event,
        abi_path: "src/abi/child-abi.json".to_owned(),
    };

    Ok((Some(child_config), Some(child_source)))
}
