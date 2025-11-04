use std::{fs, process, sync::Arc};

use alloy::json_abi::JsonAbi;
use log::{error, info};
use runic_indexer::{
    api::{ApiHandle, ApiService},
    config::RunicConfig,
    db::{Database, DatabaseHandle},
    rpc::Rpc,
};
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
    info!(
        "Starting indexer configured for contract {} from block {}",
        config.contract.address, config.contract.start_block
    );

    if config.network.rpc_endpoint.trim().is_empty() {
        error!(
            "Config error: network.rpc_endpoint is empty. Please add it to Config.toml."
        );
        process::exit(1);
    }

    if matches!(
        config.engines.db.to_lowercase().as_str(),
        "postgres" | "postgresql"
    ) && config.database.uri.trim().is_empty()
    {
        error!(
            "Config error: database.uri is empty for the Postgres backend. Please add it to Config.toml."
        );
        process::exit(1);
    }

    let config = Arc::new(config);

    let database = Database::connect(&config).unwrap_or_else(|err| {
        error!("Failed to initialize database backend: {err}");
        process::exit(1);
    });

    match database.handle() {
        DatabaseHandle::Sqlite(_) => {
            info!("Connected to SQLite datastore");
        }
        DatabaseHandle::Postgres(_) => {
            info!("Connected to Postgres database");
        }
    }

    let api_service = ApiService::from_config(&config.engines)
        .unwrap_or_else(|err| {
            error!("Failed to configure API backend: {err}");
            process::exit(1);
        });

    let api_handle = api_service.launch().unwrap_or_else(|err| {
        error!(
            "API backend `{}` failed to launch: {err}",
            api_service.backend()
        );
        process::exit(1);
    });

    if let ApiHandle::Graphql(endpoint) = &api_handle {
        info!("GraphQL schema generated ({} bytes)", endpoint.schema().len());
    }

    let abi_txt = fs::read_to_string("src/abi/abi.json")
        .expect("unable to decode abi file");

    let abi: JsonAbi =
        serde_json::from_str(&abi_txt).expect("unable to parse abi json");

    let rpc = Rpc::new(Arc::clone(&config), Arc::new(abi)).await;

    rpc.listen_events().await;
}
