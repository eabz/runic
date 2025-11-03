use std::{fs, process, sync::Arc};

use alloy::json_abi::JsonAbi;
use runic_indexer::{
    api::{ApiHandle, ApiService},
    config::RunicConfig,
    db::{Database, DatabaseConnection},
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

    let config = Arc::new(config);

    let database =
        Database::from_config(&config.engines).unwrap_or_else(|err| {
            eprintln!("Failed to initialize database backend: {err}");
            process::exit(1);
        });

    let db_connection = database.connect().unwrap_or_else(|err| {
        eprintln!(
            "Database backend `{}` failed to connect: {err}",
            database.backend()
        );
        process::exit(1);
    });

    match &db_connection {
        DatabaseConnection::Sqlite(_) => {
            println!("Connected to SQLite datastore");
        }
        DatabaseConnection::Postgres(_) => {
            println!("Connected to Postgres database");
        }
    }

    let api_service = ApiService::from_config(&config.engines)
        .unwrap_or_else(|err| {
            eprintln!("Failed to configure API backend: {err}");
            process::exit(1);
        });

    let api_handle = api_service.launch().unwrap_or_else(|err| {
        eprintln!(
            "API backend `{}` failed to launch: {err}",
            api_service.backend()
        );
        process::exit(1);
    });

    if let ApiHandle::Graphql(endpoint) = &api_handle {
        println!("GraphQL endpoint running at {}", endpoint.url());
    }

    let abi_txt = fs::read_to_string("src/abi/abi.json")
        .expect("unable to decode abi file");

    let abi: JsonAbi =
        serde_json::from_str(&abi_txt).expect("unable to parse abi json");
    let rpc = Rpc::new(Arc::clone(&config), Arc::new(abi)).await;

    rpc.listen_events().await;
}
