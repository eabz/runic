use std::{fs, path::Path};

use tera::Context;

use crate::{errors::RunicError, templates::render_template};

pub const RUNIC_RPC_TEMPLATE: &str = r#"use std::sync::Arc;

use alloy::{
    json_abi::JsonAbi,
    providers::{DynProvider, ProviderBuilder},
};
use log::info;

use crate::config::RunicConfig;

pub struct Rpc {
    pub config: Arc<RunicConfig>,
    pub abi: Arc<JsonAbi>,
    pub client: Arc<DynProvider>,
}

impl Rpc {
    /// Constructs a new RPC client using the provided configuration.
    pub async fn new(config: Arc<RunicConfig>, abi: Arc<JsonAbi>) -> Self {
        info!("Starting rpc service");

        let client = ProviderBuilder::new()
            .connect(config.network.rpc_endpoint.as_str())
            .await
            .unwrap();

        let client = DynProvider::new(client.clone());

        Self { config, client: Arc::new(client), abi }
    }

    pub async fn listen_events(&self) {
        println!("{:?}", self.abi);
        let events = self.abi.events.clone();

        info!("Listening to {} events of the contract", events.len());
    }
}

"#;

pub fn write_runic_rpc(project_root: &Path) -> Result<(), RunicError> {
    let bin_dir = project_root.join("src");
    let runic_rpc_path = bin_dir.join("rpc.rs");
    let context = Context::new();
    let runic_rpc_contents =
        render_template(RUNIC_RPC_TEMPLATE, &context)?;

    fs::write(runic_rpc_path, runic_rpc_contents)?;
    Ok(())
}
