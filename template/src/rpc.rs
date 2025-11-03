use std::sync::Arc;

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
        let events = self.abi.events.clone();

        info!("Listening to {} events of the contract", events.len());
    }
}
