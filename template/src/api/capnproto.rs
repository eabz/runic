use super::{ApiAdapter, ApiError, ApiHandle};
use capnp::message::{Builder, HeapAllocator};

#[derive(Default)]
pub struct CapnprotoApi;

#[derive(Debug, Clone)]
pub struct CapnprotoEndpoint {
    schema_path: &'static str,
}

impl CapnprotoEndpoint {
    pub fn new() -> Self {
        Self { schema_path: "src/api/models/indexer.capnp" }
    }

    pub fn schema_path(&self) -> &'static str {
        self.schema_path
    }

    pub fn message_builder(&self) -> Builder<HeapAllocator> {
        Builder::new_default()
    }
}

impl ApiAdapter for CapnprotoApi {
    fn backend(&self) -> &'static str {
        "capnproto"
    }

    fn launch(&self) -> Result<ApiHandle, ApiError> {
        println!(
            "Cap'n Proto schema available at src/api/models/indexer.capnp"
        );
        Ok(ApiHandle::Capnproto(CapnprotoEndpoint::new()))
    }
}
