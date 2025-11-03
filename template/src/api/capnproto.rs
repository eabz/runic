use super::{ApiAdapter, ApiError, ApiHandle};

#[derive(Default)]
pub struct CapnprotoApi;

#[derive(Debug, Clone)]
pub struct CapnprotoEndpoint;

impl ApiAdapter for CapnprotoApi {
    fn backend(&self) -> &'static str {
        "capnproto"
    }

    fn launch(&self) -> Result<ApiHandle, ApiError> {
        Err(ApiError::NotImplemented(
            "Cap'n Proto transport is not wired yet",
        ))
    }
}
