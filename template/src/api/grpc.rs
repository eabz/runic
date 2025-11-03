use super::{ApiAdapter, ApiError, ApiHandle};

#[derive(Default)]
pub struct GrpcApi;

#[derive(Debug, Clone)]
pub struct GrpcEndpoint;

impl ApiAdapter for GrpcApi {
    fn backend(&self) -> &'static str {
        "grpc"
    }

    fn launch(&self) -> Result<ApiHandle, ApiError> {
        Err(ApiError::NotImplemented("gRPC transport is not wired yet"))
    }
}
