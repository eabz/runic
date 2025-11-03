use crate::config::EngineConfig;
use std::{error::Error, fmt};

pub mod capnproto;
pub mod graphql;
pub mod grpc;

#[derive(Debug)]
pub enum ApiError {
    UnsupportedBackend(String),
    NotImplemented(&'static str),
    Startup(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::UnsupportedBackend(name) => {
                write!(f, "Unsupported API backend `{name}`")
            }
            ApiError::NotImplemented(name) => {
                write!(f, "{name} is not implemented yet")
            }
            ApiError::Startup(msg) => f.write_str(msg),
        }
    }
}

impl Error for ApiError {}

pub enum ApiHandle {
    Graphql(graphql::GraphqlEndpoint),
    Grpc(grpc::GrpcEndpoint),
    Capnproto(capnproto::CapnprotoEndpoint),
}

pub trait ApiAdapter: Send + Sync {
    fn backend(&self) -> &'static str;
    fn launch(&self) -> Result<ApiHandle, ApiError>;
}

pub struct ApiService {
    adapter: Box<dyn ApiAdapter>,
}

impl ApiService {
    pub fn from_config(engine: &EngineConfig) -> Result<Self, ApiError> {
        let adapter: Box<dyn ApiAdapter> =
            match engine.api.to_lowercase().as_str() {
                "graphql" => Box::new(graphql::GraphqlApi::default()),
                "grpc" => Box::new(grpc::GrpcApi::default()),
                "capnproto" | "cap'n proto" => {
                    Box::new(capnproto::CapnprotoApi::default())
                }
                other => {
                    return Err(ApiError::UnsupportedBackend(
                        other.to_owned(),
                    ));
                }
            };

        Ok(Self { adapter })
    }

    pub fn backend(&self) -> &'static str {
        self.adapter.backend()
    }

    pub fn launch(&self) -> Result<ApiHandle, ApiError> {
        self.adapter.launch()
    }
}
