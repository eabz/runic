use super::{ApiAdapter, ApiError, ApiHandle};
use crate::api::models;

#[derive(Default)]
pub struct GraphqlApi;

#[derive(Debug, Clone)]
pub struct GraphqlEndpoint {
    sdl: &'static str,
}

impl GraphqlEndpoint {
    pub fn new() -> Self {
        Self { sdl: models::graphql::SDL }
    }

    pub fn schema(&self) -> &'static str {
        self.sdl
    }
}

impl ApiAdapter for GraphqlApi {
    fn backend(&self) -> &'static str {
        "graphql"
    }

    fn launch(&self) -> Result<ApiHandle, ApiError> {
        let endpoint = GraphqlEndpoint::new();
        println!(
            "GraphQL schema available with {} bytes",
            endpoint.schema().len()
        );
        Ok(ApiHandle::Graphql(endpoint))
    }
}
