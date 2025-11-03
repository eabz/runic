use super::{ApiAdapter, ApiError, ApiHandle};

#[derive(Default)]
pub struct GraphqlApi;

#[derive(Debug, Clone)]
pub struct GraphqlEndpoint {
    url: &'static str,
}

impl GraphqlEndpoint {
    pub fn new() -> Self {
        Self { url: "http://127.0.0.1:4000/graphql" }
    }

    pub fn url(&self) -> &'static str {
        self.url
    }
}

impl ApiAdapter for GraphqlApi {
    fn backend(&self) -> &'static str {
        "graphql"
    }

    fn launch(&self) -> Result<ApiHandle, ApiError> {
        println!(
            "Starting GraphQL service on {} (placeholder implementation)",
            GraphqlEndpoint::new().url()
        );
        Ok(ApiHandle::Graphql(GraphqlEndpoint::new()))
    }
}
