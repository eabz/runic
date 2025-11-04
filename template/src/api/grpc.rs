use super::{ApiAdapter, ApiError, ApiHandle};
use crate::api::models::grpc::{
    self,
    indexer_server::{Indexer, IndexerServer},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;
use tonic::{Request, Response, Status};
use log::info;

#[derive(Default)]
pub struct GrpcApi;

#[derive(Debug, Clone)]
pub struct GrpcEndpoint;

impl GrpcEndpoint {
    pub fn noop_service(&self) -> IndexerServer<NoopIndexer> {
        IndexerServer::new(NoopIndexer)
    }
}

struct NoopIndexer;

#[async_trait]
impl Indexer for NoopIndexer {
    type StreamEventsStream =
        ReceiverStream<Result<grpc::EventEnvelope, Status>>;

    async fn stream_events(
        &self,
        _request: Request<grpc::SubscriptionRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        let stream = ReceiverStream::new(rx);
        // the stream completes immediately; users can replace this with real logic
        drop(tx);
        Ok(Response::new(stream))
    }
}

impl ApiAdapter for GrpcApi {
    fn backend(&self) -> &'static str {
        "grpc"
    }

    fn launch(&self) -> Result<ApiHandle, ApiError> {
        info!("gRPC stubs generated under src/api/models/indexer.rs");
        Ok(ApiHandle::Grpc(GrpcEndpoint))
    }
}
