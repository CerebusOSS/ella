use std::{net::SocketAddr, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use synapse_engine::Engine;
use tokio::{sync::Notify, task::JoinHandle};
use tonic::transport::Server;

use crate::gen::engine_service_server::EngineServiceServer;

use self::{flight::SynapseSqlService, synapse::SynapseEngineService};

mod flight;
mod synapse;

#[derive(Debug)]
pub struct SynapseServer {
    handle: JoinHandle<crate::Result<()>>,
    stop: Arc<Notify>,
}

impl SynapseServer {
    pub fn start(server: Server, engine: Engine, addr: SocketAddr) -> Self {
        let flight_svc = FlightServiceServer::new(SynapseSqlService::new(engine.clone()));
        let engine_svc = EngineServiceServer::new(SynapseEngineService::new(engine));
        let stop = Arc::new(Notify::new());

        let stop_signal = stop.clone();
        let handle = tokio::spawn(async move {
            let stop = stop_signal;
            server
                .layer(tower_http::trace::TraceLayer::new_for_grpc())
                .add_service(flight_svc)
                .add_service(engine_svc)
                .serve_with_shutdown(addr, stop.notified())
                .await
                .map_err(|err| crate::ServerError::transport(err).into())
        });
        Self { handle, stop }
    }

    pub fn cancel(&self) {
        self.stop.notify_one()
    }

    pub async fn stop(mut self) -> crate::Result<()> {
        self.stop.notify_one();
        (&mut self.handle).await.unwrap()
    }
}

impl Drop for SynapseServer {
    fn drop(&mut self) {
        self.cancel()
    }
}
