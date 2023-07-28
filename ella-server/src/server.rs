mod auth;
mod ella;
mod flight;

use std::{net::ToSocketAddrs, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use ella_engine::engine::EllaState;
use tokio::{sync::Notify, task::JoinHandle};
use tonic::transport::{server::TcpIncoming, Server};

use crate::gen::engine_service_server::EngineServiceServer;

use self::{
    auth::{AuthProvider, ConnectionManager},
    ella::EllaEngineService,
    flight::EllaSqlService,
};

#[derive(Debug)]
pub struct EllaServer {
    handle: JoinHandle<crate::Result<()>>,
    stop: Arc<Notify>,
}

impl EllaServer {
    // TODO: this should be configurable
    const SECRET: &[u8] = b"ella";

    pub fn start<A: ToSocketAddrs>(
        server: Server,
        state: EllaState,
        addr: A,
    ) -> crate::Result<Self> {
        let auth = Arc::new(AuthProvider::from_secret(Self::SECRET)?);
        let connections = ConnectionManager::new(auth, state);

        let flight_svc = FlightServiceServer::with_interceptor(
            EllaSqlService::new(connections.clone()),
            connections.clone(),
        );
        let engine_svc =
            EngineServiceServer::with_interceptor(EllaEngineService::default(), connections);
        let stop = Arc::new(Notify::new());

        let stop_signal = stop.clone();
        let mut last_err = None;
        let mut bound = None;
        for addr in addr.to_socket_addrs()? {
            match TcpIncoming::new(addr, false, None) {
                Ok(incoming) => {
                    bound = Some(incoming);
                    break;
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
        }
        let incoming = match bound {
            Some(bound) => bound,
            None => match last_err {
                Some(err) => return Err(crate::ServerError::Transport(err).into()),
                None => {
                    return Err(crate::ServerError::transport(
                        "failed to resolve valid bind address",
                    )
                    .into())
                }
            },
        };
        let handle = tokio::spawn(async move {
            let stop = stop_signal;
            server
                .layer(tower_http::trace::TraceLayer::new_for_grpc())
                .add_service(flight_svc)
                .add_service(engine_svc)
                .serve_with_incoming_shutdown(incoming, stop.notified())
                .await
                .map_err(|err| crate::ServerError::transport(err).into())
        });
        Ok(Self { handle, stop })
    }

    pub fn cancel(&self) {
        self.stop.notify_one()
    }

    pub async fn stop(&mut self) -> crate::Result<()> {
        self.stop.notify_one();
        (&mut self.handle).await.unwrap()
    }
}

impl Drop for EllaServer {
    fn drop(&mut self) {
        self.cancel()
    }
}
