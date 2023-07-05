use crate::{
    engine::{catalog::TopicId, lazy::Lazy, Engine, EngineConfig},
    server::{
        server::SynapseServer,
        tonic::transport::{Channel, Server},
    },
    topic::TopicRef,
};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use synapse_server::client::SynapseClient;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Synapse(pub(crate) SynapseInner);

#[derive(Debug, Clone)]
pub(crate) enum SynapseInner {
    Local {
        engine: Engine,
        server: Arc<Mutex<Option<SynapseServer>>>,
    },
    Remote(SynapseClient),
}

impl Synapse {
    pub async fn connect(addr: &str) -> crate::Result<Self> {
        let channel = Channel::builder(
            addr.parse()
                .map_err(|_| crate::server::ClientError::InvalidUri(addr.to_string()))?,
        )
        .connect()
        .await?;
        Ok(Self(SynapseInner::Remote(SynapseClient::new(channel))))
    }

    pub fn start(root: impl Into<String>) -> EngineBuilder {
        EngineBuilder {
            root: root.into(),
            config: None,
            addr: None,
        }
    }

    pub async fn shutdown(&self) -> crate::Result<()> {
        match &self.0 {
            SynapseInner::Local { engine, server } => {
                let mut lock = server.lock().await;
                let res = if let Some(server) = lock.as_mut() {
                    server.stop().await
                } else {
                    Ok(())
                };
                *lock = None;
                engine.shutdown().await?;
                res
            }
            SynapseInner::Remote(_) => Ok(()),
        }
    }

    pub async fn query(&mut self, sql: &str) -> crate::Result<Lazy> {
        match &mut self.0 {
            SynapseInner::Local { engine, .. } => engine.query(sql).await,
            SynapseInner::Remote(client) => client.query(sql).await,
        }
    }

    pub fn topic(&self, topic: impl Into<TopicId>) -> TopicRef<'_> {
        TopicRef::new(self, topic.into())
    }
}

#[derive(Debug, Clone)]
pub struct EngineBuilder {
    root: String,
    config: Option<EngineConfig>,
    addr: Option<SocketAddr>,
}

impl EngineBuilder {
    pub fn config(&mut self, config: EngineConfig) -> &mut Self {
        self.config = Some(config);
        self
    }

    pub fn serve<A: ToSocketAddrs>(&mut self, addr: A) -> crate::Result<&mut Self> {
        let addr = addr.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::AddrNotAvailable, "invalid socket address")
        })?;
        self.addr = Some(addr);
        Ok(self)
    }

    pub async fn build(&self) -> crate::Result<Synapse> {
        let engine =
            Engine::start_with_config(&self.root, self.config.clone().unwrap_or_default()).await?;
        let server = self
            .addr
            .map(|addr| SynapseServer::start(Server::builder(), engine.clone(), addr));
        let server = Arc::new(Mutex::new(server));
        Ok(Synapse(SynapseInner::Local { engine, server }))
    }
}
