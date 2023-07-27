use crate::{
    catalog::GetCatalog,
    engine::lazy::Lazy,
    schema::GetSchema,
    server::{
        server::SynapseServer,
        tonic::transport::{Channel, Server},
    },
    table::GetTable,
};
use futures::{future::BoxFuture, FutureExt};
use std::future::IntoFuture;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use synapse_engine::{
    registry::{Id, SchemaRef, TableRef},
    table::info::TableInfo,
    SynapseConfig, SynapseContext,
};
use synapse_server::{client::SynapseClient, tonic::codegen::http::uri::InvalidUri};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Synapse {
    inner: SynapseInner,
}

#[derive(Debug, Clone)]
pub(crate) enum SynapseInner {
    Local {
        ctx: SynapseContext,
        server: Arc<Mutex<Option<SynapseServer>>>,
    },
    Remote(SynapseClient),
}

impl Synapse {
    fn new(inner: SynapseInner) -> Self {
        Self { inner }
    }

    pub async fn connect(addr: impl AsRef<str>) -> crate::Result<Self> {
        let channel =
            Channel::builder(addr.as_ref().parse().map_err(|err: InvalidUri| {
                crate::server::ClientError::InvalidUri(err.to_string())
            })?)
            .connect()
            .await?;
        let client = SynapseClient::connect(channel).await?;
        Ok(Self::new(SynapseInner::Remote(client)))
    }

    pub(crate) fn open(root: impl Into<String>) -> OpenSynapse {
        OpenSynapse {
            root: root.into(),
            serve: None,
            create: None,
        }
    }

    pub(crate) fn create(
        root: impl Into<String>,
        config: impl Into<SynapseConfig>,
    ) -> CreateSynapse {
        CreateSynapse {
            root: root.into(),
            serve: None,
            config: config.into(),
            if_not_exists: false,
        }
    }

    pub async fn shutdown(self) -> crate::Result<()> {
        match self.inner {
            SynapseInner::Local { ctx, server } => {
                let mut lock = server.lock().await;
                let res = if let Some(server) = lock.as_mut() {
                    server.stop().await
                } else {
                    Ok(())
                };
                *lock = None;
                ctx.shutdown().await?;
                res
            }
            SynapseInner::Remote(_) => Ok(()),
        }
    }

    pub async fn query(&mut self, sql: impl AsRef<str>) -> crate::Result<Lazy> {
        match &mut self.inner {
            SynapseInner::Local { ctx, .. } => ctx.query(sql.as_ref()).await,
            SynapseInner::Remote(client) => client.query(sql.as_ref()).await,
        }
    }

    pub async fn execute(&mut self, sql: impl AsRef<str>) -> crate::Result<()> {
        self.query(sql).await?.execute().await?;
        Ok(())
    }

    pub fn table<'a>(&'a self, table: impl Into<TableRef<'a>>) -> GetTable<'a> {
        GetTable::new(self, table.into())
    }

    pub fn catalog<'a>(&'a self, catalog: impl Into<Id<'a>>) -> GetCatalog<'a> {
        GetCatalog::new(self, catalog.into())
    }

    pub fn schema<'a>(&'a self, schema: impl Into<SchemaRef<'a>>) -> GetSchema<'a> {
        GetSchema::new(self, schema.into())
    }

    pub async fn use_catalog<'a>(mut self, catalog: impl Into<Id<'a>>) -> crate::Result<Self> {
        match &mut self.inner {
            SynapseInner::Local { ctx, .. } => {
                *ctx = ctx.clone().use_catalog(catalog)?;
            }
            SynapseInner::Remote(client) => client.use_catalog(catalog).await?,
        }
        Ok(self)
    }

    pub async fn use_schema<'a>(mut self, schema: impl Into<Id<'a>>) -> crate::Result<Self> {
        match &mut self.inner {
            SynapseInner::Local { ctx, .. } => {
                *ctx = ctx.clone().use_schema(schema)?;
            }
            SynapseInner::Remote(client) => client.use_schema(schema).await?,
        }
        Ok(self)
    }

    pub(crate) async fn get_table(
        &self,
        table: TableRef<'_>,
    ) -> crate::Result<Option<crate::Table>> {
        Ok(match &self.inner {
            SynapseInner::Local { ctx, .. } => ctx.table(table).map(crate::Table::local),
            SynapseInner::Remote(client) => {
                client.get_table(table).await?.map(crate::Table::remote)
            }
        })
    }

    pub(crate) async fn create_table(
        &self,
        table: TableRef<'_>,
        info: TableInfo,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<crate::Table> {
        Ok(match &self.inner {
            SynapseInner::Local { ctx, .. } => crate::Table::local(
                ctx.create_table(table, info, if_not_exists, or_replace)
                    .await?,
            ),
            SynapseInner::Remote(client) => crate::Table::remote(
                client
                    .create_table(table, info, if_not_exists, or_replace)
                    .await?,
            ),
        })
    }

    pub(crate) async fn create_catalog(
        &mut self,
        catalog: Id<'_>,
        if_not_exists: bool,
    ) -> crate::Result<()> {
        match &mut self.inner {
            SynapseInner::Local { ctx, .. } => {
                ctx.create_catalog(catalog, if_not_exists).await?;
            }
            SynapseInner::Remote(client) => {
                client.create_catalog(catalog, if_not_exists).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn create_schema(
        &mut self,
        schema: SchemaRef<'_>,
        if_not_exists: bool,
    ) -> crate::Result<()> {
        match &mut self.inner {
            SynapseInner::Local { ctx, .. } => {
                ctx.create_schema(schema, if_not_exists).await?;
            }
            SynapseInner::Remote(client) => {
                client.create_schema(schema, if_not_exists).await?;
            }
        }
        Ok(())
    }
}

#[must_use]
#[derive(Debug)]
pub struct OpenSynapse {
    root: String,
    serve: Option<Vec<SocketAddr>>,
    create: Option<SynapseConfig>,
}

impl OpenSynapse {
    pub fn or_create(mut self, config: impl Into<SynapseConfig>) -> Self {
        self.create = Some(config.into());
        self
    }

    pub fn and_serve<A: ToSocketAddrs>(mut self, addr: A) -> crate::Result<Self> {
        self.serve = Some(addr.to_socket_addrs()?.collect());
        Ok(self)
    }
}

impl IntoFuture for OpenSynapse {
    type Output = crate::Result<Synapse>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let ctx = if let Some(config) = self.create {
                crate::engine::create(&self.root, config, true).await?
            } else {
                crate::engine::open(&self.root).await?
            };
            let server = match self.serve {
                Some(addrs) => Some(SynapseServer::start(
                    Server::builder(),
                    ctx.state().clone(),
                    &addrs[..],
                )?),
                None => None,
            };
            let server = Arc::new(Mutex::new(server));
            Ok(Synapse::new(SynapseInner::Local { ctx, server }))
        }
        .boxed()
    }
}

#[must_use]
#[derive(Debug)]
pub struct CreateSynapse {
    root: String,
    serve: Option<Vec<SocketAddr>>,
    config: SynapseConfig,
    if_not_exists: bool,
}

impl CreateSynapse {
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }
}

impl IntoFuture for CreateSynapse {
    type Output = crate::Result<Synapse>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let ctx = crate::engine::create(&self.root, self.config, self.if_not_exists).await?;
            let server = match self.serve {
                Some(addrs) => Some(SynapseServer::start(
                    Server::builder(),
                    ctx.state().clone(),
                    &addrs[..],
                )?),
                None => None,
            };
            let server = Arc::new(Mutex::new(server));
            Ok(Synapse::new(SynapseInner::Local { ctx, server }))
        }
        .boxed()
    }
}
