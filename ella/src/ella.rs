use crate::{
    catalog::GetCatalog,
    engine::lazy::Lazy,
    schema::GetSchema,
    server::{
        server::EllaServer,
        tonic::transport::{Channel, Server},
    },
    table::GetTable,
    Config,
};
use ella_engine::{
    registry::{Id, SchemaRef, TableRef},
    table::info::TableInfo,
    EllaContext,
};
use ella_server::{client::EllaClient, tonic::codegen::http::uri::InvalidUri};
use futures::{future::BoxFuture, FutureExt};
use std::future::IntoFuture;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Ella {
    inner: EllaInner,
}

#[derive(Debug, Clone)]
pub(crate) enum EllaInner {
    Local {
        ctx: EllaContext,
        server: Arc<Mutex<Option<EllaServer>>>,
    },
    Remote(EllaClient),
}

impl Ella {
    fn new(inner: EllaInner) -> Self {
        Self { inner }
    }

    pub(crate) async fn connect(addr: impl AsRef<str>) -> crate::Result<Self> {
        let channel =
            Channel::builder(addr.as_ref().parse().map_err(|err: InvalidUri| {
                crate::server::ClientError::InvalidUri(err.to_string())
            })?)
            .connect()
            .await?;
        let client = EllaClient::connect(channel).await?;
        Ok(Self::new(EllaInner::Remote(client)))
    }

    pub(crate) fn open(root: impl Into<String>) -> OpenElla {
        OpenElla {
            root: root.into(),
            serve: None,
            create: None,
        }
    }

    pub(crate) fn create(root: impl Into<String>, config: impl Into<Config>) -> CreateElla {
        CreateElla {
            root: root.into(),
            serve: None,
            config: config.into(),
            if_not_exists: false,
        }
    }

    pub async fn shutdown(self) -> crate::Result<()> {
        use EllaInner::*;
        match self.inner {
            Local { ctx, server } => {
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
            Remote(_) => Ok(()),
        }
    }

    pub async fn query(&self, sql: impl AsRef<str>) -> crate::Result<Lazy> {
        use EllaInner::*;
        match &self.inner {
            Local { ctx, .. } => ctx.query(sql.as_ref()).await,
            Remote(client) => client.query(sql.as_ref()).await,
        }
    }

    /// Execute a SQL statement on the datastore.
    ///
    /// This is shorthand for `self.query("<cmd>").execute()`.
    pub async fn execute(&self, sql: impl AsRef<str>) -> crate::Result<()> {
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

    /// Set `catalog` as the default catalog for the current context.
    ///
    /// This is broadly equivalent to the SQL statement `USE CATALOG <catalog>`.
    pub async fn use_catalog<'a>(mut self, catalog: impl Into<Id<'a>>) -> crate::Result<Self> {
        use EllaInner::*;
        match &mut self.inner {
            Local { ctx, .. } => {
                *ctx = ctx.clone().use_catalog(catalog)?;
            }
            Remote(client) => client.use_catalog(catalog).await?,
        }
        Ok(self)
    }

    /// Set `schema` as the default schema for the current context.
    ///
    /// This is broadly equivalent to the SQL statement `USE SCHEMA <schema>`.
    pub async fn use_schema<'a>(mut self, schema: impl Into<Id<'a>>) -> crate::Result<Self> {
        use EllaInner::*;
        match &mut self.inner {
            Local { ctx, .. } => {
                *ctx = ctx.clone().use_schema(schema)?;
            }
            Remote(client) => client.use_schema(schema).await?,
        }
        Ok(self)
    }

    pub fn config(&self) -> Config {
        use EllaInner::*;
        match &self.inner {
            Local { ctx, .. } => ctx.config().clone(),
            Remote(client) => client.config(),
        }
    }

    pub fn default_catalog(&self) -> Id<'static> {
        use EllaInner::*;
        match &self.inner {
            Local { ctx, .. } => ctx.default_catalog().clone(),
            Remote(client) => client.default_catalog(),
        }
    }

    pub fn default_schema(&self) -> Id<'static> {
        use EllaInner::*;
        match &self.inner {
            Local { ctx, .. } => ctx.default_schema().clone(),
            Remote(client) => client.default_schema(),
        }
    }

    pub(crate) async fn get_table(
        &self,
        table: TableRef<'_>,
    ) -> crate::Result<Option<crate::Table>> {
        Ok(match &self.inner {
            EllaInner::Local { ctx, .. } => ctx.table(table).map(crate::Table::local),
            EllaInner::Remote(client) => client.get_table(table).await?.map(crate::Table::remote),
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
            EllaInner::Local { ctx, .. } => crate::Table::local(
                ctx.create_table(table, info, if_not_exists, or_replace)
                    .await?,
            ),
            EllaInner::Remote(client) => crate::Table::remote(
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
            EllaInner::Local { ctx, .. } => {
                ctx.create_catalog(catalog, if_not_exists).await?;
            }
            EllaInner::Remote(client) => {
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
            EllaInner::Local { ctx, .. } => {
                ctx.create_schema(schema, if_not_exists).await?;
            }
            EllaInner::Remote(client) => {
                client.create_schema(schema, if_not_exists).await?;
            }
        }
        Ok(())
    }
}

#[must_use]
#[derive(Debug)]
pub struct OpenElla {
    root: String,
    serve: Option<Vec<SocketAddr>>,
    create: Option<Config>,
}

impl OpenElla {
    /// Create a new datastore if one doesn't already exist.
    pub fn or_create(mut self, config: impl Into<Config>) -> Self {
        self.create = Some(config.into());
        self
    }

    /// Create a new datastore with default options if one doesn't already exist.
    ///
    /// Shorthand for `or_create(ella::Config::default())`.
    pub fn or_create_default(mut self) -> Self {
        self.create = Some(Config::default());
        self
    }

    /// Serve the ella API on `addr`.
    ///
    /// This allows clients to access ella using [`ella::connect`].
    pub fn and_serve<A: ToSocketAddrs>(mut self, addr: A) -> crate::Result<Self> {
        self.serve = Some(addr.to_socket_addrs()?.collect());
        Ok(self)
    }
}

impl IntoFuture for OpenElla {
    type Output = crate::Result<Ella>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let ctx = if let Some(config) = self.create {
                crate::engine::create(&self.root, config, true).await?
            } else {
                crate::engine::open(&self.root).await?
            };
            let server = match self.serve {
                Some(addrs) => Some(EllaServer::start(
                    Server::builder(),
                    ctx.state().clone(),
                    &addrs[..],
                )?),
                None => None,
            };
            let server = Arc::new(Mutex::new(server));
            Ok(Ella::new(EllaInner::Local { ctx, server }))
        }
        .boxed()
    }
}

#[must_use]
#[derive(Debug)]
pub struct CreateElla {
    root: String,
    serve: Option<Vec<SocketAddr>>,
    config: Config,
    if_not_exists: bool,
}

impl CreateElla {
    /// Use the existing datastore at the given path if one exists.
    ///
    /// This is equivalent to `ella::open().or_create()`.
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }
}

impl IntoFuture for CreateElla {
    type Output = crate::Result<Ella>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let ctx = crate::engine::create(&self.root, self.config, self.if_not_exists).await?;
            let server = match self.serve {
                Some(addrs) => Some(EllaServer::start(
                    Server::builder(),
                    ctx.state().clone(),
                    &addrs[..],
                )?),
                None => None,
            };
            let server = Arc::new(Mutex::new(server));
            Ok(Ella::new(EllaInner::Local { ctx, server }))
        }
        .boxed()
    }
}
