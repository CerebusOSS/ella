use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use datafusion::prelude::{SessionConfig, SessionContext};
use synapse_common::Duration;

use crate::{
    catalog::{Catalog, TopicId},
    lazy::{Lazy, LocalBackend},
    metrics::MetricsServer,
    topic::{Topic, TopicConfig},
    util::Maintainer,
    Schema, SynapseContext,
};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    default_topic_config: TopicConfig,
    topic_config_overrides: HashMap<TopicId, TopicConfig>,
    serve_metrics: Option<SocketAddr>,
    maintenance_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            default_topic_config: TopicConfig::default(),
            topic_config_overrides: HashMap::new(),
            serve_metrics: None,
            maintenance_interval: Duration::seconds(30),
        }
    }
}

impl EngineConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_serve_metrics<A: ToSocketAddrs>(mut self, address: A) -> Self {
        self.serve_metrics = Some(address.to_socket_addrs().unwrap().next().unwrap());
        self
    }

    pub fn with_maintenance_interval(mut self, interval: Duration) -> Self {
        self.maintenance_interval = interval;
        self
    }

    pub fn with_default_topic_config(mut self, config: TopicConfig) -> Self {
        self.default_topic_config = config;
        self
    }

    pub fn with_topic_config<T>(mut self, topic: T, config: TopicConfig) -> Self
    where
        T: Into<TopicId>,
    {
        self.topic_config_overrides.insert(topic.into(), config);
        self
    }

    pub fn topic_config<T>(&self, topic: T) -> &TopicConfig
    where
        T: Into<TopicId>,
    {
        self.topic_config_overrides
            .get(&topic.into())
            .unwrap_or(&self.default_topic_config)
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    ctx: Arc<SynapseContext>,
    catalog: Arc<Catalog>,
    maintainer: Arc<Maintainer>,
    #[cfg(feature = "metrics")]
    metrics: Arc<Option<crate::metrics::MetricsServer>>,
}

impl Engine {
    pub async fn start(root: impl AsRef<str>) -> crate::Result<Self> {
        Self::start_with_config(root, EngineConfig::default()).await
    }

    pub async fn start_with_config(
        root: impl AsRef<str>,
        config: EngineConfig,
    ) -> crate::Result<Self> {
        let root: crate::Path = root.as_ref().parse()?;
        let df_cfg = SessionConfig::new()
            .with_create_default_catalog_and_schema(false)
            .with_default_catalog_and_schema(Catalog::CATALOG_ID, Catalog::SCHEMA_ID)
            // TODO: support repartitioning
            .with_round_robin_repartition(false)
            // TODO: support batches
            .with_coalesce_batches(false);

        let session = SessionContext::with_config(df_cfg);
        let env = session.runtime_env();
        let ctx = Arc::new(SynapseContext::new(root, session, config.clone(), &env)?);
        let catalog = Catalog::open(ctx.clone()).await?;
        ctx.session()
            .register_catalog(Catalog::CATALOG_ID, catalog.clone().catalog_provider());

        let maintainer = Arc::new(Maintainer::new(
            catalog.clone(),
            ctx.clone(),
            config.maintenance_interval,
        ));

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(
            config
                .serve_metrics
                .as_ref()
                .map(|addr| MetricsServer::start(*addr)),
        );
        Ok(Self {
            ctx,
            catalog,
            maintainer,
            #[cfg(feature = "metrics")]
            metrics,
        })
    }

    pub fn ctx(&self) -> &Arc<SynapseContext> {
        &self.ctx
    }

    pub fn topic<T>(&self, topic: T) -> TopicRef<'_>
    where
        T: Into<TopicId>,
    {
        TopicRef {
            topic: topic.into(),
            engine: self,
        }
    }

    pub fn topics(&self) -> Vec<Arc<Topic>> {
        self.catalog.topics()
    }

    pub async fn publish(&self, topic: &str, schema: Schema) -> crate::Result<Arc<Topic>> {
        if let Some(topic) = self.catalog.topic(topic) {
            Ok(topic)
        } else {
            self.catalog.create_topic(topic, schema).await
        }
    }

    pub async fn query(&self, sql: &str) -> crate::Result<Lazy> {
        let state = self.ctx.session().state();
        let plan = state.create_logical_plan(sql).await?;
        Ok(Lazy::new(plan, Arc::new(LocalBackend(state))))
    }

    pub async fn shutdown(&self) -> crate::Result<()> {
        let results = futures::future::join_all(
            self.catalog
                .topics()
                .into_iter()
                .map(|t| async move { (t.id().clone(), t.close().await) }),
        )
        .await;

        let mut out = Ok(());
        for (topic, res) in results {
            if let Err(error) = res {
                tracing::error!(topic=%topic, ?error, "error while closing topic");
                out = Err(error);
            }
        }
        self.maintainer.stop().await;
        self.ctx.log().create_snapshot().await?;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.stop().await;
        }

        out
    }

    #[doc(hidden)]
    pub fn catalog(&self) -> Arc<Catalog> {
        self.catalog.clone()
    }
}

pub struct TopicRef<'a> {
    topic: TopicId,
    engine: &'a Engine,
}

impl<'a> TopicRef<'a> {
    pub async fn get_or_create(&self, schema: Schema) -> crate::Result<Arc<Topic>> {
        if let Some(topic) = self.get() {
            Ok(topic)
        } else {
            self.engine
                .catalog
                .create_topic(self.topic.clone(), schema)
                .await
        }
    }

    pub fn get(&self) -> Option<Arc<Topic>> {
        self.engine.catalog.topic(self.topic.clone())
    }

    pub async fn create(&self, schema: Schema) -> crate::Result<Arc<Topic>> {
        self.engine
            .catalog
            .create_topic(self.topic.clone(), schema)
            .await
    }
}
