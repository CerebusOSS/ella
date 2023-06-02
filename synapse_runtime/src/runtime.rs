use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};

use crate::{
    catalog::{Catalog, TopicId},
    topic::{Topic, TopicConfig},
    Schema, SynapseContext,
};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    default_topic_config: TopicConfig,
    topic_config_overrides: HashMap<TopicId, TopicConfig>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            default_topic_config: TopicConfig::default(),
            topic_config_overrides: HashMap::new(),
        }
    }
}

impl RuntimeConfig {
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

pub struct Runtime {
    ctx: Arc<SynapseContext>,
    catalog: Arc<Catalog>,
}

impl Runtime {
    pub async fn start(root: impl AsRef<str>) -> crate::Result<Self> {
        Self::start_with_config(root, RuntimeConfig::default()).await
    }

    pub async fn start_with_config(
        root: impl AsRef<str>,
        config: RuntimeConfig,
    ) -> crate::Result<Self> {
        let root: crate::Path = root.as_ref().parse()?;
        let df_cfg = SessionConfig::new()
            .with_create_default_catalog_and_schema(false)
            .with_default_catalog_and_schema(Catalog::CATALOG_ID, Catalog::SCHEMA_ID);

        let session = SessionContext::with_config(df_cfg);
        let env = session.runtime_env();
        let ctx = Arc::new(SynapseContext::new(root, session, config, &env)?);
        let catalog = Catalog::open(ctx.clone()).await?;
        ctx.session()
            .register_catalog(Catalog::CATALOG_ID, catalog.clone().catalog_provider());
        Ok(Self { ctx, catalog })
    }

    pub fn topic<T>(&self, topic: T) -> TopicRef<'_>
    where
        T: Into<TopicId>,
    {
        TopicRef {
            topic: topic.into(),
            rt: self,
        }
    }

    pub async fn publish(&self, topic: &str, schema: Schema) -> crate::Result<Arc<Topic>> {
        if let Some(topic) = self.catalog.topic(topic) {
            Ok(topic)
        } else {
            self.catalog.create_topic(topic, schema).await
        }
    }

    pub async fn query(&self, sql: &str) -> crate::Result<DataFrame> {
        Ok(self.ctx.session().sql(sql).await?)
    }

    pub async fn shutdown(&self) -> crate::Result<()> {
        let results = futures::future::join_all(
            self.catalog
                .topics()
                .into_iter()
                .map(|t| async move { (t.id().clone(), t.close().await) }),
        )
        .await;

        self.ctx.log().create_snapshot().await?;
        let mut out = Ok(());
        for (topic, res) in results {
            if let Err(error) = res {
                tracing::error!(topic=%topic, ?error, "error while closing topic");
                out = Err(error);
            }
        }

        out
    }
}

pub struct TopicRef<'a> {
    topic: TopicId,
    rt: &'a Runtime,
}

impl<'a> TopicRef<'a> {
    pub async fn get_or_create(self, schema: Schema) -> crate::Result<Arc<Topic>> {
        if let Some(topic) = self.get() {
            Ok(topic)
        } else {
            self.rt.catalog.create_topic(self.topic, schema).await
        }
    }

    pub fn get(&self) -> Option<Arc<Topic>> {
        self.rt.catalog.topic(self.topic.clone())
    }
}
