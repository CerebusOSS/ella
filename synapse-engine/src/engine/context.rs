use std::{fmt::Debug, ops::DerefMut, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    cluster::SynapseCluster,
    config::SynapseConfig,
    engine::SynapseState,
    lazy::Lazy,
    registry::{Id, TableRef},
    table::{
        info::{TableInfo, TopicInfo, ViewInfo},
        SynapseTable, SynapseTopic, SynapseView,
    },
};

use super::Engine;

#[derive(Clone)]
pub struct SynapseContext {
    state: SynapseState,
    engine: Arc<Mutex<Option<Engine>>>,
}

impl Debug for SynapseContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SynapseContext")
            .field("state", &self.state)
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

impl SynapseContext {
    pub fn new(state: SynapseState) -> crate::Result<Self> {
        let engine = Arc::new(Mutex::new(Some(Engine::start(Arc::new(state.clone()))?)));
        Ok(Self { state, engine })
    }

    pub fn use_catalog<'a>(mut self, catalog: impl Into<Id<'a>>) -> Self {
        let catalog: Id<'static> = catalog.into().into_owned();
        let config = self
            .state
            .config()
            .clone()
            .into_builder()
            .default_catalog(catalog)
            .build();
        self.state.with_config(config);
        self
    }

    pub fn use_schema<'a>(mut self, schema: impl Into<Id<'a>>) -> Self {
        let schema: Id<'static> = schema.into().into_owned();
        let config = self
            .state
            .config()
            .clone()
            .into_builder()
            .default_schema(schema)
            .build();
        self.state.with_config(config);
        self
    }

    pub async fn query(&self, sql: impl AsRef<str>) -> crate::Result<Lazy> {
        self.state.query(sql).await
    }

    pub async fn execute(&self, sql: &str) -> crate::Result<()> {
        self.query(sql).await?.execute().await?;
        Ok(())
    }

    pub async fn create_topic<'a>(
        &self,
        table: impl Into<TableRef<'a>>,
        info: impl Into<TopicInfo>,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<Arc<SynapseTopic>> {
        self.state
            .create_topic(
                self.state.resolve(table.into()),
                info.into(),
                if_not_exists,
                or_replace,
            )
            .await
    }

    pub async fn create_view<'a>(
        &self,
        table: impl Into<TableRef<'a>>,
        info: impl Into<ViewInfo>,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<Arc<SynapseView>> {
        self.state
            .create_view(
                self.state.resolve(table.into()),
                info.into(),
                if_not_exists,
                or_replace,
            )
            .await
    }

    pub async fn create_table<'a>(
        &self,
        table: impl Into<TableRef<'a>>,
        info: impl Into<TableInfo>,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<Arc<SynapseTable>> {
        self.state
            .create_table(
                self.state.resolve(table.into()),
                info.into(),
                if_not_exists,
                or_replace,
            )
            .await
    }

    pub fn table<'a>(&self, table: impl Into<TableRef<'a>>) -> Option<Arc<SynapseTable>> {
        self.state.table(self.state.resolve(table.into()))
    }

    pub async fn shutdown(self) -> crate::Result<()> {
        if let Some(engine) = std::mem::take(self.engine.lock_owned().await.deref_mut()) {
            engine.shutdown().await?;
        }
        Ok(())
    }

    pub fn config(&self) -> &SynapseConfig {
        self.state.config()
    }

    pub fn cluster(&self) -> &Arc<SynapseCluster> {
        self.state.cluster()
    }

    pub fn default_catalog(&self) -> &Id<'static> {
        self.state.default_catalog()
    }

    pub fn default_schema(&self) -> &Id<'static> {
        self.state.default_schema()
    }

    pub fn state(&self) -> &SynapseState {
        &self.state
    }
}
