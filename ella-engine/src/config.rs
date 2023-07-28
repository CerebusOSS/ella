use std::net::{SocketAddr, ToSocketAddrs};

use ella_common::Duration;

use crate::{registry::Id, TableConfig};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct EllaConfig {
    engine_config: EngineConfig,
    table_config: TableConfig,
    default_catalog: Id<'static>,
    default_schema: Id<'static>,
}

impl Default for EllaConfig {
    fn default() -> Self {
        Self {
            engine_config: Default::default(),
            table_config: Default::default(),
            default_catalog: "ella".into(),
            default_schema: "public".into(),
        }
    }
}

impl EllaConfig {
    pub fn builder() -> EllaConfigBuilder {
        EllaConfigBuilder::default()
    }

    pub fn table_config(&self) -> &TableConfig {
        &self.table_config
    }

    pub fn engine_config(&self) -> &EngineConfig {
        &self.engine_config
    }

    pub fn default_catalog(&self) -> &Id<'static> {
        &self.default_catalog
    }

    pub fn default_schema(&self) -> &Id<'static> {
        &self.default_schema
    }

    pub fn into_builder(self) -> EllaConfigBuilder {
        EllaConfigBuilder(self)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, derive_more::Into)]
pub struct EllaConfigBuilder(EllaConfig);

impl EllaConfigBuilder {
    pub fn table_config(mut self, config: impl Into<TableConfig>) -> Self {
        self.0.table_config = config.into();
        self
    }

    pub fn engine_config(mut self, config: impl Into<EngineConfig>) -> Self {
        self.0.engine_config = config.into();
        self
    }

    pub fn default_catalog(mut self, catalog: impl Into<Id<'static>>) -> Self {
        self.0.default_catalog = catalog.into();
        self
    }

    pub fn default_schema(mut self, catalog: impl Into<Id<'static>>) -> Self {
        self.0.default_schema = catalog.into();
        self
    }

    pub fn build(self) -> EllaConfig {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    serve_metrics: Option<SocketAddr>,
    maintenance_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            serve_metrics: None,
            maintenance_interval: Duration::seconds(30),
        }
    }
}

impl EngineConfig {
    pub fn builder() -> EngineConfigBuilder {
        EngineConfigBuilder::default()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn serve_metrics(&self) -> Option<&SocketAddr> {
        self.serve_metrics.as_ref()
    }

    pub fn maintenance_interval(&self) -> Duration {
        self.maintenance_interval
    }

    pub fn into_builder(self) -> EngineConfigBuilder {
        EngineConfigBuilder(self)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, derive_more::Into)]
pub struct EngineConfigBuilder(EngineConfig);

impl EngineConfigBuilder {
    pub fn serve_metrics<A: ToSocketAddrs>(mut self, address: A) -> Self {
        self.0.serve_metrics = Some(address.to_socket_addrs().unwrap().next().unwrap());
        self
    }

    pub fn maintenance_interval(mut self, interval: Duration) -> Self {
        self.0.maintenance_interval = interval;
        self
    }

    pub fn build(self) -> EngineConfig {
        self.0
    }
}
