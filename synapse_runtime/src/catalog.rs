mod id;
pub(crate) mod snapshot;
mod transaction_log;
pub mod transactions;

pub use id::*;
pub use transaction_log::TransactionLog;

use dashmap::DashMap;

use crate::{topic::Topic, Schema, SynapseContext};
use std::sync::Arc;

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
};

use self::snapshot::Snapshot;

#[derive(Debug)]
pub struct Catalog {
    ctx: Arc<SynapseContext>,
    topics: DashMap<TopicId, Arc<Topic>>,
}

impl Catalog {
    pub const CATALOG_ID: &'static str = "synapse";
    pub const SCHEMA_ID: &'static str = "topic";

    pub async fn open(ctx: Arc<SynapseContext>) -> crate::Result<Arc<Self>> {
        let state = ctx.log().load_snapshot().await?;
        let topics = Self::recreate_topics(&state, ctx.clone());
        Ok(Arc::new(Self { ctx, topics }))
    }

    pub async fn create_topic<S>(
        self: &Arc<Self>,
        id: S,
        schema: Schema,
    ) -> crate::Result<Arc<Topic>>
    where
        S: Into<TopicId>,
    {
        let id: TopicId = id.into();
        let topic = Arc::new(Topic::create(id.clone(), schema, self.ctx.clone()).await?);
        self.topics.insert(id, topic.clone());
        Ok(topic)
    }

    pub fn topic<S>(&self, id: S) -> Option<Arc<Topic>>
    where
        S: Into<TopicId>,
    {
        let id: TopicId = id.into();
        self.topics.get(&id).map(|t| t.value().clone())
    }

    pub fn catalog_provider(self: Arc<Self>) -> Arc<SynapseCatalogProvider> {
        Arc::new(SynapseCatalogProvider(self))
    }

    pub fn topics(&self) -> Vec<Arc<Topic>> {
        self.topics
            .iter()
            .map(|t| t.value().clone())
            .collect::<Vec<_>>()
    }

    fn recreate_topics(state: &Snapshot, ctx: Arc<SynapseContext>) -> DashMap<TopicId, Arc<Topic>> {
        // let mut state = self.state.write().await;
        let topics = DashMap::with_capacity(state.topics.len());
        for topic in &state.topics {
            topics.insert(topic.id.clone(), Arc::new(Topic::new(ctx.clone(), topic)));
        }
        topics
    }
}

#[async_trait::async_trait]
impl SchemaProvider for Catalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.topics
            .iter()
            .map(|t| t.key().clone().to_string())
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.topics.contains_key(&name.into())
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.topic(name).map(|t| t as Arc<_>)
    }
}

pub struct SynapseCatalogProvider(Arc<Catalog>);

impl CatalogProvider for SynapseCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![Catalog::SCHEMA_ID.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::schema::SchemaProvider>> {
        match name {
            Catalog::SCHEMA_ID => Some(self.0.clone()),
            _ => None,
        }
    }
}
