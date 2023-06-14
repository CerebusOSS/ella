mod channel;
mod config;
mod rw;
mod shard;

pub use channel::{Publisher, Subscriber, TopicChannel};
pub use config::TopicConfig;
pub use rw::RwBuffer;
pub(crate) use shard::compact_shards;
pub use shard::ShardManager;

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::ToDFSchema,
    datasource::TableProvider,
    error::Result,
    execution::context::SessionState,
    logical_expr::TableType,
    physical_plan::{project_schema, union::UnionExec, ExecutionPlan},
    prelude::Expr,
};

use crate::{
    catalog::{snapshot::TopicState, transactions::CreateTopic, TopicId},
    Path, Schema, SynapseContext,
};

use self::shard::ShardSet;

#[derive(Debug)]
pub struct Topic {
    id: TopicId,
    config: TopicConfig,
    schema: Arc<Schema>,
    path: Path,
    channel: Arc<TopicChannel>,
    rw: Arc<RwBuffer>,
    shards: Arc<ShardManager>,
}

impl Topic {
    pub(crate) fn new(ctx: Arc<SynapseContext>, state: &TopicState) -> Self {
        let id = state.id.clone();
        let path = state.path.clone();
        let schema = Arc::new(state.schema.clone());
        let config = ctx.config().topic_config(id.clone()).clone();
        let shards = Arc::new(ShardManager::new(
            ctx.clone(),
            schema.clone(),
            config.shard_config(),
            state,
        ));
        let rw = Arc::new(RwBuffer::new(
            id.clone(),
            shards.clone(),
            config.rw_buffer_config(),
        ));
        let channel = Arc::new(TopicChannel::new(
            id.clone(),
            schema.clone(),
            rw.clone(),
            config.channel_config(),
        ));

        Self {
            id,
            schema,
            channel,
            rw,
            shards,
            path,
            config,
        }
    }

    pub(crate) async fn create(
        id: TopicId,
        schema: Schema,
        ctx: Arc<SynapseContext>,
    ) -> crate::Result<Self> {
        let tsn = CreateTopic::new(id.clone(), schema.clone(), ctx.root());
        let state = TopicState::from(tsn.clone());
        ctx.log().commit(tsn).await?;
        Ok(Self::new(ctx, &state))
    }

    pub fn publish(&self) -> Publisher {
        self.channel.publish()
    }

    pub fn id(&self) -> &TopicId {
        &self.id
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn config(&self) -> &TopicConfig {
        &self.config
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn close(&self) -> crate::Result<()> {
        self.rw.close().await;
        self.shards.close().await
    }

    pub(crate) fn shards(&self) -> &Arc<ShardSet> {
        self.shards.shards()
    }
}

#[async_trait::async_trait]
impl TableProvider for Topic {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (streaming, rw, shards) = futures::try_join!(
            self.channel.scan(state, projection, filters, limit),
            self.rw.scan(state, projection, filters, limit),
            self.shards.scan(state, projection, filters, limit),
        )?;
        let schema = project_schema(self.schema.arrow_schema(), projection)?;
        Ok(Arc::new(UnionExec::try_new_with_schema(
            vec![shards, rw, streaming],
            schema.to_dfschema_ref()?,
        )?))
    }

    // async fn insert_into(&self, state: &SessionState, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    //     // let plan = state.create_physical_plan(input).await?;

    //     // let plan = CoalescePartitionsExec::new(plan);

    //     let mut stream = input.execute(0, state.task_ctx())?;
    //     while let Some(batch) = stream.try_next().await? {
    //         self.publish.insert(batch)?;
    //     }
    //     Ok(())
    // }
}
