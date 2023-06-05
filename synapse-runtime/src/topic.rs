mod config;
mod rw;
mod shard;
mod streaming;

pub use config::TopicConfig;
pub use rw::RwBuffer;
pub use shard::ShardManager;
pub use streaming::{RwPub, RwSub, StreamingTable};

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::ToDFSchema,
    datasource::TableProvider,
    error::Result,
    execution::context::SessionState,
    logical_expr::TableType,
    physical_plan::{union::UnionExec, ExecutionPlan},
    prelude::Expr,
};

use crate::{
    catalog::{snapshot::TopicState, transactions::CreateTopic, TopicId},
    Path, Schema, SynapseContext,
};

#[derive(Debug)]
pub struct Topic {
    ctx: Arc<SynapseContext>,
    id: TopicId,
    config: TopicConfig,
    schema: Arc<Schema>,
    path: Path,
    streaming: Arc<StreamingTable>,
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
        let streaming = Arc::new(StreamingTable::new(
            id.clone(),
            rw.clone(),
            config.streaming_config(),
        ));

        Self {
            id,
            schema,
            streaming,
            rw,
            shards,
            ctx,
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

    pub fn publish(&self) -> RwPub {
        self.streaming.publish()
    }

    pub fn id(&self) -> &TopicId {
        &self.id
    }

    pub async fn close(&self) -> crate::Result<()> {
        self.streaming.close().await;
        self.rw.close().await;
        self.shards.close().await
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
            self.streaming.scan(state, projection, filters, limit),
            self.rw.scan(state, projection, filters, limit),
            self.shards.scan(state, projection, filters, limit),
        )?;
        Ok(Arc::new(UnionExec::try_new_with_schema(
            vec![shards, rw, streaming],
            self.schema.arrow_schema().clone().to_dfschema_ref()?,
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
