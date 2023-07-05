mod channel;
mod config;
mod rw;
mod shard;

pub use channel::{Publisher, Subscriber, TopicChannel};
pub use config::TopicConfig;
use futures::{stream::BoxStream, Stream, StreamExt};
pub use rw::RwBuffer;
pub(crate) use shard::compact_shards;
pub use shard::ShardManager;

use std::{sync::Arc, task::Poll};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::{provider_as_source, TableProvider},
    error::Result,
    execution::context::SessionState,
    logical_expr::{LogicalPlanBuilder, TableProviderFilterPushDown, TableType, UNNAMED_TABLE},
    physical_plan::{
        insert::InsertExec, ExecutionPlan, Partitioning, RecordBatchStream, Statistics,
    },
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
            ctx,
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = filters.iter().cloned().reduce(|acc, f| acc.and(f));
        let child_plan = |provider: Arc<dyn TableProvider>| async {
            let mut plan = LogicalPlanBuilder::scan(
                UNNAMED_TABLE,
                provider_as_source(provider),
                projection.cloned(),
            )?;
            if let Some(filter) = &filter {
                plan = plan.filter(filter.clone())?;
            }
            if let Some(limit) = limit {
                plan = plan.limit(0, Some(limit))?;
            }
            state.create_physical_plan(&plan.build()?).await
        };
        let shards = child_plan(self.shards.clone()).await?;
        let rw = child_plan(self.rw.clone()).await?;
        let channel = child_plan(self.channel.clone()).await?;

        Ok(Arc::new(TopicExec {
            topic: self.id.clone(),
            shards,
            rw,
            channel,
        }))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(self.publish());
        Ok(Arc::new(InsertExec::new(input, sink)))
    }
}

#[derive(Debug, Clone)]
struct TopicExec {
    topic: TopicId,
    shards: Arc<dyn ExecutionPlan>,
    rw: Arc<dyn ExecutionPlan>,
    channel: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for TopicExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.channel.schema()
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children.iter().any(|c| *c))
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        // Sanity check that all child plans have the same output ordering
        debug_assert_eq!(self.channel.output_ordering(), self.rw.output_ordering());
        debug_assert_eq!(self.rw.output_ordering(), self.shards.output_ordering());
        self.channel.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; 3]
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.shards.clone(), self.rw.clone(), self.channel.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 3 {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "TopicExec expects 3 children, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            topic: self.topic.clone(),
            shards: children[0].clone(),
            rw: children[1].clone(),
            channel: children[2].clone(),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let shards = (0..self.shards.output_partitioning().partition_count())
            .map(|i| self.shards.execute(i, context.clone()))
            .collect::<Result<Vec<_>>>()?;
        let shards = futures::stream::iter(shards).flatten().boxed();

        let rw = (0..self.rw.output_partitioning().partition_count())
            .map(|i| self.rw.execute(i, context.clone()))
            .collect::<Result<Vec<_>>>()?;
        let rw = futures::stream::iter(rw).flatten().boxed();

        let channel = (0..self.channel.output_partitioning().partition_count())
            .map(|i| self.channel.execute(i, context.clone()))
            .collect::<Result<Vec<_>>>()?;
        let channel = futures::stream::iter(channel).flatten().boxed();

        Ok(Box::pin(TopicStream {
            schema: self.schema(),
            shards: Some(shards),
            rw: Some(rw),
            channel: Some(channel),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "TopicExec: topic={}", self.topic)
    }
}

struct TopicStream {
    schema: SchemaRef,
    shards: Option<BoxStream<'static, Result<RecordBatch>>>,
    rw: Option<BoxStream<'static, Result<RecordBatch>>>,
    channel: Option<BoxStream<'static, Result<RecordBatch>>>,
}

impl Stream for TopicStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some(shards) = &mut self.shards {
            match futures::ready!(shards.poll_next_unpin(cx)) {
                Some(item) => return Poll::Ready(Some(item)),
                None => {
                    self.shards = None;
                }
            }
        }
        if let Some(rw) = &mut self.rw {
            match futures::ready!(rw.poll_next_unpin(cx)) {
                Some(item) => return Poll::Ready(Some(item)),
                None => {
                    self.rw = None;
                }
            }
        }
        if let Some(channel) = &mut self.channel {
            match futures::ready!(channel.poll_next_unpin(cx)) {
                Some(item) => return Poll::Ready(Some(item)),
                None => {
                    self.channel = None;
                }
            }
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for TopicStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
