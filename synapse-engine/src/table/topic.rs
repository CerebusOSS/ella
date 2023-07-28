mod channel;
mod rw;
pub(crate) mod shard;

pub use channel::{Publisher, Subscriber, TopicChannel};
use futures::{stream::BoxStream, Stream, StreamExt};
pub(crate) use rw::RwBuffer;
pub(crate) use shard::compact_shards;
pub use shard::ShardInfo;
pub(crate) use shard::ShardManager;

use std::{sync::Arc, task::Poll};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::{provider_as_source, TableProvider},
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{LogicalPlanBuilder, TableProviderFilterPushDown, TableType, UNNAMED_TABLE},
    parquet::format::SortingColumn,
    physical_plan::{
        insert::InsertExec, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        RecordBatchStream, Statistics,
    },
    prelude::Expr,
};

use crate::{engine::SynapseState, registry::TableId, table::TableConfig, Path};

use self::shard::ShardSet;

use super::info::{SynapseTableInfo, TopicInfo};

#[derive(Debug)]
pub struct SynapseTopic {
    info: TopicInfo,
    table_info: SynapseTableInfo,
    config: TableConfig,
    channel: Arc<TopicChannel>,
    rw: Option<Arc<RwBuffer>>,
    shards: Option<Arc<ShardManager>>,
}

impl SynapseTopic {
    pub(crate) fn new(
        id: TableId<'static>,
        info: TopicInfo,
        state: &SynapseState,
    ) -> crate::Result<Self> {
        let table_info = info.table_info(id, state)?;
        let config = table_info.config().clone();

        let (shards, rw) = if !info.temporary() {
            let shards = Arc::new(ShardManager::new(
                state.log().clone(),
                state.store().clone(),
                table_info.clone(),
                config.shard_config(),
            ));
            let rw = Arc::new(RwBuffer::new(
                table_info.clone(),
                shards.clone(),
                config.rw_buffer_config(),
            ));
            (Some(shards), Some(rw))
        } else {
            (None, None)
        };

        let channel = Arc::new(TopicChannel::new(
            table_info.clone(),
            rw.clone(),
            config.channel_config(),
        ));

        Ok(Self {
            info,
            table_info,
            channel,
            rw,
            shards,
            config,
        })
    }

    pub fn publish(&self) -> Publisher {
        self.channel.publish()
    }

    pub fn table(&self) -> &TableId<'static> {
        self.table_info.id()
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    pub fn path(&self) -> &Path {
        self.table_info.path()
    }

    pub fn temporary(&self) -> bool {
        self.info.temporary()
    }

    pub async fn close(&self) -> crate::Result<()> {
        if let Some(rw) = &self.rw {
            rw.close().await;
        }
        if let Some(shards) = &self.shards {
            shards.close().await?;
        }
        Ok(())
    }

    pub(crate) async fn drop_shards(&self) -> crate::Result<()> {
        self.close().await?;
        if let Some(shards) = &self.shards {
            shards.delete_all().await?;
        }
        Ok(())
    }

    pub fn info(&self) -> &TopicInfo {
        &self.info
    }

    pub(crate) fn shards(&self) -> Option<&Arc<ShardSet>> {
        self.shards.as_ref().map(|s| s.shards())
    }

    pub(crate) fn file_schema(&self) -> SchemaRef {
        self.table_info
            .parquet_schema()
            .cloned()
            .unwrap_or_else(|| self.table_info.arrow_schema().clone())
    }

    pub(crate) fn sort(&self) -> Option<Vec<SortingColumn>> {
        self.table_info.sorting_cols().cloned()
    }
}

#[async_trait::async_trait]
impl TableProvider for SynapseTopic {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_info.arrow_schema().clone()
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
        let shards = match self.shards.clone() {
            Some(shards) => Some(child_plan(shards).await?),
            None => None,
        };
        let rw = match self.rw.clone() {
            Some(rw) => Some(child_plan(rw).await?),
            None => None,
        };
        let channel = child_plan(self.channel.clone()).await?;

        Ok(Arc::new(TopicExec {
            table: self.table().clone(),
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
    table: TableId<'static>,
    shards: Option<Arc<dyn ExecutionPlan>>,
    rw: Option<Arc<dyn ExecutionPlan>>,
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
        self.channel.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.children().into_iter().map(|_| true).collect()
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        let mut children = Vec::with_capacity(3);
        if let Some(shards) = self.shards.clone() {
            children.push(shards);
        }
        if let Some(rw) = self.rw.clone() {
            children.push(rw);
        }
        children.push(self.channel.clone());
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let num_children = self.children().len();
        if children.len() != num_children {
            return Err(DataFusionError::Execution(format!(
                "TopicExec expects exactly {} children, but got {}",
                num_children,
                children.len()
            )));
        }
        let mut iter = children.into_iter();

        let shards = self.shards.is_some().then(|| iter.next().unwrap());
        let rw = self.rw.is_some().then(|| iter.next().unwrap());
        let channel = iter.next().unwrap();
        Ok(Arc::new(Self {
            table: self.table.clone(),
            shards,
            rw,
            channel,
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let exec_child =
            |child: &Arc<dyn ExecutionPlan>| -> Result<BoxStream<'static, Result<RecordBatch>>> {
                let streams = (0..child.output_partitioning().partition_count())
                    .map(|i| child.execute(i, context.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(futures::stream::iter(streams).flatten().boxed())
            };

        let shards = match self.shards.as_ref() {
            Some(shards) => Some(exec_child(shards)?),
            None => None,
        };
        let rw = match self.rw.as_ref() {
            Some(rw) => Some(exec_child(rw)?),
            None => None,
        };

        let channel = (0..self.channel.output_partitioning().partition_count())
            .map(|i| self.channel.execute(i, context.clone()))
            .collect::<Result<Vec<_>>>()?;
        let channel = futures::stream::iter(channel).flatten().boxed();

        Ok(Box::pin(TopicStream {
            schema: self.schema(),
            shards,
            rw,
            channel: Some(channel),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TopicExec: table={}", self.table)
    }
}

impl DisplayAs for TopicExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TopicExec: table={}", self.table)
            }
        }
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
