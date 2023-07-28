use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};

use arrow_schema::SchemaRef;
use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::{context::SessionState, TaskContext},
    logical_expr::TableType,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        insert::DataSink, project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
    prelude::Expr,
};
use ella_common::row::{RowFormat, RowSink};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use tokio::sync::{broadcast, Notify};
use tokio_util::sync::ReusableBoxFuture;

use crate::{
    registry::TableId,
    table::{config::ChannelConfig, info::EllaTableInfo},
    ArrowSchema,
};

use super::{rw::RwBufferSink, RwBuffer};

#[derive(Debug)]
pub struct TopicChannel {
    table: EllaTableInfo,
    config: ChannelConfig,
    publisher: Publisher,
}

impl TopicChannel {
    pub(crate) fn new(
        table: EllaTableInfo,
        rw: Option<Arc<RwBuffer>>,
        config: ChannelConfig,
    ) -> Self {
        let (sub_sender, _) = broadcast::channel(config.subscriber_queue_size);
        let subs = Arc::new(sub_sender);
        let stop = Arc::new(Notify::new());
        let active = Arc::new(AtomicUsize::new(0));

        let publisher = Publisher {
            table: table.id().clone(),
            schema: table.arrow_schema().clone(),
            inner: PublisherInner {
                rw: RwBuffer::sink(rw),
                subs,
                stop,
                active,
                is_active: false,
            },
        };
        Self {
            table,
            config,
            publisher,
        }
    }

    pub fn publish(&self) -> Publisher {
        self.publisher.clone()
    }

    pub fn subscribe(&self, stop_on_inactive: bool) -> Subscriber {
        Subscriber::new(self.subscribe_inner(stop_on_inactive))
    }

    pub fn config(&self) -> &ChannelConfig {
        &self.config
    }

    fn subscribe_inner(&self, stop_on_inactive: bool) -> SubscriberInner {
        SubscriberInner {
            inner: self.publisher.inner.subs.subscribe(),
            stop: self.publisher.inner.stop.clone(),
            active: self.publisher.inner.active.clone(),
            stop_on_inactive,
        }
    }
}

#[derive(Debug)]
struct PublisherInner {
    rw: RwBufferSink,
    subs: Arc<broadcast::Sender<RecordBatch>>,
    stop: Arc<Notify>,
    active: Arc<AtomicUsize>,
    is_active: bool,
}

impl Clone for PublisherInner {
    fn clone(&self) -> Self {
        self.clone_inner(true)
    }
}

impl Drop for PublisherInner {
    fn drop(&mut self) {
        if self.is_active {
            let active = self.active.fetch_sub(1, Ordering::Release) - 1;
            if active == 0 {
                self.stop.notify_one();
            }
        }
    }
}

impl PublisherInner {
    fn clone_inner(&self, is_active: bool) -> Self {
        if is_active {
            self.active.fetch_add(1, Ordering::Release);
        }

        Self {
            rw: self.rw.clone(),
            subs: self.subs.clone(),
            stop: self.stop.clone(),
            active: self.active.clone(),
            is_active,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Publisher {
    table: TableId<'static>,
    schema: SchemaRef,
    inner: PublisherInner,
}

impl Sink<RecordBatch> for Publisher {
    type Error = crate::Error;

    #[inline]
    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.rw.poll_ready_unpin(cx)
    }

    #[inline]
    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: RecordBatch,
    ) -> std::result::Result<(), Self::Error> {
        let batch = item.with_schema(self.schema.clone())?;
        let _ = self.inner.subs.send(batch.clone());
        self.inner.rw.start_send_unpin(batch)
    }

    #[inline]
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.rw.poll_flush_unpin(cx)
    }

    #[inline]
    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.rw.poll_close_unpin(cx)
    }
}

impl Publisher {
    pub fn rows<R: RowFormat>(self, buffer: usize) -> crate::Result<RowSink<R>> {
        let schema = self.schema.clone();
        RowSink::try_new(self, schema, buffer)
    }

    pub(crate) fn clone_weak(&self) -> Self {
        self.clone_inner(false)
    }

    fn clone_inner(&self, is_active: bool) -> Self {
        Self {
            table: self.table.clone(),
            schema: self.schema.clone(),
            inner: self.inner.clone_inner(is_active),
        }
    }
}

impl Display for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "publisher ({})", self.table)
    }
}

impl DisplayAs for Publisher {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "Publisher: {}", self.table),
            DisplayFormatType::Verbose => write!(
                f,
                "Publisher: table={}, schema={:?}",
                self.table, self.schema,
            ),
        }
    }
}

#[async_trait::async_trait]
impl DataSink for Publisher {
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _ctx: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut this = self.clone_weak();
        let mut rows = 0;

        while let Some(batch) = data.try_next().await? {
            rows += batch.num_rows();
            this.feed(batch)
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
        }
        this.flush()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(rows as u64)
    }
}

#[derive(Debug)]
pub struct Subscriber {
    inner: ReusableBoxFuture<'static, (Option<Result<RecordBatch>>, SubscriberInner)>,
}

impl Subscriber {
    fn new(inner: SubscriberInner) -> Self {
        let inner = ReusableBoxFuture::new(inner.next());
        Self { inner }
    }
}

impl Stream for Subscriber {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let (item, inner) = futures::ready!(self.inner.poll(cx));
        self.inner.set(inner.next());
        Poll::Ready(item)
    }
}

#[derive(Debug)]
struct SubscriberInner {
    inner: broadcast::Receiver<RecordBatch>,
    stop: Arc<Notify>,
    active: Arc<AtomicUsize>,
    stop_on_inactive: bool,
}

impl SubscriberInner {
    async fn next(mut self) -> (Option<Result<RecordBatch>>, Self) {
        loop {
            if self.stop_on_inactive && self.active.load(Ordering::Acquire) == 0 {
                let item = match self.inner.recv().now_or_never() {
                    Some(Ok(batch)) => Some(Ok(batch)),
                    Some(Err(broadcast::error::RecvError::Closed)) => None,
                    Some(Err(broadcast::error::RecvError::Lagged(lag))) => {
                        Some(Err(DataFusionError::Execution(format!(
                            "subscriber lagged data stream by {} item",
                            lag
                        ))))
                    }
                    None => None,
                };
                return (item, self);
            }

            tokio::select! {
                biased;
                res = self.inner.recv() => {
                    let item = match res {
                        Ok(batch) => Some(Ok(batch)),
                        Err(broadcast::error::RecvError::Closed) => None,
                        Err(broadcast::error::RecvError::Lagged(lag)) => {
                            Some(Err(DataFusionError::Execution(format!("subscriber lagged data stream by {} item", lag))))
                        }
                    };
                    return (item, self)
                },
                _ = self.stop.notified(), if self.stop_on_inactive => {
                    if self.active.load(Ordering::Acquire) == 0 {
                        return (None, self)
                    }
                },
            };
        }
    }
}

impl Clone for SubscriberInner {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.resubscribe(),
            stop: self.stop.clone(),
            active: self.active.clone(),
            stop_on_inactive: self.stop_on_inactive,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for TopicChannel {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.table.arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ChannelExec::try_new(
            self.subscribe_inner(true),
            self.table.arrow_schema(),
            projection.cloned(),
            self.table.output_ordering(),
            limit,
        )?))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct ChannelExec {
    src: SubscriberInner,
    projected_schema: Arc<ArrowSchema>,
    projection: Option<Vec<usize>>,
    order: Option<Vec<PhysicalSortExpr>>,
    limit: Option<usize>,
}

impl ChannelExec {
    fn try_new(
        src: SubscriberInner,
        schema: &SchemaRef,
        projection: Option<Vec<usize>>,
        mut order: Option<Vec<PhysicalSortExpr>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let projected_schema = project_schema(schema, projection.as_ref())?;
        if let Some((sort, project)) = order.as_deref().zip(projection.as_deref()) {
            order = Some(crate::util::project_ordering(schema, project, sort)?);
        }
        Ok(Self {
            src,
            projected_schema,
            projection,
            order,
            limit,
        })
    }
}

impl ExecutionPlan for ChannelExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.order.as_deref()
    }

    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        Ok(!self.src.stop_on_inactive)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "with_new_children not implemented for ChannelExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let stream = ChannelStream {
            inner: Subscriber::new(self.src.clone()),
            limit: self.limit,
            rows: 0,
            schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
        };
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let publishers = self.src.active.load(Ordering::Relaxed);
        write!(f, "ChannelExec: publishers={}", publishers)?;
        if let Some(limit) = self.limit {
            write!(f, ", limit={}", limit)?;
        }
        Ok(())
    }
}

impl DisplayAs for ChannelExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let publishers = self.src.active.load(Ordering::Relaxed);
                write!(f, "ChannelExec: publishers={}", publishers)?;
                if let Some(limit) = self.limit {
                    write!(f, ", limit={}", limit)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ChannelStream {
    inner: Subscriber,
    limit: Option<usize>,
    rows: usize,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl Stream for ChannelStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.limit == Some(self.rows) {
            return Poll::Ready(None);
        }

        Poll::Ready(match futures::ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(mut batch)) => {
                self.rows += batch.num_rows();
                if let Some(projection) = &self.projection {
                    batch = batch.project(projection)?;
                }
                Some(Ok(batch))
            }
            res => res,
        })
    }
}

impl RecordBatchStream for ChannelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
