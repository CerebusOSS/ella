use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::TableType,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        project_schema, stream::RecordBatchStreamAdapter, ColumnStatistics, ExecutionPlan,
        Partitioning, Statistics,
    },
    prelude::Expr,
};
use futures::{FutureExt, Stream, StreamExt};
use tokio::{
    sync::{
        broadcast,
        mpsc::{self, error::TrySendError},
        Mutex, Notify,
    },
    task::JoinHandle,
};
use tokio_util::sync::ReusableBoxFuture;

use super::RwBuffer;
use crate::{catalog::TopicId, ArrowSchema, Schema};

#[derive(Debug, Clone)]
pub struct RwConfig {
    pub max_rows: usize,
    pub max_elapsed: Duration,
}

impl Default for RwConfig {
    fn default() -> Self {
        Self {
            max_rows: Default::default(),
            max_elapsed: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct RwSub {
    inner: ReusableBoxFuture<'static, (Option<Result<RecordBatch>>, RwSubInner)>,
}

impl RwSub {
    fn new(inner: RwSubInner) -> Self {
        let inner = ReusableBoxFuture::new(inner.next());
        Self { inner }
    }
}

impl Stream for RwSub {
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
struct RwSubInner {
    inner: broadcast::Receiver<RecordBatch>,
    stop: Arc<Notify>,
    active: Arc<AtomicUsize>,
    stop_on_inactive: bool,
}

impl RwSubInner {
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

impl Clone for RwSubInner {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.resubscribe(),
            stop: self.stop.clone(),
            active: self.active.clone(),
            stop_on_inactive: self.stop_on_inactive,
        }
    }
}

#[derive(Debug)]
pub struct RwPub {
    inner: mpsc::Sender<RecordBatch>,
    schema: Arc<Schema>,
    stop: Arc<Notify>,
    active: Arc<AtomicUsize>,
}

impl Clone for RwPub {
    fn clone(&self) -> Self {
        self.active.fetch_add(1, Ordering::Release);
        Self {
            inner: self.inner.clone(),
            schema: self.schema.clone(),
            stop: self.stop.clone(),
            active: self.active.clone(),
        }
    }
}

impl Drop for RwPub {
    fn drop(&mut self) {
        let active = self.active.fetch_sub(1, Ordering::Release) - 1;
        tracing::debug!(active, "publisher closed");
        self.stop.notify_one();
    }
}

impl RwPub {
    pub fn insert<R>(&self, batch: R) -> Result<()>
    where
        R: Into<RecordBatch>,
    {
        let batch: RecordBatch = batch.into();
        let batch = batch.with_schema(self.schema.arrow_schema().clone())?;

        match self.inner.try_send(batch) {
            Ok(_) => Ok(()),
            Err(TrySendError::Closed(_)) => {
                Err(DataFusionError::Execution("table unavailable".to_string()))
            }
            Err(TrySendError::Full(_)) => {
                Err(DataFusionError::Execution("table buffer full".to_string()))
            }
        }
    }
}

#[derive(Debug)]
pub struct StreamingTable {
    topic: TopicId,
    schema: Arc<Schema>,
    publish: RwPub,
    subscribe: Arc<broadcast::Sender<RecordBatch>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    stop: Arc<Notify>,
}

impl StreamingTable {
    pub fn new(topic: TopicId, rw: Arc<RwBuffer>) -> Self {
        let schema = rw.schema();
        let (pub_send, pub_recv) = mpsc::channel(100);
        let (sub_send, _) = broadcast::channel(100);
        let subscribe = Arc::new(sub_send);

        let stop = Arc::new(Notify::new());
        let active = Arc::new(AtomicUsize::new(0));
        let publish = RwPub {
            inner: pub_send,
            schema: schema.clone(),
            stop: stop.clone(),
            active: active.clone(),
        };
        let handle = tokio::spawn(Self::run(pub_recv, subscribe.clone(), rw, stop.clone()));
        let handle = Mutex::new(Some(handle));
        Self {
            topic,
            schema,
            publish,
            subscribe,
            handle,
            stop,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn publish(&self) -> RwPub {
        self.publish.clone()
    }

    pub fn subscribe(&self, stop_on_inactive: bool) -> RwSub {
        RwSub::new(self.subscribe_inner(stop_on_inactive))
    }

    pub async fn close(&self) {
        self.stop.notify_one();

        let mut lock = self.handle.lock().await;
        if let Some(handle) = lock.as_mut() {
            if let Err(error) = handle.await {
                tracing::error!(topic=%self.topic, error=?error, "streaming table worker panicked");
            }
            *lock = None;
        }
    }

    fn subscribe_inner(&self, stop_on_inactive: bool) -> RwSubInner {
        RwSubInner {
            inner: self.subscribe.subscribe(),
            stop: self.publish.stop.clone(),
            active: self.publish.active.clone(),
            stop_on_inactive,
        }
    }

    async fn run(
        mut recv: mpsc::Receiver<RecordBatch>,
        send: Arc<broadcast::Sender<RecordBatch>>,
        rw: Arc<RwBuffer>,
        stop: Arc<Notify>,
    ) {
        let wait_stop = stop.notified();
        futures::pin_mut!(wait_stop);

        loop {
            tokio::select! {
                biased;
                batch = recv.recv() => match batch {
                    Some(batch) => {
                        let _ = send.send(batch.clone());
                        rw.insert(batch);
                    },
                    None => break,
                },
                _ = &mut wait_stop => break,
            }
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for StreamingTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.arrow_schema().clone()
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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // let filters = create_physical_expr(
        //     conjunction(filters),
        //     self.schema.as_ref().to_dfschema(),
        //     &self.schema,
        //     state.execution_props(),
        // )?;
        Ok(Arc::new(TableExec::try_new(
            self.subscribe_inner(true),
            self.schema.clone(),
            projection.cloned(),
        )?))
    }
}

#[derive(Debug)]
struct TableExec {
    src: RwSubInner,
    schema: Arc<Schema>,
    projected_schema: Arc<ArrowSchema>,
    projection: Option<Vec<usize>>,
    order: Option<Vec<PhysicalSortExpr>>,
}

impl TableExec {
    fn try_new(
        src: RwSubInner,
        schema: Arc<Schema>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(schema.arrow_schema(), projection.as_ref())?;
        let order = schema.output_ordering();
        Ok(Self {
            src,
            schema,
            projected_schema,
            projection,
            order,
        })
    }
}

impl ExecutionPlan for TableExec {
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
        Ok(true)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let stream = RwSub::new(self.src.clone());
        if let Some(projection) = self.projection.clone() {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.projected_schema.clone(),
                stream.map(move |item| {
                    item.and_then(|b| b.project(projection.as_ref()).map_err(Into::into))
                }),
            )))
        } else {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.projected_schema.clone(),
                stream,
            )))
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
