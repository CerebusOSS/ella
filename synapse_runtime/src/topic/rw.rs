use std::sync::{Arc, Mutex as SyncMutex, MutexGuard};
use std::task::Poll;

use datafusion::error::Result as DfResult;
use datafusion::{
    arrow::{compute, datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::TableType,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    prelude::Expr,
};
use futures::{Future, FutureExt};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, Mutex, Notify};
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};

use super::config::RwBufferConfig;
use super::ShardManager;
use crate::catalog::TopicId;
use crate::Schema;

#[derive(Debug)]
pub struct RwBuffer {
    topic: TopicId,
    config: RwBufferConfig,
    schema: Arc<Schema>,
    input: mpsc::Sender<RecordBatch>,
    buffer: Arc<RwLock<BatchWriteBuffer>>,
    pending: Arc<PendingQueue>,
    handle: Mutex<Option<JoinHandle<()>>>,
    stop: Arc<Notify>,
}

impl RwBuffer {
    pub fn new(topic: TopicId, shards: Arc<ShardManager>, config: RwBufferConfig) -> Self {
        let schema = shards.schema();
        let buffer = Arc::new(RwLock::new(BatchWriteBuffer::new(&schema)));
        let (input, recv) = mpsc::channel(100);
        let pending = Arc::new(PendingQueue::new());
        let stop = Arc::new(Notify::new());

        let handle = tokio::spawn(Self::run(
            topic.clone(),
            buffer.clone(),
            recv,
            pending.clone(),
            shards,
            stop.clone(),
            config.clone(),
        ));
        let handle = Mutex::new(Some(handle));

        Self {
            topic,
            input,
            schema,
            buffer,
            handle,
            pending,
            stop,
            config,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn insert(&self, batch: RecordBatch) {
        if let Err(_error) = self.input.try_send(batch) {
            tracing::error!(topic=?self.topic, "failed to write record batch to table");
        }
    }

    pub async fn close(&self) {
        self.stop.notify_one();

        let mut lock = self.handle.lock().await;
        if let Some(handle) = lock.as_mut() {
            if let Err(error) = handle.await {
                tracing::error!(topic=%self.topic, error=?error, "R/W buffer worker panicked");
            }
            *lock = None;
        }
    }

    async fn run(
        topic: TopicId,
        buffer: Arc<RwLock<BatchWriteBuffer>>,
        mut recv: mpsc::Receiver<RecordBatch>,
        pending: Arc<PendingQueue>,
        shards: Arc<ShardManager>,
        stop: Arc<Notify>,
        config: RwBufferConfig,
    ) {
        let wait_stop = stop.notified();
        futures::pin_mut!(wait_stop);

        let compact_buffer = |buf: &mut BatchWriteBuffer| {
            tracing::debug!(topic=%topic, rows=buf.len(), "compacting R/W buffer");
            let compacted = buf.compact().unwrap();
            let done = shards.write(compacted.clone());
            pending.push(compacted, done);
        };

        loop {
            tokio::select! {
                biased;
                batch = recv.recv() => match batch {
                    Some(batch) => {
                        let mut buf = buffer.write().await;
                        buf.write(batch);

                        if buf.len() >= config.capacity {
                            compact_buffer(&mut buf);
                        }
                    },
                    None => break,
                },
                _ = &mut wait_stop => break,
            }
        }
        tracing::debug!(topic=%topic, "shutting down R/W buffer worker");

        // Push any rows in the buffer to the write queue
        let mut buf = buffer.write().await;
        if buf.len() > 0 {
            compact_buffer(&mut buf);
        }

        // We don't need to wait for write jobs to finish since we're only tracking them
        // to make sure we hold onto data until it's persisted to disk
        tracing::debug!(topic=%topic, "R/W buffer worker shutdown");
    }
}

#[async_trait::async_trait]
impl TableProvider for RwBuffer {
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let batches = self.buffer.read().await.items().clone();
        let pending = self.pending.values();
        Ok(Arc::new(MemoryExec::try_new(
            &[batches, pending],
            self.schema().arrow_schema().clone(),
            projection.cloned(),
        )?))
    }
}

#[derive(Debug)]
pub struct BatchWriteBuffer {
    arrow_schema: SchemaRef,
    items: Vec<RecordBatch>,
    len: usize,
}

impl BatchWriteBuffer {
    pub fn new(schema: &Schema) -> Self {
        let arrow_schema = schema.arrow_schema().clone();
        Self {
            arrow_schema,
            items: Vec::new(),
            len: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn items(&self) -> &Vec<RecordBatch> {
        &self.items
    }

    pub fn write(&mut self, batch: RecordBatch) {
        self.len += batch.num_rows();
        self.items.push(batch);
    }

    pub fn compact(&mut self) -> crate::Result<RecordBatch> {
        let compacted = compute::concat_batches(&self.arrow_schema, self.items.iter())?;
        self.len = 0;
        self.items.clear();
        Ok(compacted)
    }
}

#[derive(Debug)]
pub struct PendingJob {
    batch: RecordBatch,
    done: oneshot::Receiver<()>,
}

impl PendingJob {
    pub fn ready(&mut self) -> bool {
        match self.done.try_recv() {
            Ok(_) | Err(TryRecvError::Closed) => true,
            Err(TryRecvError::Empty) => false,
        }
    }
}

impl Future for PendingJob {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let _ = futures::ready!(self.done.poll_unpin(cx));
        Poll::Ready(())
    }
}

#[derive(Debug)]
pub struct PendingQueue {
    values: SyncMutex<Vec<PendingJob>>,
}

impl PendingQueue {
    pub fn new() -> Self {
        let values = SyncMutex::new(Vec::new());
        Self { values }
    }

    pub fn values(&self) -> Vec<RecordBatch> {
        let mut values = self.values.lock().unwrap();
        Self::prune(&mut values);
        values
            .iter()
            .map(|job| job.batch.clone())
            .collect::<Vec<_>>()
    }

    pub fn push(&self, batch: RecordBatch, done: oneshot::Receiver<()>) {
        let job = PendingJob { batch, done };
        let mut values = self.values.lock().unwrap();
        Self::prune(&mut values);
        values.push(job);
    }

    fn prune(values: &mut MutexGuard<'_, Vec<PendingJob>>) {
        values.retain_mut(|job| !job.ready());
    }
}
