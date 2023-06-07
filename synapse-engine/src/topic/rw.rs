use std::fmt::Debug;
use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::{
    arrow::{compute, datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::TableType,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    prelude::Expr,
};

use flume::TrySendError;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use super::config::RwBufferConfig;
use super::ShardManager;
use crate::catalog::TopicId;
use crate::util::instrument::Instrument;
use crate::util::work_queue::{work_queue, WorkQueueIn, WorkQueueOut};
use crate::Schema;

pub struct RwBuffer {
    topic: TopicId,
    config: RwBufferConfig,
    schema: Arc<Schema>,
    input: Instrument<flume::Sender<RecordBatch>>,
    compacting: Arc<WorkQueueIn<RecordBatch>>,
    writing: Arc<WorkQueueIn<()>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    stop: Arc<Notify>,
}

impl Debug for RwBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwBuffer")
            .field("topic", &self.topic)
            .field("config", &self.config)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl RwBuffer {
    pub fn new(topic: TopicId, shards: Arc<ShardManager>, config: RwBufferConfig) -> Self {
        let schema = shards.schema();
        let (send, recv) = flume::bounded(config.queue_size);
        let input = Instrument::new(send, &format!("{}.rw.input", topic));
        let (compacting, compacting_out) = work_queue(config.queue_size);
        let (writing, writing_out) = work_queue(config.queue_size);
        let compacting = Arc::new(compacting);
        let writing = Arc::new(writing);
        let stop = Arc::new(Notify::new());

        let handle = tokio::spawn(Self::run(
            topic.clone(),
            recv,
            compacting.clone(),
            compacting_out,
            writing.clone(),
            writing_out,
            shards,
            stop.clone(),
            config.clone(),
            schema.clone(),
        ));
        let handle = Mutex::new(Some(handle));

        Self {
            topic,
            input,
            schema,
            handle,
            compacting,
            writing,
            stop,
            config,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn config(&self) -> &RwBufferConfig {
        &self.config
    }

    pub async fn insert(&self, batch: RecordBatch) -> crate::Result<()> {
        match self.input.send_async(batch).await {
            Ok(_) => Ok(()),
            Err(_) => Err(crate::Error::TableClosed),
        }
    }

    pub fn try_insert(&self, batch: RecordBatch) -> crate::Result<()> {
        match self.input.try_send(batch) {
            Ok(_) => Ok(()),
            Err(TrySendError::Disconnected(_)) => Err(crate::Error::TableClosed),
            Err(TrySendError::Full(_)) => Err(crate::Error::TableQueueFull),
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
        recv: flume::Receiver<RecordBatch>,
        compacting_in: Arc<WorkQueueIn<RecordBatch>>,
        mut compacting_out: WorkQueueOut<RecordBatch>,
        writing_in: Arc<WorkQueueIn<()>>,
        mut writing_out: WorkQueueOut<()>,
        shards: Arc<ShardManager>,
        stop: Arc<Notify>,
        config: RwBufferConfig,
        schema: Arc<Schema>,
    ) {
        let wait_stop = stop.notified();
        futures::pin_mut!(wait_stop);

        let compact_buffer = |rows: usize| {
            let arrow_schema = schema.arrow_schema().clone();
            let res = compacting_in.process(|mut values| async move {
                if values.len() == 1 {
                    return values.pop().unwrap();
                } else {
                    tokio::task::spawn_blocking(move || {
                        compute::concat_batches(&arrow_schema, &values).unwrap()
                    })
                    .await
                    .unwrap()
                }
            });
            match res {
                Ok(_) => tracing::debug!(%topic, rows, "compacting r/w buffer"),
                Err(error) => tracing::error!(?error, rows, "failed to compact r/w buffer"),
            }
        };

        let write_batch = |batch: RecordBatch| {
            let rows = batch.num_rows();
            writing_in.push(batch);
            let shards = shards.clone();
            let res = writing_in.try_process(|values| async move {
                let _ = shards.write(values)?.await;
                Ok(())
            });
            match res {
                Ok(_) => tracing::debug!(%topic, rows, "writing compacted buffer"),
                Err(error) => tracing::error!(?error, rows, "failed to write compacted buffer"),
            }
        };

        let mut len = 0;
        loop {
            tokio::select! {
                biased;
                batch = recv.recv_async() => match batch {
                    Ok(batch) => {
                        len += batch.num_rows();
                        compacting_in.push(batch);
                        if len >= config.write_batch_size {
                            compact_buffer(len);
                            len = 0;
                        }
                    },
                    Err(_) => break,
                },
                // Take compacted batches from the compacting queue and push them to th write queue.
                compacted = compacting_out.ready() => match compacted {
                    Some(res) => {
                        match res {
                            Ok(batch) => write_batch(batch),
                            Err(error) => tracing::error!(%topic, ?error, "failed to compact records"),
                        }
                    },
                    None => unreachable!(),
                },
                Some(res) = writing_out.ready() => {
                    if let Err(error) = res {
                        tracing::error!(%topic, ?error, "failed to write batch to disk");
                    }
                },
                _ = &mut wait_stop => break,
            }
        }
        tracing::debug!(topic=%topic, "shutting down R/W buffer worker");

        for batch in recv.drain() {
            len += batch.num_rows();
            compacting_in.push(batch);
            if len >= config.write_batch_size {
                compact_buffer(len);
                len = 0;
            }
        }
        if len > 0 {
            compact_buffer(len);
        }
        // Finish compacting any remaining rows and send the compacted batches to the write queue.
        compacting_out.close();
        while let Some(res) = compacting_out.ready().await {
            match res {
                Ok(batch) => write_batch(batch),
                Err(error) => tracing::error!(%topic, ?error, "failed to compact records"),
            }
        }

        // Close the write queue.
        // We don't need to wait for the queue to finish since that will be handled by the shard writer.
        writing_out.close();

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
        let compacting = self.compacting.values();
        let writing = self.writing.values();
        let mut table = MemoryExec::try_new(
            &[compacting, writing],
            self.schema().arrow_schema().clone(),
            projection.cloned(),
        )?;
        if let Some(sort) = self.schema.output_ordering() {
            table = table.with_sort_information(sort);
        }
        Ok(Arc::new(table))
    }
}
