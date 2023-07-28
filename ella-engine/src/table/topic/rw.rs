use std::fmt::Debug;
use std::sync::Arc;
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

use flume::r#async::SendSink;
use futures::{Sink, SinkExt};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::Instrument;

use super::ShardManager;
use crate::metrics::{InstrumentedBuffer, LoadLabels, MonitorLoadExt};
use crate::registry::TableId;
use crate::table::config::RwBufferConfig;
use crate::table::info::EllaTableInfo;
use crate::util::work_queue::{work_queue, WorkQueueIn, WorkQueueOut};

pub(crate) struct RwBuffer {
    table: EllaTableInfo,
    config: RwBufferConfig,
    input: InstrumentedBuffer<SendSink<'static, RecordBatch>>,
    compacting: Arc<WorkQueueIn<RecordBatch>>,
    writing: Arc<WorkQueueIn<()>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    stop: Arc<Notify>,
}

impl Debug for RwBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwBuffer")
            .field("table", &self.table)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RwBuffer {
    pub fn new(table: EllaTableInfo, shards: Arc<ShardManager>, config: RwBufferConfig) -> Self {
        let (send, recv) = flume::bounded(config.queue_size);

        let input = send
            .into_sink()
            .monitor_load(LoadLabels::new("input").with_task("rw").with(table.id()));
        let (compacting, compacting_out) = work_queue(config.queue_size);
        let (writing, writing_out) = work_queue(config.queue_size);
        let compacting = Arc::new(compacting);
        let writing = Arc::new(writing);
        let stop = Arc::new(Notify::new());
        let worker = RwBufferWorker {
            arrow_schema: table.arrow_schema().clone(),
            recv,
            compacting_in: compacting.clone(),
            compacting_out,
            writing_in: writing.clone(),
            writing_out,
            shards,
            stop: stop.clone(),
            config: config.clone(),
        };

        let handle = tokio::spawn(
            worker
                .run()
                .instrument(tracing::info_span!("rw", table=%table.id())),
        );
        let handle = Mutex::new(Some(handle));

        Self {
            table,
            input,
            handle,
            compacting,
            writing,
            stop,
            config,
        }
    }

    pub fn sink(this: Option<Arc<Self>>) -> RwBufferSink {
        RwBufferSink(this.map(|rw| rw.input.clone()))
    }

    #[tracing::instrument(skip(self), fields(table=%self.table()))]
    pub async fn close(&self) {
        self.stop.notify_one();

        let mut lock = self.handle.lock().await;
        if let Some(handle) = lock.as_mut() {
            if let Err(error) = handle.await {
                tracing::error!(error=?error, "R/W buffer worker panicked");
            }
            *lock = None;
        }
    }

    pub fn table(&self) -> &TableId {
        self.table.id()
    }
}

#[derive(Debug)]
struct RwBufferWorker {
    arrow_schema: SchemaRef,
    recv: flume::Receiver<RecordBatch>,
    compacting_in: Arc<WorkQueueIn<RecordBatch>>,
    compacting_out: WorkQueueOut<RecordBatch>,
    writing_in: Arc<WorkQueueIn<()>>,
    writing_out: WorkQueueOut<()>,
    shards: Arc<ShardManager>,
    stop: Arc<Notify>,
    config: RwBufferConfig,
}

impl RwBufferWorker {
    async fn run(mut self) {
        let wait_stop = self.stop.notified();
        futures::pin_mut!(wait_stop);

        let compact_buffer = |rows: usize| {
            let arrow_schema = self.arrow_schema.clone();
            let res = self.compacting_in.process(|mut values| {
                async move {
                    if values.len() == 1 {
                        values.pop().unwrap()
                    } else {
                        tokio::task::spawn_blocking(move || {
                            compute::concat_batches(&arrow_schema, &values).unwrap()
                        })
                        .await
                        .unwrap()
                    }
                }
                .in_current_span()
            });
            match res {
                Ok(_) => tracing::debug!(rows, "compacting r/w buffer"),
                Err(error) => tracing::error!(?error, rows, "failed to compact r/w buffer"),
            }
        };

        let write_batch = |batch: RecordBatch| {
            let rows = batch.num_rows();
            self.writing_in.push(batch);
            let shards = self.shards.clone();
            let res = self.writing_in.try_process(|values| {
                async move {
                    let _ = shards.write(values)?.await;
                    Ok(())
                }
                .in_current_span()
            });
            match res {
                Ok(_) => tracing::debug!(rows, "writing compacted buffer"),
                Err(error) => tracing::error!(?error, rows, "failed to write compacted buffer"),
            }
        };

        let mut len = 0;
        loop {
            tokio::select! {
                biased;
                batch = self.recv.recv_async() => match batch {
                    Ok(batch) => {
                        len += batch.num_rows();
                        self.compacting_in.push(batch);
                        if len >= self.config.write_batch_size {
                            compact_buffer(len);
                            len = 0;
                        }
                    },
                    Err(_) => break,
                },
                // Take compacted batches from the compacting queue and push them to th write queue.
                compacted = self.compacting_out.ready() => match compacted {
                    Some(res) => {
                        match res {
                            Ok(batch) => write_batch(batch),
                            Err(error) => tracing::error!(?error, "failed to compact records"),
                        }
                    },
                    None => unreachable!(),
                },
                Some(res) = self.writing_out.ready() => {
                    if let Err(error) = res {
                        tracing::error!(?error, "failed to write batch to disk");
                    }
                },
                _ = &mut wait_stop => break,
            }
        }
        tracing::debug!("shutting down R/W buffer worker");

        for batch in self.recv.drain() {
            len += batch.num_rows();
            self.compacting_in.push(batch);
            if len >= self.config.write_batch_size {
                compact_buffer(len);
                len = 0;
            }
        }
        if len > 0 {
            compact_buffer(len);
        }
        // Finish compacting any remaining rows and send the compacted batches to the write queue.
        self.compacting_out.close();
        while let Some(res) = self.compacting_out.ready().await {
            match res {
                Ok(batch) => write_batch(batch),
                Err(error) => {
                    tracing::error!(?error, "failed to compact records")
                }
            }
        }

        // Close the write queue.
        // We don't need to wait for the queue to finish since that will be handled by the shard writer.
        self.writing_out.close();

        tracing::debug!("R/W buffer worker shutdown");
    }
}

#[derive(Clone)]
pub struct RwBufferSink(Option<InstrumentedBuffer<SendSink<'static, RecordBatch>>>);

impl Debug for RwBufferSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwBufferSink").finish_non_exhaustive()
    }
}

impl Sink<RecordBatch> for RwBufferSink {
    type Error = crate::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(buf) = &mut self.0 {
            buf.poll_ready_unpin(cx)
                .map_err(|_| crate::EngineError::TableClosed.into())
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: RecordBatch,
    ) -> Result<(), Self::Error> {
        if let Some(buf) = &mut self.0 {
            buf.start_send_unpin(item)
                .map_err(|_| crate::EngineError::TableClosed.into())
        } else {
            Ok(())
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(buf) = &mut self.0 {
            buf.poll_flush_unpin(cx)
                .map_err(|_| crate::EngineError::TableClosed.into())
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(buf) = &mut self.0 {
            buf.poll_close_unpin(cx)
                .map_err(|_| crate::EngineError::TableClosed.into())
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for RwBuffer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.arrow_schema().clone()
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
            &[writing, compacting],
            self.table.arrow_schema().clone(),
            projection.cloned(),
        )?;
        if let Some(mut sort) = self.table.output_ordering() {
            if let Some(projection) = projection {
                sort = crate::util::project_ordering(self.table.arrow_schema(), projection, &sort)?;
            }
            table = table.with_sort_information(sort);
        }
        Ok(Arc::new(table))
    }
}
