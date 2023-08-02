use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::{
    arrow::record_batch::RecordBatch,
    parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties, format::SortingColumn},
};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use object_store::ObjectStore;
use tokio::{
    io::AsyncWrite,
    sync::{oneshot, Notify},
    task::JoinHandle,
};
use tracing::Instrument;

use crate::{
    table::{config::ShardConfig, info::EllaTableInfo},
    util::parquet::cast_batch,
    Path,
};

use super::{ShardInfo, ShardSet};

#[derive(Debug)]
pub struct ShardWriterWorker {
    table: EllaTableInfo,
    store: Arc<dyn ObjectStore>,
    stop: Arc<Notify>,
    config: ShardConfig,
    shards: Arc<ShardSet>,
    recv: flume::Receiver<WriteJob>,
    active: Option<JobHandle>,
    len: usize,
    pending: FuturesUnordered<BoxFuture<'static, crate::Result<()>>>,
}

impl ShardWriterWorker {
    pub(crate) fn new(
        table: EllaTableInfo,
        store: Arc<dyn ObjectStore>,
        stop: Arc<Notify>,
        config: ShardConfig,
        shards: Arc<ShardSet>,
        recv: flume::Receiver<WriteJob>,
    ) -> Self {
        Self {
            table,
            store,
            stop,
            config,
            shards,
            recv,
            active: None,
            len: 0,
            pending: FuturesUnordered::new(),
        }
    }

    pub(crate) async fn run(mut self) -> crate::Result<()> {
        let stop = self.stop.clone();
        let wait_stop = stop.notified();
        futures::pin_mut!(wait_stop);

        loop {
            tokio::select! {
                biased;
                //
                job = self.recv.recv_async() => {
                    if let Ok(job) = job {
                        self.handle_write(job).await?;
                    } else {
                        break;
                    }
                },
                // Clear pending write jobs from the queue
                res = self.pending.next(), if !self.pending.is_empty() => {
                    if let Some(Err(error)) = res {
                        tracing::error!(?error, "failed to write data shard to parquet");
                    }
                },
                _ = &mut wait_stop => break,
            }
        }
        tracing::debug!("shutting down shard writer");
        let remaining = self.recv.try_iter().collect::<Vec<_>>();
        for job in remaining {
            self.handle_write(job).await?;
        }
        debug_assert!(
            self.recv.is_empty(),
            "shard writer is closed but still has pending jobs"
        );

        // Close active write channel
        if let Some(active) = self.active {
            self.pending.push(active.finish().boxed());
        }
        // Wait for all pending jobs to finish
        while let Some(res) = self.pending.next().await {
            if let Err(error) = res {
                tracing::error!(?error, "failed to write data shard to parquet");
            }
        }
        tracing::debug!("shard writer shut down");

        Ok(())
    }

    async fn handle_write(&mut self, job: WriteJob) -> crate::Result<()> {
        for batch in &job.values {
            self.len += batch.num_rows();
        }
        let handle = if let Some(handle) = &mut self.active {
            handle
        } else {
            let shard = SingleShardWriter::create(
                self.table.arrow_schema().clone(),
                self.table.parquet_schema().cloned(),
                self.table.sorting_cols().cloned(),
                self.store.clone(),
                &self.config,
                self.shards.clone(),
            )
            .await?;
            self.active = Some(JobHandle::new(shard));
            self.active.as_mut().unwrap()
        };
        if let Err(error) = handle.send(job) {
            tracing::error!(?error, "failed to write to active shard");
            self.active = None;
        }
        if self.len >= self.config.min_shard_size {
            self.pending
                .push(std::mem::take(&mut self.active).unwrap().finish().boxed());
            self.len = 0;
        }
        crate::Result::Ok(())
    }
}

#[derive(Debug)]
pub struct WriteJob {
    values: Vec<RecordBatch>,
    done: oneshot::Sender<()>,
}

impl WriteJob {
    pub(crate) fn new(values: Vec<RecordBatch>, done: oneshot::Sender<()>) -> Self {
        Self { values, done }
    }
}

#[derive(Debug)]
pub(crate) struct JobHandle {
    handle: Option<JoinHandle<crate::Result<()>>>,
    input: flume::Sender<WriteJob>,
}

impl JobHandle {
    pub fn new(writer: SingleShardWriter) -> Self {
        let shard = writer.shard.id;
        let (input, output) = flume::bounded(writer.config.queue_size);
        let handle = Some(tokio::spawn(
            Self::run(output, writer)
                .in_current_span()
                .instrument(tracing::info_span!("writer", %shard)),
        ));
        Self { handle, input }
    }

    pub fn send(&mut self, job: WriteJob) -> crate::Result<()> {
        if self.input.try_send(job).is_err() {
            if let Some(handle) = &mut self.handle {
                if handle.is_finished() {
                    match self
                        .handle
                        .take()
                        .expect("join handle should not be empty")
                        .now_or_never()
                        .expect("expected worker to be complete")
                    {
                        Err(e) => Err(crate::EngineError::worker_panic(
                            "single_shard_writer",
                            &e.into_panic(),
                        )
                        .into()),
                        Ok(Err(e)) => Err(e),
                        Ok(Ok(_)) => unreachable!(),
                    }
                } else {
                    Err(crate::EngineError::TableQueueFull.into())
                }
            } else {
                Err(crate::EngineError::TableClosed.into())
            }
        } else {
            Ok(())
        }
    }

    pub async fn finish(self) -> crate::Result<()> {
        drop(self.input);
        if let Some(handle) = self.handle {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(crate::EngineError::worker_panic(
                    "single_shard_worker",
                    &e.into_panic(),
                )
                .into()),
            }
        } else {
            Ok(())
        }
    }

    async fn run(
        jobs: flume::Receiver<WriteJob>,
        mut shard: SingleShardWriter,
    ) -> crate::Result<()> {
        let mut pending = Vec::new();
        while let Ok(job) = jobs.recv_async().await {
            for batch in job.values {
                if let Err(error) = shard.write(&batch).await {
                    shard.abort().await?;
                    return Err(error);
                }
            }
            pending.push(job.done);
        }
        shard.close().await?;
        for p in pending {
            let _ = p.send(());
        }

        Ok(())
    }
}

pub(crate) struct SingleShardWriter {
    shard: ShardInfo,
    file_schema: Option<SchemaRef>,
    file: AsyncArrowWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    abort: String,
    num_rows: usize,
    shards: Arc<ShardSet>,
    store: Arc<dyn ObjectStore>,
    config: ShardConfig,
}

impl SingleShardWriter {
    const BUFFER_SIZE: usize = 10;

    pub async fn create(
        table_schema: SchemaRef,
        file_schema: Option<SchemaRef>,
        sort: Option<Vec<SortingColumn>>,
        store: Arc<dyn ObjectStore>,
        cfg: &ShardConfig,
        shards: Arc<ShardSet>,
    ) -> crate::Result<Self> {
        let schema = file_schema.clone().unwrap_or(table_schema);
        let shard = shards.create_shard(schema.clone()).await?;
        let path = shard.path.clone();

        let (abort, file) = store.put_multipart(&path.as_path()).await?;

        let props = WriterProperties::builder()
            .set_sorting_columns(sort)
            .set_max_row_group_size(cfg.row_group_size)
            .set_write_batch_size(cfg.write_batch_size)
            .build();

        let file = AsyncArrowWriter::try_new(file, schema, Self::BUFFER_SIZE, Some(props))?;

        Ok(Self {
            shard,
            abort,
            store,
            file_schema,
            file,
            shards,
            config: cfg.clone(),
            num_rows: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.shard.path
    }

    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        if let Some(schema) = &self.file_schema {
            let batch = cast_batch(batch, schema.clone())?;
            self.num_rows += batch.num_rows();
            Ok(self.file.write(&batch).await?)
        } else {
            self.num_rows += batch.num_rows();
            Ok(self.file.write(batch).await?)
        }
    }

    async fn abort(self) -> crate::Result<()> {
        self.shards.delete_shard(self.shard.id).await?;
        self.store
            .abort_multipart(&self.shard.path.as_path(), &self.abort)
            .await?;
        Ok(())
    }

    async fn close(self) -> crate::Result<()> {
        if self.num_rows == 0 {
            tracing::debug!(path=%self.path(), "discarding empty shard");
            return self.abort().await;
        }

        let meta = self.file.close().await?;
        debug_assert_eq!(self.num_rows, meta.num_rows as usize);
        self.shards
            .close_shard(self.shard.id, self.num_rows)
            .await?;
        Ok(())
    }
}
