use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::{
    arrow::record_batch::RecordBatch,
    parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties},
};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::{
    io::AsyncWrite,
    sync::{oneshot, Notify},
    task::JoinHandle,
};

use crate::{
    catalog::{snapshot::ShardState, TopicId},
    topic::config::ShardConfig,
    util::parquet::cast_batch,
    Path, Schema, SynapseContext,
};

use super::ShardSet;

#[derive(Debug)]
pub struct ShardWriterWorker {
    schema: Arc<Schema>,
    topic: TopicId,
    ctx: Arc<SynapseContext>,
    stop: Arc<Notify>,
    config: ShardConfig,
    shards: Arc<ShardSet>,
    recv: flume::Receiver<WriteJob>,
}

impl ShardWriterWorker {
    pub(crate) fn new(
        schema: Arc<Schema>,
        topic: TopicId,
        ctx: Arc<SynapseContext>,
        stop: Arc<Notify>,
        config: ShardConfig,
        shards: Arc<ShardSet>,
        recv: flume::Receiver<WriteJob>,
    ) -> Self {
        Self {
            schema,
            topic,
            ctx,
            stop,
            config,
            shards,
            recv,
        }
    }

    pub(crate) async fn run(self) -> crate::Result<()> {
        let wait_stop = self.stop.notified();
        futures::pin_mut!(wait_stop);

        let mut pending = FuturesUnordered::new();
        let mut active: Option<JobHandle> = None;
        let mut len = 0;

        loop {
            tokio::select! {
                biased;
                job = self.recv.recv_async() => {
                    if let Ok(job) = job {
                        for batch in &job.values {
                            len += batch.num_rows();
                        }
                        let handle = if let Some(handle) = &mut active {
                            handle
                        } else {
                            let shard = SingleShardWriter::create(
                                &self.schema,
                                self.ctx.clone(),
                                &self.config,
                                self.shards.clone(),
                            ).await?;
                            active = Some(JobHandle::new(shard));
                            active.as_mut().unwrap()
                        };
                        if let Err(error) = handle.send(job) {
                            tracing::error!(topic=%self.topic, ?error, "failed to write to active shard");
                            active = None;
                        }
                        if len >= self.config.min_shard_size {
                            pending.push(std::mem::take(&mut active).unwrap().finish().boxed());
                            len = 0;
                        }
                    } else {
                        break;
                    }
                },
                // Clear pending write jobs from the queue
                res = pending.next(), if !pending.is_empty() => {
                    if let Some(Err(error)) = res {
                        tracing::error!(topic=%self.topic, ?error, "failed to write data shard to parquet");
                    }
                },
                _ = &mut wait_stop => break,
            }
        }
        tracing::debug!(topic=%self.topic, "shutting down shard writer");
        // Close active write channel
        if let Some(active) = active {
            pending.push(active.finish().boxed());
        }
        // Wait for all pending jobs to finish
        while let Some(res) = pending.next().await {
            if let Err(error) = res {
                tracing::error!(?error, "failed to write data shard to parquet");
            }
        }
        tracing::debug!(topic=%self.topic, "shard writer shut down");

        Ok(())
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
pub struct JobHandle {
    handle: Option<JoinHandle<crate::Result<()>>>,
    input: flume::Sender<WriteJob>,
}

impl JobHandle {
    pub fn new(shard: SingleShardWriter) -> Self {
        let (input, output) = flume::bounded(shard.config.queue_size);
        let handle = Some(tokio::spawn(Self::run(output, shard)));
        Self { handle, input }
    }

    pub fn send(&mut self, job: WriteJob) -> crate::Result<()> {
        if let Err(_) = self.input.try_send(job) {
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

pub struct SingleShardWriter {
    shard: ShardState,
    file_schema: Option<SchemaRef>,
    file: AsyncArrowWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    abort: String,
    num_rows: usize,
    shards: Arc<ShardSet>,
    ctx: Arc<SynapseContext>,
    config: ShardConfig,
}

impl SingleShardWriter {
    const BUFFER_SIZE: usize = 10;

    pub async fn create(
        schema: &Arc<Schema>,
        ctx: Arc<SynapseContext>,
        cfg: &ShardConfig,
        shards: Arc<ShardSet>,
    ) -> crate::Result<Self> {
        let shard = shards.create_shard((**schema).clone()).await?;
        let path = shard.path.clone();

        let (abort, file) = ctx.store().put_multipart(&path.as_path()).await?;

        let props = WriterProperties::builder()
            .set_sorting_columns(schema.sorting_columns())
            .set_max_row_group_size(cfg.row_group_size)
            .set_write_batch_size(cfg.write_batch_size)
            .build();

        let file_schema = schema.parquet_schema().cloned();
        let file = AsyncArrowWriter::try_new(
            file,
            file_schema
                .clone()
                .unwrap_or_else(|| schema.arrow_schema().clone()),
            Self::BUFFER_SIZE,
            Some(props),
        )?;

        Ok(Self {
            shard,
            abort,
            ctx,
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
        self.ctx
            .store()
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
