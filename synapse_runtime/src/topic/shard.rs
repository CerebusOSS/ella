use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    arrow::record_batch::RecordBatch,
    common::ToDFSchema,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        TableProvider,
    },
    error::{DataFusionError, Result as DfResult},
    execution::context::SessionState,
    logical_expr::TableType,
    optimizer::utils::conjunction,
    parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties},
    physical_expr::create_physical_expr,
    physical_plan::{file_format::FileScanConfig, ExecutionPlan, Statistics},
    prelude::Expr,
};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{
    io::AsyncWrite,
    sync::{mpsc, oneshot, Mutex, Notify, RwLock},
    task::JoinHandle,
};

use crate::{
    catalog::{
        snapshot::{ShardState, TopicState},
        transactions::{CloseShard, CreateShard, DeleteShard},
        ShardId, TopicId,
    },
    Path, Schema, SynapseContext,
};

use super::config::ShardConfig;

#[derive(Debug)]
pub struct ShardSet {
    topic: TopicId,
    path: Path,
    shards: RwLock<BTreeMap<ShardId, ShardState>>,
    ctx: Arc<SynapseContext>,
}

impl ShardSet {
    pub fn new(ctx: Arc<SynapseContext>, state: &TopicState) -> Self {
        let topic = state.id.clone();
        let path = state.path.clone();
        let mut shards = BTreeMap::new();
        for shard in &state.shards {
            shards.insert(shard.id, shard.clone());
        }

        let shards = RwLock::new(shards);
        Self {
            ctx,
            shards,
            topic,
            path,
        }
    }

    pub async fn create_shard(&self, schema: Schema) -> crate::Result<ShardState> {
        let mut shards = self.shards.write().await;
        let tsn = CreateShard::new(self.topic.clone(), schema, &self.path);

        tracing::debug!(topic=%self.topic, path=%tsn.path, "creating new shard");
        self.ctx.log().commit(tsn.clone()).await?;
        let shard = ShardState::from(tsn);
        shards.insert(shard.id, shard.clone());
        Ok(shard)
    }

    pub async fn close_shard(&self, id: ShardId, rows: usize) -> crate::Result<()> {
        let mut shards = self.shards.write().await;
        let tsn = CloseShard::new(self.topic.clone(), id, rows);
        self.ctx.log().commit(tsn).await?;

        if let Some(shard) = shards.get_mut(&id) {
            shard.rows = Some(rows);
            tracing::debug!(topic=%self.topic, path=%shard.path, rows, "closed shard");
        } else {
            tracing::warn!(topic=%self.topic, shard=%id, "attempted to close missing shard");
        }
        Ok(())
    }

    pub async fn delete_shard(&self, id: ShardId) -> crate::Result<()> {
        let mut shards = self.shards.write().await;
        let tsn = DeleteShard::new(self.topic.clone(), id);
        self.ctx.log().commit(tsn).await?;
        if shards.remove(&id).is_some() {
            tracing::debug!(topic=%self.topic, shard=%id, "deleting shard");
        } else {
            tracing::warn!(topic=%self.topic, shard=%id, "attempted to delete missing shard");
        }
        Ok(())
    }

    pub async fn readable_shards(&self) -> Vec<ShardState> {
        self.shards
            .read()
            .await
            .values()
            .filter(|s| s.rows.is_some())
            .cloned()
            .collect::<Vec<_>>()
    }
}

#[derive(Debug)]
pub struct ShardManager {
    topic: TopicId,
    schema: Arc<Schema>,
    path: Path,
    ctx: Arc<SynapseContext>,
    shards: Arc<ShardSet>,
    input: mpsc::Sender<WriteJob>,
    stop: Arc<Notify>,
    handle: Mutex<Option<JoinHandle<crate::Result<()>>>>,
}

impl ShardManager {
    pub fn new(
        ctx: Arc<SynapseContext>,
        schema: Arc<Schema>,
        config: ShardConfig,
        state: &TopicState,
    ) -> Self {
        let topic = state.id.clone();
        let path = state.path.clone();
        let shards = Arc::new(ShardSet::new(ctx.clone(), state));
        let (input, output) = mpsc::channel(10);
        let stop = Arc::new(Notify::new());
        let worker = ShardWriterWorker {
            schema: schema.clone(),
            topic: topic.clone(),
            ctx: ctx.clone(),
            stop: stop.clone(),
            config,
            shards: shards.clone(),
            recv: output,
        };
        let handle = Mutex::new(Some(tokio::spawn(worker.run())));

        Self {
            topic,
            schema,
            path,
            ctx,
            shards,
            stop,
            handle,
            input,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn write(&self, values: RecordBatch) -> oneshot::Receiver<()> {
        let (done, out) = oneshot::channel();
        let job = WriteJob { done, values };
        if let Err(_error) = self.input.try_send(job) {
            tracing::error!(topic=?self.topic, "failed to write record batch to disk");
        }
        out
    }

    pub async fn close(&self) -> crate::Result<()> {
        self.stop.notify_one();
        let mut lock = self.handle.lock().await;
        if let Some(handle) = lock.as_mut() {
            let res = match handle.await {
                Ok(res) => res,
                Err(error) => {
                    tracing::error!(topic=%self.topic, error=?error, "shard writer worker panicked");
                    Ok(())
                }
            };
            *lock = None;
            res
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for ShardManager {
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
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let shards = self.shards.readable_shards().await;

        let files = futures::future::try_join_all(shards.iter().map(|s| async {
            let object_meta = self.ctx.store().head(&s.path.as_path()).await?;
            Result::<_, DataFusionError>::Ok(PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            })
        }))
        .await?;

        let table_partition_cols = vec![];
        let output_ordering = if let Some(order) = self.schema.output_ordering() {
            vec![order]
        } else {
            Vec::new()
        };

        let config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse(&self.path.store_url())?,
            file_schema: self.schema.arrow_schema().clone(),
            file_groups: vec![files],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols,
            output_ordering,
            infinite_source: false,
        };
        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            let table_df_schema = self.schema.arrow_schema().clone().to_dfschema()?;
            let filters = create_physical_expr(
                &expr,
                &table_df_schema,
                self.schema.arrow_schema(),
                state.execution_props(),
            )?;
            Some(filters)
        } else {
            None
        };
        ParquetFormat::new()
            .create_physical_plan(state, config, filters.as_ref())
            .await
    }
}

#[derive(Debug)]
pub struct ShardWriterWorker {
    schema: Arc<Schema>,
    topic: TopicId,
    ctx: Arc<SynapseContext>,
    stop: Arc<Notify>,
    config: ShardConfig,
    shards: Arc<ShardSet>,
    recv: mpsc::Receiver<WriteJob>,
}

impl ShardWriterWorker {
    async fn run(mut self) -> crate::Result<()> {
        let wait_stop = self.stop.notified();
        futures::pin_mut!(wait_stop);

        let mut pending = FuturesUnordered::new();
        let mut active: Option<JobHandle> = None;
        let mut len = 0;

        loop {
            tokio::select! {
                biased;
                job = self.recv.recv() => {
                    if let Some(job) = job {
                        len += job.values.num_rows();
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
                        handle.send(job);
                        if len >= self.config.min_shard_size {
                            pending.push(std::mem::take(&mut active).unwrap().finish());
                            len = 0;
                        }
                    } else {
                        break;
                    }
                },
                // Clear pending write jobs from the queue
                res = pending.next(), if !pending.is_empty() => {
                    if let Some(Err(error)) = res {
                        tracing::error!(?error, "failed to write data shard to parquet");
                    }
                },
                _ = &mut wait_stop => break,
            }
        }
        tracing::debug!(topic=%self.topic, "shutting down shard writer");
        // Close active write channel
        if let Some(active) = active {
            pending.push(active.finish());
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
    values: RecordBatch,
    done: oneshot::Sender<()>,
}

#[derive(Debug)]
pub struct JobHandle {
    handle: JoinHandle<crate::Result<()>>,
    input: mpsc::Sender<WriteJob>,
}

impl JobHandle {
    pub fn new(shard: SingleShardWriter) -> Self {
        let (input, output) = mpsc::channel(10);
        let handle = tokio::spawn(Self::run(output, shard));
        Self { handle, input }
    }

    pub fn send(&self, job: WriteJob) {
        if let Err(_e) = self.input.try_send(job) {
            todo!()
        }
    }

    pub fn finish(self) -> JoinHandle<crate::Result<()>> {
        self.handle
    }

    async fn run(
        mut jobs: mpsc::Receiver<WriteJob>,
        mut shard: SingleShardWriter,
    ) -> crate::Result<()> {
        let mut pending = Vec::new();
        while let Some(job) = jobs.recv().await {
            shard.write(&job.values).await?;
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
    file: AsyncArrowWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    abort: String,
    num_rows: usize,
    shards: Arc<ShardSet>,
    ctx: Arc<SynapseContext>,
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

        let file = AsyncArrowWriter::try_new(
            file,
            schema.arrow_schema().clone(),
            Self::BUFFER_SIZE,
            Some(props),
        )?;

        Ok(Self {
            shard,
            abort,
            ctx,
            file,
            shards,
            num_rows: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.shard.path
    }

    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        self.num_rows += batch.num_rows();
        Ok(self.file.write(batch).await?)
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
