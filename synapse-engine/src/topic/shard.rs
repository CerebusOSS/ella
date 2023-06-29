mod compact;
mod writer;

pub(crate) use compact::compact_shards;

use writer::{ShardWriterWorker, WriteJob};

use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    arrow::record_batch::RecordBatch,
    common::ToDFSchema,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result as DfResult},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    optimizer::utils::conjunction,
    physical_expr::create_physical_expr,
    physical_plan::{project_schema, ExecutionPlan, Statistics},
    prelude::Expr,
};
use futures::FutureExt;
use tokio::{
    sync::{oneshot, Mutex, Notify, RwLock},
    task::JoinHandle,
};

use crate::{
    catalog::{
        snapshot::{ShardState, TopicState},
        transactions::{CloseShard, CompactShards, CreateShard, DeleteShard},
        ShardId, TopicId,
    },
    metrics::{InstrumentedBuffer, MonitorLoadExt},
    util::parquet::cast_batch_plan,
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

    pub async fn all_shards(&self) -> Vec<ShardState> {
        self.shards
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>()
    }

    pub async fn start_compact(
        &self,
        src: Vec<ShardId>,
        schema: Schema,
    ) -> crate::Result<ShardState> {
        let mut shards = self.shards.write().await;
        let tsn = CompactShards::new(self.topic.clone(), src, schema, &self.path);
        self.ctx.log().commit(tsn.clone()).await?;
        let shard = ShardState::from(tsn);
        shards.insert(shard.id, shard.clone());
        Ok(shard)
    }

    pub async fn finish_compact(
        &self,
        src: Vec<ShardId>,
        dst: ShardId,
        rows: usize,
    ) -> crate::Result<()> {
        let mut shards = self.shards.write().await;
        let tsn = CloseShard::new(self.topic.clone(), dst, rows);
        self.ctx.log().commit(tsn).await?;

        if let Some(shard) = shards.get_mut(&dst) {
            shard.rows = Some(rows);
        }

        for &shard in &src {
            let tsn = DeleteShard::new(self.topic.clone(), shard);
            self.ctx.log().commit(tsn).await?;
            shards.remove(&shard);
        }
        tracing::debug!(
            topic=%self.topic,
            src=?src,
            dst=%dst,
            rows=rows,
            "finished compacting shards"
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct ShardManager {
    topic: TopicId,
    schema: Arc<Schema>,
    path: Path,
    ctx: Arc<SynapseContext>,
    shards: Arc<ShardSet>,
    input: InstrumentedBuffer<flume::Sender<WriteJob>>,
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
        let (input, output) = flume::bounded(config.queue_size);
        let input = input.monitor_load(&topic, "shard.manager");
        let stop = Arc::new(Notify::new());
        let worker = ShardWriterWorker::new(
            schema.clone(),
            topic.clone(),
            ctx.clone(),
            stop.clone(),
            config.clone(),
            shards.clone(),
            output,
        );
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

    pub fn write(&self, values: Vec<RecordBatch>) -> crate::Result<oneshot::Receiver<()>> {
        let (done, out) = oneshot::channel();
        let job = WriteJob::new(values, done);
        if let Err(_error) = self.input.try_send(job) {
            let mut handle = tokio::task::block_in_place(|| self.handle.blocking_lock());
            if let Some(h) = handle.as_mut() {
                if h.is_finished() {
                    let res = match h.now_or_never().expect("expected worker to be finished") {
                        Err(e) => Err(crate::EngineError::worker_panic(
                            "shard_writer",
                            &e.into_panic(),
                        )
                        .into()),
                        Ok(Err(e)) => Err(e),
                        Ok(Ok(_)) => Err(crate::EngineError::TableClosed.into()),
                    };
                    *handle = None;
                    res
                } else {
                    Err(crate::EngineError::TableClosed.into())
                }
            } else {
                Err(crate::EngineError::TableClosed.into())
            }
        } else {
            Ok(out)
        }
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

    pub fn shards(&self) -> &Arc<ShardSet> {
        &self.shards
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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

        let file_schema = self
            .schema
            .parquet_schema()
            .cloned()
            .unwrap_or_else(|| self.schema.arrow_schema().clone());

        let config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse(&self.path.store_url())?,
            file_schema,
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
        let mut plan = ParquetFormat::new()
            .create_physical_plan(state, config, filters.as_ref())
            .await?;

        if let Some(schema) = self.schema.parquet_schema() {
            let parquet_projected = project_schema(schema, projection)?;
            let arrow_projected = project_schema(self.schema.arrow_schema(), projection)?;
            if parquet_projected != arrow_projected {
                plan = cast_batch_plan(plan, &parquet_projected, &arrow_projected)?;
            }
        }
        Ok(plan)
    }
}
