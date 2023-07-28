mod compact;
mod writer;

pub(crate) use compact::compact_shards;

use object_store::ObjectStore;
use tracing::Instrument;
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
    metrics::{InstrumentedBuffer, LoadLabels, MonitorLoadExt},
    registry::{
        transactions::{CloseShard, CompactShards, CreateShard, DeleteShard},
        ShardId, TableId, TransactionLog,
    },
    table::{config::ShardConfig, info::EllaTableInfo},
    util::parquet::cast_batch_plan,
    Path,
};

#[derive(Debug)]
pub(crate) struct ShardSet {
    table: TableId<'static>,
    path: Path,
    shards: RwLock<BTreeMap<ShardId, ShardInfo>>,
    log: Arc<TransactionLog>,
}

impl ShardSet {
    pub fn new(table: &EllaTableInfo, log: Arc<TransactionLog>) -> Self {
        let path = table.path().clone();
        let mut shards = BTreeMap::new();
        for shard in table.shards() {
            shards.insert(shard.id, shard.clone());
        }

        let shards = RwLock::new(shards);
        Self {
            log,
            shards,
            table: table.id().clone(),
            path,
        }
    }

    #[tracing::instrument(skip_all, fields(shard=tracing::field::Empty))]
    pub async fn create_shard(&self, file_schema: SchemaRef) -> crate::Result<ShardInfo> {
        let mut shards = self.shards.write().await;
        let tsn = CreateShard::new(self.table.clone(), file_schema, &self.path);
        tracing::Span::current().record("shard", tsn.shard.to_string());

        tracing::debug!(path=%tsn.path, "creating new shard");
        self.log.commit(tsn.clone()).await?;
        let shard = ShardInfo::from(tsn);
        shards.insert(shard.id, shard.clone());
        Ok(shard)
    }

    #[tracing::instrument(skip(self, id), fields(shard=%id))]
    pub async fn close_shard(&self, id: ShardId, rows: usize) -> crate::Result<()> {
        let mut shards = self.shards.write().await;
        let tsn = CloseShard::new(self.table.clone(), id, rows);
        self.log.commit(tsn).await?;

        if let Some(shard) = shards.get_mut(&id) {
            shard.rows = Some(rows);
            tracing::debug!(path=%shard.path, "closed shard");
        } else {
            tracing::warn!("attempted to close missing shard");
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(shard=%id))]
    pub async fn delete_shard(&self, id: ShardId) -> crate::Result<()> {
        let mut shards = self.shards.write().await;
        let tsn = DeleteShard::new(self.table.clone(), id);
        self.log.commit(tsn).await?;
        if shards.remove(&id).is_some() {
            tracing::debug!("deleting shard");
        } else {
            tracing::warn!("attempted to delete missing shard");
        }
        Ok(())
    }

    pub async fn readable_shards(&self) -> Vec<ShardInfo> {
        self.shards
            .read()
            .await
            .values()
            .filter(|s| s.rows.is_some())
            .cloned()
            .collect::<Vec<_>>()
    }

    pub async fn all_shards(&self) -> Vec<ShardInfo> {
        self.shards
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>()
    }

    #[tracing::instrument(skip_all)]
    pub async fn start_compact(
        &self,
        src: Vec<ShardId>,
        file_schema: SchemaRef,
    ) -> crate::Result<ShardInfo> {
        let mut shards = self.shards.write().await;
        let tsn = CompactShards::new(self.table.clone(), src, file_schema, &self.path);
        self.log.commit(tsn.clone()).await?;
        let shard = ShardInfo::from(tsn);
        shards.insert(shard.id, shard.clone());
        Ok(shard)
    }

    #[tracing::instrument(skip(self, src, dst))]
    pub async fn finish_compact(
        &self,
        src: Vec<ShardId>,
        dst: ShardId,
        rows: usize,
    ) -> crate::Result<()> {
        let mut shards = self.shards.write().await;
        let tsn = CloseShard::new(self.table.clone(), dst, rows);
        self.log.commit(tsn).await?;

        if let Some(shard) = shards.get_mut(&dst) {
            shard.rows = Some(rows);
        }

        for &shard in &src {
            let tsn = DeleteShard::new(self.table.clone(), shard);
            self.log.commit(tsn).await?;
            shards.remove(&shard);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ShardManager {
    table: EllaTableInfo,
    store: Arc<dyn ObjectStore>,
    shards: Arc<ShardSet>,
    input: InstrumentedBuffer<flume::Sender<WriteJob>>,
    stop: Arc<Notify>,
    handle: Mutex<Option<JoinHandle<crate::Result<()>>>>,
}

impl ShardManager {
    pub fn new(
        log: Arc<TransactionLog>,
        store: Arc<dyn ObjectStore>,
        table: EllaTableInfo,
        config: ShardConfig,
    ) -> Self {
        let shards = Arc::new(ShardSet::new(&table, log));
        let (input, output) = flume::bounded(config.queue_size);
        let input = input.monitor_load(
            LoadLabels::new("input")
                .with_task("shard_manager")
                .with(table.id()),
        );
        let stop = Arc::new(Notify::new());
        let worker = ShardWriterWorker::new(
            table.clone(),
            store.clone(),
            stop.clone(),
            config,
            shards.clone(),
            output,
        );
        let handle = Mutex::new(Some(tokio::spawn(
            worker
                .run()
                .instrument(tracing::info_span!("shard", table=%table.id())),
        )));

        Self {
            table,
            store,
            shards,
            stop,
            handle,
            input,
        }
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

    #[tracing::instrument(skip(self), fields(table=%self.table()))]
    pub async fn close(&self) -> crate::Result<()> {
        self.stop.notify_one();
        let mut lock = self.handle.lock().await;
        if let Some(handle) = lock.as_mut() {
            let res = match handle.await {
                Ok(res) => res,
                Err(error) => {
                    tracing::error!(error=?error, "shard writer worker panicked");
                    Ok(())
                }
            };
            *lock = None;
            res
        } else {
            Ok(())
        }
    }

    pub async fn delete_all(&self) -> crate::Result<()> {
        for shard in self.shards.all_shards().await {
            self.shards.delete_shard(shard.id).await?;
        }
        Ok(())
    }

    pub fn shards(&self) -> &Arc<ShardSet> {
        &self.shards
    }

    pub fn path(&self) -> &Path {
        self.table.path()
    }

    pub fn table(&self) -> &TableId {
        self.table.id()
    }
}

#[async_trait::async_trait]
impl TableProvider for ShardManager {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.arrow_schema().clone()
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
            let object_meta = self.store.head(&s.path.as_path()).await?;
            Result::<_, DataFusionError>::Ok(PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            })
        }))
        .await?;

        let table_partition_cols = vec![];
        let output_ordering = if let Some(order) = self.table.output_ordering() {
            vec![order.to_vec()]
        } else {
            Vec::new()
        };

        let file_schema = self
            .table
            .parquet_schema()
            .cloned()
            .unwrap_or_else(|| self.table.arrow_schema().clone());

        let config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse(self.path().store_url())?,
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
            let table_df_schema = self.table.arrow_schema().clone().to_dfschema()?;
            let filters = create_physical_expr(
                &expr,
                &table_df_schema,
                self.table.arrow_schema(),
                state.execution_props(),
            )?;
            Some(filters)
        } else {
            None
        };
        let mut plan = ParquetFormat::new()
            .create_physical_plan(state, config, filters.as_ref())
            .await?;

        if let Some(schema) = self.table.parquet_schema() {
            let parquet_projected = project_schema(schema, projection)?;
            let arrow_projected = project_schema(self.table.arrow_schema(), projection)?;
            if parquet_projected != arrow_projected {
                plan = cast_batch_plan(plan, &parquet_projected, &arrow_projected)?;
            }
        }
        Ok(plan)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardInfo {
    pub id: ShardId,
    pub table: TableId<'static>,
    pub file_schema: SchemaRef,
    pub path: Path,
    pub rows: Option<usize>,
}

impl ShardInfo {
    pub fn new(id: ShardId, table: TableId<'static>, file_schema: SchemaRef, path: Path) -> Self {
        Self {
            id,
            table,
            file_schema,
            path,
            rows: None,
        }
    }

    pub fn close(&mut self, rows: usize) {
        self.rows = Some(rows);
    }
}
