pub mod parquet;
pub mod work_queue;

use std::{collections::HashSet, sync::Arc};

use arrow_schema::Schema;
use datafusion::{
    error::DataFusionError, physical_expr::PhysicalSortExpr, physical_plan::expressions::Column,
};
use futures::{TryFutureExt, TryStreamExt};
use synapse_common::Duration;
use tokio::{sync::Notify, task::JoinHandle, time::MissedTickBehavior};
use tracing::Instrument;

use crate::{
    engine::SynapseState,
    table::{topic::compact_shards, SynapseTable},
};

#[derive(Debug)]
pub struct Maintainer {
    handle: JoinHandle<()>,
    stop: Arc<Notify>,
}

impl Maintainer {
    pub fn new(state: Arc<SynapseState>, interval: Duration) -> Self {
        let stop = Arc::new(Notify::new());
        let worker = MaintenanceWorker {
            state,
            interval,
            stop: stop.clone(),
        };
        let handle = tokio::spawn(worker.run().instrument(tracing::info_span!("maintainer")));
        Self { handle, stop }
    }

    pub async fn stop(self) {
        self.stop.notify_one();
        if let Err(error) = self.handle.await {
            tracing::error!(error=?error, "maintenance worker panicked");
        }
    }
}

struct MaintenanceWorker {
    state: Arc<SynapseState>,
    interval: Duration,
    stop: Arc<Notify>,
}

impl MaintenanceWorker {
    async fn run(self) {
        let stop = self.stop.notified();
        futures::pin_mut!(stop);
        let mut interval = tokio::time::interval(self.interval.unsigned_abs());
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let tables = self.state.cluster().catalogs()
                        .into_iter()
                        .flat_map(|c| c.schemas())
                        .flat_map(|s| s.tables());

                    for table in tables {
                        self.compact_table(&table)
                            .unwrap_or_else(|error| {
                                tracing::error!(error=?error, "failed to compact topic");
                            })
                            .instrument(tracing::info_span!("compact", table=%table.id()))
                            .await;

                        self.cleanup_table(&table)
                            .unwrap_or_else(|error| {
                                tracing::error!(error=?error, "failed to cleanup topic");
                            })
                            .instrument(tracing::info_span!("compact", table=%table.id()))
                            .await;
                    }
                },
                _ = &mut stop => break,
            }
        }
    }

    async fn compact_table(&self, table: &Arc<SynapseTable>) -> crate::Result<()> {
        let mut pending = vec![];
        let mut pending_rows = 0;
        let target_rows = table.config().target_shard_size;
        let shard_set = match table.shards() {
            Some(s) => s.clone(),
            None => return Ok(()),
        };
        let shards = shard_set.readable_shards().await;
        for shard in &shards {
            if let Some(rows) = shard.rows {
                if rows < target_rows {
                    pending.push(shard.clone());
                    pending_rows += rows;

                    if pending_rows >= target_rows {
                        break;
                    }
                }
            }
        }
        if pending.len() > 1 {
            compact_shards(
                pending,
                table.file_schema(),
                table.sort(),
                shard_set,
                self.state.clone(),
                table.config().shard_config(),
            )
            .await?;
        }
        Ok(())
    }

    async fn cleanup_table(&self, table: &Arc<SynapseTable>) -> crate::Result<()> {
        let store = self.state.store();
        let mut files = store
            .list(Some(&table.path().as_path()))
            .await?
            .map_ok(|f| f.location)
            .try_collect::<HashSet<_>>()
            .await?;

        let shards = match table.shards() {
            Some(s) => s.all_shards().await,
            None => return Ok(()),
        };
        for shard in shards {
            files.remove(&shard.path.as_path());
        }
        let mut paths =
            store.delete_stream(Box::pin(futures::stream::iter(files.into_iter().map(Ok))));
        while let Some(path) = paths.try_next().await? {
            tracing::warn!(%path, "cleaning up orphaned file");
        }
        Ok(())
    }
}

pub(crate) fn project_ordering(
    schema: &Schema,
    projection: &[usize],
    ordering: &[PhysicalSortExpr],
) -> Result<Vec<PhysicalSortExpr>, DataFusionError> {
    let projected = schema.project(projection)?;
    let mut out = Vec::with_capacity(ordering.len());
    for PhysicalSortExpr { expr, options } in ordering {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            let name = col.name();
            if let Some((idx, _)) = projected.column_with_name(name) {
                out.push(PhysicalSortExpr {
                    expr: Arc::new(Column::new(name, idx)),
                    options: *options,
                });
                continue;
            }
        }
        break;
    }
    Ok(out)
}
