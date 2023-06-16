pub mod parquet;
pub mod work_queue;

use std::{collections::HashSet, sync::Arc};

use futures::TryStreamExt;
use synapse_time::Duration;
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tracing::{Instrument, Level};

use crate::{catalog::Catalog, topic::compact_shards, SynapseContext, Topic};

#[derive(Debug)]
pub struct Maintainer {
    handle: Mutex<Option<JoinHandle<()>>>,
    stop: Arc<Notify>,
}

impl Maintainer {
    pub fn new(catalog: Arc<Catalog>, ctx: Arc<SynapseContext>, interval: Duration) -> Self {
        let stop = Arc::new(Notify::new());
        let worker = MaintenanceWorker {
            catalog,
            ctx,
            interval,
            stop: stop.clone(),
        };
        let handle = Mutex::new(Some(tokio::spawn(
            worker.run().instrument(tracing::trace_span!("maintainer")),
        )));
        Self { handle, stop }
    }

    pub async fn stop(&self) {
        self.stop.notify_one();
        let mut lock = self.handle.lock().await;
        if let Some(handle) = lock.as_mut() {
            if let Err(error) = handle.await {
                tracing::error!(error=?error, "maintenance worker panicked");
            }
            *lock = None;
        }
    }
}

struct MaintenanceWorker {
    catalog: Arc<Catalog>,
    ctx: Arc<SynapseContext>,
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
                    for topic in self.catalog.topics() {
                        if let Err(error) = self.compact_topic(&topic).await {
                            tracing::error!(topic=%topic.id(), error=?error, "failed to compact topic");
                        }

                        if let Err(error) = self.cleanup_topic(&topic).await {
                            tracing::error!(topic=%topic.id(), error=?error, "failed to cleanup topic");
                        }
                    }
                },
                _ = &mut stop => break,
            }
        }
    }

    #[tracing::instrument(skip_all, fields(topic=%topic.id()), level = Level::TRACE)]
    async fn compact_topic(&self, topic: &Arc<Topic>) -> crate::Result<()> {
        let mut pending = vec![];
        let mut pending_rows = 0;
        let target_rows = topic.config().target_shard_size;
        let shards = topic.shards().readable_shards().await;
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
                topic.schema().clone(),
                topic.shards().clone(),
                self.ctx.clone(),
                topic.config().shard_config(),
            )
            .await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(topic=%topic.id()), level = Level::TRACE)]
    async fn cleanup_topic(&self, topic: &Arc<Topic>) -> crate::Result<()> {
        let store = self.ctx.store();
        let mut files = store
            .list(Some(&topic.path().as_path()))
            .await?
            .map_ok(|f| f.location)
            .try_collect::<HashSet<_>>()
            .await?;
        let shards = topic.shards().all_shards().await;
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
