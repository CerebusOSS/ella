use futures::TryStreamExt;
use object_store::ObjectStore;

use crate::{config::EllaConfig, Path};
use std::sync::Arc;

use super::{snapshot::Snapshot, transactions::Transaction};

#[derive(Debug)]
pub struct TransactionLog {
    path: Path,
    store: Arc<dyn ObjectStore>,
}

impl TransactionLog {
    const EXT: &'static str = "txt";
    const SNAPSHOTS: &'static str = "snapshots";
    const TRANSACTIONS: &'static str = "transactions";

    pub fn new(path: Path, store: Arc<dyn ObjectStore>) -> Self {
        Self { path, store }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub async fn load_config(&self) -> crate::Result<EllaConfig> {
        let Snapshot { config, .. } = self.load_snapshot().await?;
        Ok(config)
    }

    pub async fn create(&self, config: EllaConfig) -> crate::Result<()> {
        self.write_snapshot(&Snapshot::empty(config)).await
    }

    pub async fn commit<T>(&self, tsn: T) -> crate::Result<()>
    where
        T: Into<Transaction>,
    {
        let tsn: Transaction = tsn.into();
        let path = tsn
            .uuid()
            .encode_path(&self.path.join(Self::TRANSACTIONS), Self::EXT);
        let raw = serde_json::to_vec(&tsn)?;
        self.store.put(&path.as_path(), raw.into()).await?;
        Ok(())
    }

    pub async fn create_snapshot(&self) -> crate::Result<()> {
        let transactions = self.load_transactions().await?;

        if transactions.is_empty() {
            return Ok(());
        }

        let mut snapshot = self
            .load_newest_snapshot()
            .await?
            .ok_or_else(|| crate::EngineError::InvalidDatastore(self.path.to_string()))?;

        snapshot.commit_many(transactions.clone())?;
        self.write_snapshot(&snapshot).await?;
        self.clear_transactions(transactions).await?;
        Ok(())
    }

    pub async fn load_snapshot(&self) -> crate::Result<Snapshot> {
        let mut snapshot = self
            .load_newest_snapshot()
            .await?
            .ok_or_else(|| crate::EngineError::InvalidDatastore(self.path.to_string()))?;
        tracing::debug!(uuid=%snapshot.uuid, "loaded snapshot");
        snapshot.commit_many(self.load_transactions().await?)?;
        Ok(snapshot)
    }

    async fn write_snapshot(&self, snapshot: &Snapshot) -> crate::Result<()> {
        tracing::info!(uuid=%snapshot.uuid, "saving catalog snapshot");

        let path = snapshot
            .uuid
            .encode_path(&self.path.join(Self::SNAPSHOTS), Self::EXT);
        let raw = serde_json::to_vec(&snapshot)?;
        self.store.put(&path.as_path(), raw.into()).await?;
        Ok(())
    }

    async fn load_transactions(&self) -> crate::Result<Vec<Transaction>> {
        let mut file_list = self
            .store
            .list(Some(&self.path.join(Self::TRANSACTIONS).as_path()))
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        // Sort oldest to newest
        file_list.sort_unstable_by(|a, b| a.location.filename().cmp(&b.location.filename()));

        let mut transactions = Vec::with_capacity(file_list.len());
        for file in file_list {
            let raw = self.store.get(&file.location).await?.bytes().await?;
            let t = serde_json::from_slice(&raw)?;
            transactions.push(t);
        }

        Ok(transactions)
    }

    async fn load_newest_snapshot(&self) -> crate::Result<Option<Snapshot>> {
        let mut file_list = self
            .store
            .list(Some(&self.path.join(Self::SNAPSHOTS).as_path()))
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        if file_list.is_empty() {
            return Ok(None);
        }
        // Sort newest to oldest (reverse order)
        let (_, first, _) = file_list
            .select_nth_unstable_by(0, |a, b| b.location.filename().cmp(&a.location.filename()));
        let raw = self.store.get(&first.location).await?.bytes().await?;
        Ok(Some(serde_json::from_slice(&raw)?))
    }

    async fn clear_transactions<I>(&self, transactions: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = Transaction>,
    {
        for t in transactions {
            let path = t
                .uuid()
                .encode_path(&self.path.join(Self::TRANSACTIONS), Self::EXT);
            self.store.delete(&path.as_path()).await?;
        }
        Ok(())
    }
}
