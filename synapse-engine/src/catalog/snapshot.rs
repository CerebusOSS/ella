use super::id::*;
use super::transactions::*;
use crate::Path;

use crate::Schema;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub uuid: SnapshotId,
    pub last_transaction: Option<TransactionId>,
    pub topics: Vec<TopicState>,
}

impl Snapshot {
    pub fn empty() -> Self {
        Self {
            uuid: SnapshotId::new(),
            last_transaction: None,
            topics: Vec::new(),
        }
    }

    pub fn commit_many<I>(&mut self, iter: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = Transaction>,
    {
        for t in iter {
            self.commit(t)?;
        }
        self.uuid = SnapshotId::new();
        Ok(())
    }

    pub fn commit_one(&mut self, t: Transaction) -> crate::Result<()> {
        self.commit(t)?;
        self.uuid = SnapshotId::new();
        Ok(())
    }

    fn commit(&mut self, t: Transaction) -> crate::Result<()> {
        use Transaction::*;
        if Some(t.uuid()) < self.last_transaction {
            tracing::warn!(transaction=%t.uuid(), "skipping outdated transaction");
            return Ok(());
        }

        tracing::debug!(uuid=%t.uuid(), kind=t.kind(), "committing transaction");
        self.last_transaction = Some(t.uuid());
        match t {
            CreateTopic(t) => self.create_topic(t),
            CreateShard(t) => self.create_shard(t),
            CloseShard(t) => self.close_shard(t),
            DeleteShard(t) => self.delete_shard(t),
        }
    }

    fn create_topic(&mut self, tsn: CreateTopic) -> crate::Result<()> {
        self.topics.push(tsn.into());
        Ok(())
    }

    fn create_shard(&mut self, tsn: CreateShard) -> crate::Result<()> {
        let topic = self.topic_mut(&tsn.topic)?;
        topic.shards.push(tsn.into());
        Ok(())
    }

    fn close_shard(&mut self, tsn: CloseShard) -> crate::Result<()> {
        self.topic_mut(&tsn.topic)?
            .shard_mut(&tsn.shard)?
            .close(tsn.rows);
        Ok(())
    }

    fn delete_shard(&mut self, tsn: DeleteShard) -> crate::Result<()> {
        let topic = self.topic_mut(&tsn.topic)?;
        topic.shards.retain(|s| s.id != tsn.shard);
        Ok(())
    }

    fn topic_mut(&mut self, id: &TopicId) -> crate::Result<&mut TopicState> {
        for topic in &mut self.topics {
            if &topic.id == id {
                return Ok(topic);
            }
        }
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopicState {
    pub id: TopicId,
    pub schema: Schema,
    pub path: Path,
    pub shards: Vec<ShardState>,
}

impl TopicState {
    fn shard_mut(&mut self, id: &ShardId) -> crate::Result<&mut ShardState> {
        match self.shards.binary_search_by_key(id, |s| s.id) {
            Ok(idx) => Ok(&mut self.shards[idx]),
            Err(_) => todo!(),
        }
    }
}

impl From<CreateTopic> for TopicState {
    fn from(t: CreateTopic) -> Self {
        Self::new(t.topic, t.schema, t.path)
    }
}

impl TopicState {
    fn new(id: TopicId, schema: Schema, path: Path) -> Self {
        Self {
            id,
            schema,
            path,
            shards: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardState {
    pub id: ShardId,
    pub schema: Schema,
    pub path: Path,
    pub rows: Option<usize>,
}

impl From<CreateShard> for ShardState {
    fn from(s: CreateShard) -> Self {
        Self::new(s.shard, s.schema, s.path)
    }
}

impl ShardState {
    fn new(id: ShardId, schema: Schema, path: Path) -> Self {
        Self {
            id,
            schema,
            path,
            rows: None,
        }
    }

    fn close(&mut self, rows: usize) {
        self.rows = Some(rows);
    }
}
