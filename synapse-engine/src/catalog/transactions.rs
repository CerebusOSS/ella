use crate::Path;

use super::id::*;
use crate::Schema;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionId {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionValue {
    Int(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateTopic {
    pub uuid: TransactionId,
    pub topic: TopicId,
    pub path: Path,
    pub schema: Schema,
}

impl CreateTopic {
    pub fn new(topic: TopicId, schema: Schema, root: &Path) -> Self {
        let path = root.join(topic.as_ref());
        Self {
            uuid: TransactionId::new(),
            topic,
            path,
            schema,
        }
    }
}

// #[derive(Debug)]
// pub struct CreatePartition {
//     pub topic: TopicId,
//     pub path: Path,
// }

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateShard {
    pub uuid: TransactionId,
    pub topic: TopicId,
    // pub partition: PartitionId,
    pub shard: ShardId,
    pub schema: Schema,
    pub path: Path,
}

impl CreateShard {
    pub fn new(topic: TopicId, schema: Schema, root: &Path) -> Self {
        let shard = ShardId::new();
        let path = shard.encode_path(root, "parquet");
        Self {
            uuid: TransactionId::new(),
            topic,
            shard,
            schema,
            path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloseShard {
    pub uuid: TransactionId,
    pub topic: TopicId,
    pub shard: ShardId,
    pub rows: usize,
}

impl CloseShard {
    pub fn new(topic: TopicId, shard: ShardId, rows: usize) -> Self {
        Self {
            uuid: TransactionId::new(),
            topic,
            shard,
            rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeleteShard {
    pub uuid: TransactionId,
    pub topic: TopicId,
    pub shard: ShardId,
}

impl DeleteShard {
    pub fn new(topic: TopicId, shard: ShardId) -> Self {
        Self {
            uuid: TransactionId::new(),
            topic,
            shard,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactShards {
    pub uuid: TransactionId,
    pub topic: TopicId,
    pub src: Vec<ShardId>,
    pub dst: ShardId,
    pub schema: Schema,
    pub path: Path,
}

impl CompactShards {
    pub fn new(topic: TopicId, src: Vec<ShardId>, schema: Schema, root: &Path) -> Self {
        let dst = ShardId::generate_from(src.first().expect("cannot compact empty shard list"));
        let path = dst.encode_path(root, "parquet");
        Self {
            uuid: TransactionId::new(),
            topic,
            src,
            dst,
            schema,
            path,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    strum::Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum Transaction {
    CreateTopic(CreateTopic),
    // CreatePartition(CreatePartition),
    CreateShard(CreateShard),
    CloseShard(CloseShard),
    DeleteShard(DeleteShard),
    CompactShards(CompactShards),
}

impl Transaction {
    pub fn uuid(&self) -> TransactionId {
        use Transaction::*;
        match self {
            CreateTopic(t) => t.uuid,
            CreateShard(t) => t.uuid,
            CloseShard(t) => t.uuid,
            DeleteShard(t) => t.uuid,
            CompactShards(t) => t.uuid,
        }
    }

    pub fn kind(&self) -> String {
        self.to_string()
    }
}
