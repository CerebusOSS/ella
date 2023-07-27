use arrow_schema::SchemaRef;

use crate::{
    table::{
        info::{TableInfo, TopicInfo, ViewInfo},
        topic::ShardInfo,
    },
    Path,
};

use super::id::*;

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
pub struct CreateCatalog {
    pub uuid: TransactionId,
    pub id: CatalogId<'static>,
    pub path: Path,
}

impl CreateCatalog {
    pub fn new(id: CatalogId<'static>, root: &Path) -> Self {
        let path = root.join(id.as_ref());
        Self {
            uuid: TransactionId::new(),
            id,
            path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateSchema {
    pub uuid: TransactionId,
    pub id: SchemaId<'static>,
    pub path: Path,
}

impl CreateSchema {
    pub fn new(id: SchemaId<'static>, root: &Path) -> Self {
        let path = root.join(id.catalog.as_ref()).join(id.schema.as_ref());
        Self {
            uuid: TransactionId::new(),
            id,
            path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateTable {
    pub uuid: TransactionId,
    pub id: TableId<'static>,
    pub info: TableInfo,
}

impl CreateTable {
    pub fn topic(id: TableId<'static>, info: TopicInfo) -> Self {
        Self {
            uuid: TransactionId::new(),
            id,
            info: info.into(),
        }
    }

    pub fn view(id: TableId<'static>, info: ViewInfo) -> Self {
        Self {
            uuid: TransactionId::new(),
            id,
            info: info.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TableKind {
    Topic(TopicInfo),
    View(ViewInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateShard {
    pub uuid: TransactionId,
    pub table: TableId<'static>,
    pub shard: ShardId,
    pub file_schema: SchemaRef,
    pub path: Path,
}

impl CreateShard {
    pub fn new(table: TableId<'static>, file_schema: SchemaRef, root: &Path) -> Self {
        let shard = ShardId::new();
        let path = shard.encode_path(root, "parquet");
        Self {
            uuid: TransactionId::new(),
            table,
            shard,
            file_schema,
            path,
        }
    }
}

impl From<CreateShard> for ShardInfo {
    fn from(tsn: CreateShard) -> Self {
        ShardInfo::new(tsn.shard, tsn.table, tsn.file_schema, tsn.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloseShard {
    pub uuid: TransactionId,
    pub table: TableId<'static>,
    pub shard: ShardId,
    pub rows: usize,
}

impl CloseShard {
    pub fn new(table: TableId<'static>, shard: ShardId, rows: usize) -> Self {
        Self {
            uuid: TransactionId::new(),
            table,
            shard,
            rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeleteShard {
    pub uuid: TransactionId,
    pub table: TableId<'static>,
    pub shard: ShardId,
}

impl DeleteShard {
    pub fn new(table: TableId<'static>, shard: ShardId) -> Self {
        Self {
            uuid: TransactionId::new(),
            table,
            shard,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactShards {
    pub uuid: TransactionId,
    pub table: TableId<'static>,
    pub src: Vec<ShardId>,
    pub dst: ShardId,
    pub file_schema: SchemaRef,
    pub path: Path,
}

impl CompactShards {
    pub fn new(
        table: TableId<'static>,
        src: Vec<ShardId>,
        file_schema: SchemaRef,
        root: &Path,
    ) -> Self {
        let dst = ShardId::generate_from(src.first().expect("cannot compact empty shard list"));
        let path = dst.encode_path(root, "parquet");
        Self {
            uuid: TransactionId::new(),
            table,
            src,
            dst,
            file_schema,
            path,
        }
    }
}

impl From<CompactShards> for ShardInfo {
    fn from(tsn: CompactShards) -> Self {
        ShardInfo::new(tsn.dst, tsn.table, tsn.file_schema, tsn.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropTable {
    pub uuid: TransactionId,
    pub id: TableId<'static>,
}

impl DropTable {
    pub fn new(id: TableId<'static>) -> Self {
        Self {
            uuid: TransactionId::new(),
            id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropSchema {
    pub uuid: TransactionId,
    pub id: SchemaId<'static>,
}

impl DropSchema {
    pub fn new(id: SchemaId<'static>) -> Self {
        Self {
            uuid: TransactionId::new(),
            id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DropCatalog {
    pub uuid: TransactionId,
    pub id: CatalogId<'static>,
}

impl DropCatalog {
    pub fn new(id: CatalogId<'static>) -> Self {
        Self {
            uuid: TransactionId::new(),
            id,
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
    CreateCatalog(CreateCatalog),
    CreateSchema(CreateSchema),
    CreateTable(CreateTable),
    CreateShard(CreateShard),
    CloseShard(CloseShard),
    DeleteShard(DeleteShard),
    CompactShards(CompactShards),
    DropTable(DropTable),
    DropSchema(DropSchema),
    DropCatalog(DropCatalog),
}

impl Transaction {
    pub fn uuid(&self) -> TransactionId {
        use Transaction::*;
        match self {
            CreateCatalog(t) => t.uuid,
            CreateSchema(t) => t.uuid,
            CreateTable(t) => t.uuid,
            CreateShard(t) => t.uuid,
            CloseShard(t) => t.uuid,
            DeleteShard(t) => t.uuid,
            CompactShards(t) => t.uuid,
            DropTable(t) => t.uuid,
            DropSchema(t) => t.uuid,
            DropCatalog(t) => t.uuid,
        }
    }

    pub fn kind(&self) -> String {
        self.to_string()
    }
}
