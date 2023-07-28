use super::id::*;
use super::transactions::*;
use crate::Path;

use crate::config::EllaConfig;
use crate::table::info::TableInfo;
use crate::table::info::TopicInfo;
use crate::table::info::ViewInfo;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub uuid: SnapshotId,
    pub last_transaction: Option<TransactionId>,
    pub catalogs: Vec<CatalogState>,
    pub config: EllaConfig,
}

impl Snapshot {
    pub fn empty(config: EllaConfig) -> Self {
        Self {
            uuid: SnapshotId::new(),
            last_transaction: None,
            catalogs: Vec::new(),
            config,
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
            CreateCatalog(t) => self.create_catalog(t),
            CreateSchema(t) => self.create_schema(t),
            CreateTable(t) => self.create_table(t),
            CreateShard(t) => self.create_shard(t),
            CloseShard(t) => self.close_shard(t),
            DeleteShard(t) => self.delete_shard(t),
            CompactShards(t) => self.compact_shards(t),
            DropTable(t) => self.drop_table(t),
            DropSchema(t) => self.drop_schema(t),
            DropCatalog(t) => self.drop_catalog(t),
        }
    }

    fn create_catalog(&mut self, tsn: CreateCatalog) -> crate::Result<()> {
        self.catalogs.push(tsn.into());
        Ok(())
    }

    fn create_schema(&mut self, tsn: CreateSchema) -> crate::Result<()> {
        self.catalog_mut(&tsn.id.catalog)?.schemas.push(tsn.into());
        Ok(())
    }

    fn create_table(&mut self, tsn: CreateTable) -> crate::Result<()> {
        self.schema_mut(&tsn.id.clone().into())?
            .tables
            .push(tsn.into());
        Ok(())
    }

    fn create_shard(&mut self, tsn: CreateShard) -> crate::Result<()> {
        self.table_mut(&tsn.table)?
            .topic_mut()?
            .insert_shard(tsn.into())?;
        Ok(())
    }

    fn close_shard(&mut self, tsn: CloseShard) -> crate::Result<()> {
        self.table_mut(&tsn.table)?
            .topic_mut()?
            .shard_mut(&tsn.shard)?
            .close(tsn.rows);
        Ok(())
    }

    fn delete_shard(&mut self, tsn: DeleteShard) -> crate::Result<()> {
        let topic = self.table_mut(&tsn.table)?.topic_mut()?;
        topic.shards_mut().retain(|s| s.id != tsn.shard);
        Ok(())
    }

    fn compact_shards(&mut self, tsn: CompactShards) -> crate::Result<()> {
        let topic = self.table_mut(&tsn.table)?.topic_mut()?;
        topic.insert_shard(tsn.into())?;
        Ok(())
    }

    fn drop_table(&mut self, tsn: DropTable) -> crate::Result<()> {
        self.catalog_mut(&tsn.id.catalog)?
            .schema_mut(&tsn.id.schema)?
            .tables
            .retain(|t| t.id != tsn.id);
        Ok(())
    }

    fn drop_schema(&mut self, tsn: DropSchema) -> crate::Result<()> {
        self.catalog_mut(&tsn.id.catalog)?
            .schemas
            .retain(|s| s.id != tsn.id);

        Ok(())
    }

    fn drop_catalog(&mut self, tsn: DropCatalog) -> crate::Result<()> {
        self.catalogs.retain(|c| c.id == tsn.id);

        Ok(())
    }

    fn catalog_mut(&mut self, id: &Id) -> crate::Result<&mut CatalogState> {
        self.catalogs
            .iter_mut()
            .find(|c| &c.id.0 == id)
            .ok_or_else(|| crate::EngineError::CatalogNotFound(id.to_string()).into())
    }

    fn schema_mut(&mut self, id: &SchemaId) -> crate::Result<&mut SchemaState> {
        self.catalog_mut(&id.catalog)?.schema_mut(&id.schema)
    }

    fn table_mut(&mut self, id: &TableId) -> crate::Result<&mut TableState> {
        self.catalog_mut(&id.catalog)?
            .schema_mut(&id.schema)?
            .table_mut(&id.table)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CatalogState {
    pub id: CatalogId<'static>,
    pub path: Path,
    pub schemas: Vec<SchemaState>,
}

impl CatalogState {
    pub fn new(id: CatalogId<'static>, path: Path) -> Self {
        Self {
            id,
            path,
            schemas: Vec::new(),
        }
    }

    pub fn schema_mut(&mut self, id: &Id) -> crate::Result<&mut SchemaState> {
        self.schemas
            .iter_mut()
            .find(|s| &s.id.schema == id)
            .ok_or_else(|| crate::EngineError::SchemaNotFound(id.to_string()).into())
    }
}

impl From<CreateCatalog> for CatalogState {
    fn from(value: CreateCatalog) -> Self {
        Self::new(value.id, value.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaState {
    pub id: SchemaId<'static>,
    pub path: Path,
    pub tables: Vec<TableState>,
}

impl SchemaState {
    pub fn table_mut(&mut self, id: &Id) -> crate::Result<&mut TableState> {
        self.tables
            .iter_mut()
            .find(|t| &t.id.table == id)
            .ok_or_else(|| crate::EngineError::TableNotFound(id.to_string()).into())
    }
}

impl From<CreateSchema> for SchemaState {
    fn from(value: CreateSchema) -> Self {
        Self {
            id: value.id,
            path: value.path,
            tables: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableState {
    pub id: TableId<'static>,
    pub info: TableInfo,
}

impl TableState {
    pub fn topic(&self) -> crate::Result<&TopicInfo> {
        match &self.info {
            TableInfo::Topic(t) => Ok(t),
            TableInfo::View(_) => Err(crate::EngineError::table_kind("topic", "view").into()),
        }
    }

    pub fn topic_mut(&mut self) -> crate::Result<&mut TopicInfo> {
        match &mut self.info {
            TableInfo::Topic(t) => Ok(t),
            TableInfo::View(_) => Err(crate::EngineError::table_kind("topic", "view").into()),
        }
    }

    pub fn view(&self) -> crate::Result<&ViewInfo> {
        match &self.info {
            TableInfo::Topic(_) => Err(crate::EngineError::table_kind("view", "topic").into()),
            TableInfo::View(v) => Ok(v),
        }
    }
}

impl From<CreateTable> for TableState {
    fn from(value: CreateTable) -> Self {
        Self {
            id: value.id.clone(),
            info: value.info,
        }
    }
}
