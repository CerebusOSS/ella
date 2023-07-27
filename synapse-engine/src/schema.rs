use std::{any::Any, sync::Arc};

pub use arrow_schema::Schema as ArrowSchema;
use dashmap::DashMap;
use datafusion::{
    catalog::schema::SchemaProvider, datasource::TableProvider, error::DataFusionError,
};

use crate::{
    engine::SynapseState,
    registry::{snapshot::SchemaState, transactions::DropTable, Id, SchemaId, TransactionLog},
    table::SynapseTable,
};

#[derive(Debug)]
pub struct SynapseSchema {
    id: SchemaId<'static>,
    tables: DashMap<Id<'static>, Arc<SynapseTable>>,
    log: Arc<TransactionLog>,
}

impl SynapseSchema {
    pub(crate) fn new(id: SchemaId<'static>, log: Arc<TransactionLog>) -> Self {
        Self {
            id,
            tables: DashMap::new(),
            log,
        }
    }

    pub fn id(&self) -> &SchemaId<'static> {
        &self.id
    }

    pub fn tables(&self) -> Vec<Arc<SynapseTable>> {
        self.tables.iter().map(|t| t.value().clone()).collect()
    }

    pub fn table<'a>(&self, id: impl Into<Id<'a>>) -> Option<Arc<SynapseTable>> {
        let id: Id<'a> = id.into();
        self.tables.get(id.as_ref()).map(|t| t.value().clone())
    }

    pub async fn register<'a>(
        &self,
        id: impl Into<Id<'a>>,
        table: Arc<SynapseTable>,
    ) -> crate::Result<()> {
        let id: Id<'static> = id.into().into_owned();
        if self.tables.contains_key(&id) {
            return Err(crate::EngineError::TableExists(self.id.table(id).to_string()).into());
        }
        self.log.commit(table.transaction()).await?;
        self.tables.insert(id, table);
        Ok(())
    }

    async fn deregister<'a, F>(
        &self,
        id: impl Into<Id<'a>>,
        if_exists: bool,
        f: F,
    ) -> crate::Result<()>
    where
        F: FnOnce(&Arc<SynapseTable>) -> bool,
    {
        let id: Id<'a> = id.into();
        let table = self.tables.remove_if(id.as_ref(), |_k, v| f(v));
        match (if_exists, table) {
            (_, Some((_, table))) => {
                table.drop_shards().await?;
                self.log
                    .commit(DropTable::new(self.id.table(id.into_owned())))
                    .await?;
                Ok(())
            }
            (true, None) => Ok(()),
            (false, None) => {
                Err(crate::EngineError::TableNotFound(self.id.table(id).to_string()).into())
            }
        }
    }

    pub async fn drop_table<'a>(
        &self,
        id: impl Into<Id<'a>>,
        if_exists: bool,
    ) -> crate::Result<()> {
        self.deregister(id, if_exists, |_| true).await
    }

    pub async fn drop_topic<'a>(
        &self,
        id: impl Into<Id<'a>>,
        if_exists: bool,
    ) -> crate::Result<()> {
        self.deregister(id, if_exists, |table: &Arc<SynapseTable>| {
            table.as_topic().is_some()
        })
        .await
    }

    pub async fn drop_view<'a>(&self, id: impl Into<Id<'a>>, if_exists: bool) -> crate::Result<()> {
        self.deregister(id, if_exists, |table: &Arc<SynapseTable>| {
            table.as_view().is_some()
        })
        .await
    }

    pub(crate) async fn close(&self) -> crate::Result<()> {
        let results = futures::future::join_all(
            self.tables()
                .into_iter()
                .map(|t| async move { t.close().await }),
        )
        .await;
        results
            .into_iter()
            .find(|res| res.is_err())
            .unwrap_or_else(|| Ok(()))
    }

    pub(crate) async fn drop_tables(&self) -> crate::Result<()> {
        // This collect is necessary to avoid a lifetime issue.
        let tables = self
            .tables
            .iter()
            .map(|t| t.id().table.clone())
            .collect::<Vec<_>>();

        for table in tables {
            self.deregister(table, true, |_| true).await?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub(crate) fn load(schema: &SchemaState, state: &SynapseState) -> crate::Result<Self> {
        let tables = DashMap::new();

        for table in &schema.tables {
            tables.insert(
                table.id.table.clone(),
                Arc::new(SynapseTable::load(table, state)?),
            );
        }

        Ok(Self {
            id: schema.id.clone(),
            tables,
            log: state.log().clone(),
        })
    }

    pub(crate) fn resolve(&self, state: &SynapseState) -> crate::Result<()> {
        for table in &self.tables {
            table.resolve(state)?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl SchemaProvider for SynapseSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|t| t.key().to_string())
            .collect::<Vec<_>>()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.table(name).map(|t| t as Arc<_>)
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        unimplemented!()
    }

    fn deregister_table(
        &self,
        _name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        unimplemented!()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
