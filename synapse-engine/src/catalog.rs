use std::sync::Arc;

use dashmap::DashMap;
use datafusion::{catalog::CatalogProvider, error::DataFusionError};

use crate::{
    engine::SynapseState,
    registry::{
        snapshot::CatalogState,
        transactions::{CreateSchema, DropSchema},
        CatalogId, Id, TransactionLog,
    },
    schema::SynapseSchema,
    Path,
};

#[derive(Debug)]
pub struct SynapseCatalog {
    id: CatalogId<'static>,
    schemas: DashMap<Id<'static>, Arc<SynapseSchema>>,
    log: Arc<TransactionLog>,
    root: Path,
}

impl SynapseCatalog {
    pub(crate) fn new(id: CatalogId<'static>, log: Arc<TransactionLog>, root: Path) -> Self {
        Self {
            id,
            schemas: DashMap::new(),
            log,
            root,
        }
    }

    pub fn id(&self) -> &CatalogId<'static> {
        &self.id
    }

    pub fn schemas(&self) -> Vec<Arc<SynapseSchema>> {
        self.schemas.iter().map(|s| s.value().clone()).collect()
    }

    pub fn schema<'a>(&self, id: impl Into<Id<'a>>) -> Option<Arc<SynapseSchema>> {
        let id: Id<'a> = id.into();
        self.schemas.get(id.as_ref()).map(|s| s.value().clone())
    }

    pub async fn create_schema<'a>(
        &self,
        id: impl Into<Id<'a>>,
        if_not_exists: bool,
    ) -> crate::Result<Arc<SynapseSchema>> {
        let id: Id<'static> = id.into().into_owned();
        match (if_not_exists, self.schema(id.as_ref())) {
            (true, Some(schema)) => Ok(schema),
            (true, None) | (false, None) => {
                let schema = Arc::new(SynapseSchema::new(
                    (self.id.clone(), id.clone()).into(),
                    self.log.clone(),
                ));
                self.register(id, schema.clone()).await?;
                Ok(schema)
            }
            (false, Some(_)) => Err(crate::EngineError::SchemaExists(id.to_string()).into()),
        }
    }

    pub(crate) async fn register(
        &self,
        id: Id<'static>,
        schema: Arc<SynapseSchema>,
    ) -> crate::Result<Option<Arc<SynapseSchema>>> {
        self.log
            .commit(CreateSchema::new(
                (self.id.clone(), id.clone()).into(),
                &self.root,
            ))
            .await?;
        Ok(self.schemas.insert(id, schema))
    }

    pub async fn deregister<'a>(
        &self,
        id: impl Into<Id<'a>>,
        if_exists: bool,
        cascade: bool,
    ) -> crate::Result<()> {
        let id: Id<'a> = id.into();
        match (if_exists, self.schema(id.as_ref())) {
            (_, Some(schema)) => match (cascade, schema.is_empty()) {
                (true, _) | (false, true) => {
                    let (_, schema) = self
                        .schemas
                        .remove(id.as_ref())
                        .ok_or_else(|| crate::EngineError::SchemaNotFound(id.to_string()))?;
                    schema.drop_tables().await?;
                    self.log
                        .commit(DropSchema::new(self.id.schema(id.into_owned())))
                        .await?;
                    Ok(())
                }
                (false, false) => Err(DataFusionError::Execution(format!(
                    "cannot remove non-empty schema {}",
                    id,
                ))
                .into()),
            },
            (true, None) => Ok(()),
            (false, None) => Err(crate::EngineError::SchemaNotFound(id.to_string()).into()),
        }
    }

    pub(crate) async fn close(&self) -> crate::Result<()> {
        let results = futures::future::join_all(
            self.schemas()
                .into_iter()
                .map(|c| async move { c.close().await }),
        )
        .await;
        results
            .into_iter()
            .find(|res| res.is_err())
            .unwrap_or_else(|| Ok(()))
    }

    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    pub(crate) fn load(catalog: &CatalogState, state: &SynapseState) -> crate::Result<Self> {
        let schemas = DashMap::new();

        for schema in &catalog.schemas {
            schemas.insert(
                schema.id.schema.clone(),
                Arc::new(SynapseSchema::load(schema, state)?),
            );
        }
        Ok(Self {
            id: catalog.id.clone(),
            schemas,
            log: state.log().clone(),
            root: state.root().clone(),
        })
    }

    pub(crate) fn resolve(&self, state: &SynapseState) -> crate::Result<()> {
        for schema in &self.schemas {
            schema.resolve(state)?;
        }
        Ok(())
    }

    pub(crate) async fn drop_schemas(&self) -> crate::Result<()> {
        for schema in self.schemas.iter() {
            self.deregister(&schema.id().schema, true, true).await?;
        }
        Ok(())
    }
}

impl CatalogProvider for SynapseCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .iter()
            .map(|s| s.key().to_string())
            .collect::<Vec<_>>()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::schema::SchemaProvider>> {
        self.schema(name).map(|s| s as Arc<_>)
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn datafusion::catalog::schema::SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::catalog::schema::SchemaProvider>>>
    {
        unimplemented!()
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::catalog::schema::SchemaProvider>>>
    {
        unimplemented!()
    }
}
