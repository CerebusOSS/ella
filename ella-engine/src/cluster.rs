use std::sync::Arc;

use dashmap::DashMap;
use datafusion::{catalog::CatalogList, error::DataFusionError};

use crate::{
    catalog::EllaCatalog,
    engine::EllaState,
    registry::{
        snapshot::Snapshot,
        transactions::{CreateCatalog, DropCatalog},
        Id, TransactionLog,
    },
    Path,
};

#[derive(Debug)]
pub struct EllaCluster {
    catalogs: DashMap<Id<'static>, Arc<EllaCatalog>>,
    log: Arc<TransactionLog>,
    root: Path,
}

impl EllaCluster {
    pub fn new(log: Arc<TransactionLog>, root: Path) -> Self {
        Self {
            catalogs: DashMap::new(),
            log,
            root,
        }
    }

    pub fn catalogs(&self) -> Vec<Arc<EllaCatalog>> {
        self.catalogs.iter().map(|c| c.value().clone()).collect()
    }

    pub fn catalog<'a>(&self, id: impl Into<Id<'a>>) -> Option<Arc<EllaCatalog>> {
        let id: Id<'a> = id.into();
        self.catalogs.get(id.as_ref()).map(|c| c.value().clone())
    }

    pub async fn create_catalog<'a>(
        &self,
        id: impl Into<Id<'a>>,
        if_not_exists: bool,
    ) -> crate::Result<Arc<EllaCatalog>> {
        let id: Id<'static> = id.into().into_owned();
        match (if_not_exists, self.catalog(id.as_ref())) {
            (true, Some(catalog)) => Ok(catalog),
            (true, None) | (false, None) => {
                let catalog = Arc::new(EllaCatalog::new(
                    id.clone().into(),
                    self.log.clone(),
                    self.root.clone(),
                ));
                self.register(id, catalog.clone()).await?;
                Ok(catalog)
            }
            (false, Some(_)) => Err(crate::EngineError::CatalogExists(id.to_string()).into()),
        }
    }

    pub(crate) async fn register(
        &self,
        id: Id<'static>,
        catalog: Arc<EllaCatalog>,
    ) -> crate::Result<Option<Arc<EllaCatalog>>> {
        self.log
            .commit(CreateCatalog::new(id.clone().into(), &self.root))
            .await?;
        Ok(self.catalogs.insert(id, catalog))
    }

    pub async fn deregister<'a>(&self, id: impl Into<Id<'a>>, cascade: bool) -> crate::Result<()> {
        let id: Id<'a> = id.into();
        if let Some(catalog) = self.catalog(id.as_ref()) {
            match (cascade, catalog.is_empty()) {
                (true, _) | (false, true) => {
                    let (_, catalog) = self
                        .catalogs
                        .remove(id.as_ref())
                        .ok_or_else(|| crate::EngineError::CatalogNotFound(id.to_string()))?;
                    catalog.drop_schemas().await?;
                    self.log
                        .commit(DropCatalog::new(id.into_owned().into()))
                        .await?;
                    Ok(())
                }
                (false, false) => Err(DataFusionError::Execution(format!(
                    "cannot remove non-empty catalog {}",
                    id,
                ))
                .into()),
            }
        } else {
            Err(crate::EngineError::CatalogNotFound(id.to_string()).into())
        }
    }

    pub(crate) async fn close(&self) -> crate::Result<()> {
        let results = futures::future::join_all(
            self.catalogs()
                .into_iter()
                .map(|c| async move { c.close().await }),
        )
        .await;
        results
            .into_iter()
            .find(|res| res.is_err())
            .unwrap_or_else(|| Ok(()))
    }

    pub(crate) fn load(&self, snapshot: &Snapshot, state: &EllaState) -> crate::Result<()> {
        for catalog in &snapshot.catalogs {
            self.catalogs.insert(
                catalog.id.clone().into(),
                Arc::new(EllaCatalog::load(catalog, state)?),
            );
        }
        self.resolve(state)?;
        Ok(())
    }

    pub(crate) fn resolve(&self, state: &EllaState) -> crate::Result<()> {
        for catalog in &self.catalogs {
            catalog.resolve(state)?;
        }
        Ok(())
    }
}

impl CatalogList for EllaCluster {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: std::sync::Arc<dyn datafusion::catalog::CatalogProvider>,
    ) -> Option<std::sync::Arc<dyn datafusion::catalog::CatalogProvider>> {
        unimplemented!()
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().to_string()).collect()
    }

    fn catalog(
        &self,
        name: &str,
    ) -> Option<std::sync::Arc<dyn datafusion::catalog::CatalogProvider>> {
        self.catalog(name).map(|c| c as Arc<_>)
    }
}
