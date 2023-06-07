use std::{fmt::Debug, sync::Arc};

use datafusion::{execution::runtime_env::RuntimeEnv, prelude::SessionContext};
use object_store::ObjectStore;

use crate::{catalog::TransactionLog, EngineConfig, Path};

pub struct SynapseContext {
    root: Path,
    log: Arc<TransactionLog>,
    store: Arc<dyn ObjectStore + 'static>,
    session: SessionContext,
    config: EngineConfig,
}

impl Debug for SynapseContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SynapseContext")
            .field("root", &self.root)
            .field("log", &self.log)
            .finish_non_exhaustive()
    }
}

impl SynapseContext {
    const LOG: &'static str = ".synapse";

    pub fn new(
        root: Path,
        session: SessionContext,
        config: EngineConfig,
        env: &RuntimeEnv,
    ) -> crate::Result<Self> {
        let store = env.object_store(&root)?;
        let log = Arc::new(TransactionLog::new(root.join(Self::LOG), store.clone()));

        Ok(Self {
            root,
            log,
            store,
            session,
            config,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn log(&self) -> &Arc<TransactionLog> {
        &self.log
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore + 'static> {
        &self.store
    }

    pub fn session(&self) -> &SessionContext {
        &self.session
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}
