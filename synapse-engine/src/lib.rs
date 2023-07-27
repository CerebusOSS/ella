mod catalog;
mod cluster;
pub mod codec;

pub mod config;
pub mod engine;
pub mod lazy;
pub(crate) mod metrics;
mod path;
mod plan;
pub mod registry;
pub mod schema;
pub mod table;
pub(crate) mod util;

pub use config::SynapseConfig;
pub use engine::SynapseContext;
pub use path::Path;
pub use plan::Plan;
pub use schema::ArrowSchema;
pub use synapse_common::{error::EngineError, Error, Result};
pub use table::TableConfig;

pub async fn open(root: &str) -> crate::Result<SynapseContext> {
    let state = engine::SynapseState::open(root).await?;
    SynapseContext::new(state)
}

pub async fn create(
    root: &str,
    config: SynapseConfig,
    if_not_exists: bool,
) -> crate::Result<SynapseContext> {
    let state = engine::SynapseState::create(root, config, if_not_exists).await?;
    SynapseContext::new(state)
}
