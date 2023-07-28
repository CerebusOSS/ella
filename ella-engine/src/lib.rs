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

pub use config::EllaConfig;
pub use ella_common::{error::EngineError, Error, Result};
pub use engine::EllaContext;
pub use path::Path;
pub use plan::Plan;
pub use schema::ArrowSchema;
pub use table::TableConfig;

pub async fn open(root: &str) -> crate::Result<EllaContext> {
    let state = engine::EllaState::open(root).await?;
    EllaContext::new(state)
}

pub async fn create(
    root: &str,
    config: EllaConfig,
    if_not_exists: bool,
) -> crate::Result<EllaContext> {
    let state = engine::EllaState::create(root, config, if_not_exists).await?;
    EllaContext::new(state)
}
