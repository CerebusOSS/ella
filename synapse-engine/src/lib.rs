pub mod catalog;
mod context;
mod engine;
pub mod lazy;
pub(crate) mod metrics;
mod path;
pub mod schema;
pub mod topic;
pub(crate) mod util;

pub use context::SynapseContext;
pub use engine::{Engine, EngineConfig};
pub use path::Path;
pub use schema::{ArrowSchema, Schema};
pub use synapse_common::{error::EngineError, Error, Result};
pub use topic::{Topic, TopicConfig};
