pub mod catalog;
mod context;
mod path;
mod runtime;
pub mod schema;
pub mod topic;

pub use context::SynapseContext;
pub use path::Path;
pub use runtime::{Runtime, RuntimeConfig};
pub use schema::{ArrowSchema, Schema};
pub use topic::{Topic, TopicConfig};

use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("datafusion error")]
    DataFusion(#[from] DataFusionError),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("arrow error")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error("parquet error")]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),
    #[error("failed to parse time string")]
    TimeParse(#[from] time::error::Parse),
    #[error("invalid url")]
    Url(#[from] url::ParseError),
    #[error("object store error")]
    ObjectStore(#[from] object_store::Error),
    #[error("JSON serialization error")]
    Json(#[from] serde_json::Error),
    #[error("tensor error")]
    Tensor(#[from] synapse_tensor::Error),
    #[error("expected file but {0} is a directory")]
    UnexpectedDirectory(String),
    #[error("invalid synapse filename {0}")]
    InvalidFilename(String),
    #[error("UUID error")]
    Uuid(#[from] uuid::Error),
    // #[error("failed to parse UUID from {0}")]
    // InvalidUuid(String),
}

pub type Result<T> = std::result::Result<T, Error>;
