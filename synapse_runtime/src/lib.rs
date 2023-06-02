// mod bounded_stream;
pub mod catalog;
mod context;
mod path;
mod runtime;
pub mod schema;
pub mod topic;

use catalog::TopicId;
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
    #[error("topic {0} unavailable")]
    TopicUnavailable(TopicId),
    #[error("publisher queue full for topic {0}")]
    TopicQueueFull(TopicId),
}

pub type Result<T> = std::result::Result<T, Error>;
