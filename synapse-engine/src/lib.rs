// mod bounded_stream;
pub mod catalog;
mod context;
mod engine;
mod path;
#[cfg(feature = "pyo3")]
mod py;
pub mod schema;
pub mod topic;
pub(crate) mod util;

use std::{any::Any, fmt::Debug};

pub use context::SynapseContext;
pub use engine::{Engine, EngineConfig};
pub use path::Path;
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
    #[error("table closed")]
    TableClosed,
    #[error("table queue full")]
    TableQueueFull,
    #[error("worker {id} exited with error: {error:?}")]
    Worker { id: String, error: String },
}

impl Error {
    pub(crate) fn worker_panic(id: &str, error: &Box<dyn Any + Send + 'static>) -> Self {
        let error = if let Some(e) = error.downcast_ref::<String>() {
            e.clone()
        } else if let Some(e) = error.downcast_ref::<&'static str>() {
            e.to_string()
        } else {
            format!("{:?}", error)
        };
        Self::Worker {
            id: id.to_string(),
            error,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
