use std::{any::Any, sync::Arc};

use datafusion::arrow::datatypes::{DataType, Field};

use crate::TensorType;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum ShapeError {
    #[error("expected {expected} dimensions but shape has {actual} dimensions")]
    Ndim { expected: usize, actual: usize },
    #[error("all tensors must have common shape {0:?}")]
    Incompatible(Vec<usize>),
    #[error("shapes {0:?} and {1:?} cannot be broadcast together")]
    Broadcast(Vec<usize>, Vec<usize>),
}

impl ShapeError {
    pub fn ndim(expected: usize, actual: usize) -> Self {
        Self::Ndim { expected, actual }
    }

    pub fn incompatible(shape: &[usize]) -> Self {
        Self::Incompatible(shape.to_vec())
    }

    pub fn broadcast(lhs: &[usize], rhs: &[usize]) -> Self {
        Self::Broadcast(lhs.to_vec(), rhs.to_vec())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
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

#[cfg(feature = "pyo3")]
#[derive(Debug, thiserror::Error)]
pub enum PySynapseError {
    #[error("no topic with id '{0}' (to create the topic pass a schema)")]
    TopicNotFound(String),
    #[error("expected one of 'ascending' or 'descending' for index, got '{0}'")]
    InvalidIndexMode(String),
}

impl EngineError {
    pub fn worker_panic(id: &str, error: &Box<dyn Any + Send + 'static>) -> Self {
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("engine error: {0:?}")]
    Engine(#[from] EngineError),
    #[error("unsupported arrow datatype {0}")]
    DataType(DataType),
    #[error("shape error: {0:?}")]
    Shape(#[from] ShapeError),
    #[error("axis {0:?} out of bounds for shape with {1} dimensions")]
    AxisOutOfBounds(isize, usize),
    #[error("empty list passed to operation that requires at least one tensor")]
    EmptyList,
    #[error("no column found for column name {0}")]
    ColumnLookup(String),
    #[error("failed to cast tensor of type {from:?} to type {to:?}")]
    Cast { to: TensorType, from: TensorType },
    #[error("unknown extension type {0}")]
    UnknownExtension(String),
    #[error("missing metadata for extension type {0}")]
    MissingMetadata(String),
    #[error("serialization error")]
    Serialization(BoxError),
    #[error("row builder expected {0} columns but found {1} fields in schema")]
    FieldCount(usize, usize),
    #[error("row builder incompatible with field {0:?}")]
    IncompatibleRow(Arc<Field>),
    #[error("datafusion error")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("arrow error")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error("parquet error")]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),
    #[error("object store error")]
    ObjectStore(#[from] object_store::Error),
    #[error("invalid url")]
    Url(#[from] url::ParseError),
    #[cfg(feature = "pyo3")]
    #[error(transparent)]
    PySynapse(#[from] PySynapseError),
}

impl Error {
    pub fn cast(to: TensorType, from: TensorType) -> Self {
        Self::Cast { to, from }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(Box::new(value))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
