pub(crate) mod arrow;
mod column;
mod fmt;
mod frame;
pub mod mask;
mod ops;
pub mod shape;
pub mod slice;
mod tensor;
mod tensor_type;
mod tensor_value;

pub use column::{tensor_schema, Column, ColumnData};
pub use frame::{DataFrame, Frame};
pub use mask::Mask;
pub use shape::{Axis, Const, Dyn, IntoShape, RemoveAxis, Shape};
pub use tensor::{Tensor, Tensor1, Tensor2, Tensor3, Tensor4, TensorD};
pub use tensor_type::TensorType;
pub use tensor_value::TensorValue;

use ::arrow::datatypes::DataType;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported datatype {0}")]
    DataType(DataType),
    #[error("shape expects {expected} dimensions but received {actual} dimensions")]
    Dimensions { expected: usize, actual: usize },
    #[error("failed to convert shape {:?} to shape with {} dimensions", src.slice(), ndim)]
    Shape { src: Dyn, ndim: usize },
    #[error("shapes {0:?} and {1:?} cannot be broadcast together")]
    Broadcast(Dyn, Dyn),
    #[error("no column found for column name {0}")]
    ColumnLookup(String),
    #[error("failed to cast tensor of type {from:?} to type {to:?}")]
    Cast { to: TensorType, from: TensorType },
    #[error("unknown extension type {0}")]
    UnknownExtension(String),
    #[error("missing metadata for extension type {0}")]
    MissingMetadata(String),
    #[error("JSON serialization error")]
    Json(#[from] serde_json::Error),
}

impl Error {
    pub fn cast(to: TensorType, from: TensorType) -> Self {
        Self::Cast { to, from }
    }
    pub fn dimensions(expected: usize, actual: usize) -> Self {
        Self::Dimensions { expected, actual }
    }

    pub fn shape<S>(src: S, ndim: usize) -> Self
    where
        S: Into<Dyn>,
    {
        Self::Shape {
            src: src.into(),
            ndim,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
