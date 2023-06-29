use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
    strum::EnumString,
    strum::FromRepr,
)]
#[repr(u8)]
#[strum(serialize_all = "snake_case")]
pub enum TensorType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Timestamp,
    Duration,
    String,
}

impl TensorType {
    pub fn to_arrow(&self) -> DataType {
        use TensorType::*;

        match self {
            Bool => DataType::Boolean,
            Int8 => DataType::Int8,
            Int16 => DataType::Int16,
            Int32 => DataType::Int32,
            Int64 => DataType::Int64,
            UInt8 => DataType::UInt8,
            UInt16 => DataType::UInt16,
            UInt32 => DataType::UInt32,
            UInt64 => DataType::UInt64,
            Float32 => DataType::Float32,
            Float64 => DataType::Float64,
            Duration => DataType::Duration(TimeUnit::Nanosecond),
            Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00"))),
            String => DataType::Utf8,
        }
    }

    pub fn from_arrow(dtype: &DataType) -> crate::Result<Self> {
        use DataType::*;

        Ok(match dtype {
            Boolean => Self::Bool,
            Int8 => Self::Int8,
            Int16 => Self::Int16,
            Int32 => Self::Int32,
            Int64 => Self::Int64,
            UInt8 => Self::UInt8,
            UInt16 => Self::UInt16,
            UInt32 => Self::UInt32,
            UInt64 => Self::UInt64,
            Float32 => Self::Float32,
            Float64 => Self::Float64,
            Duration(TimeUnit::Nanosecond) => Self::Duration,
            Timestamp(TimeUnit::Nanosecond, _) => Self::Timestamp,
            Utf8 => Self::String,
            _ => return Err(crate::Error::DataType(dtype.clone())),
        })
    }
}
