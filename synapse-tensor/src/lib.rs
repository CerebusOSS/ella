pub(crate) mod arrow;
mod column;
mod fmt;
mod frame;
pub mod mask;
mod ops;
pub mod row;
pub mod shape;
pub mod slice;
mod tensor;

pub use column::{tensor_schema, Column, ColumnData};
pub use frame::{DataFrame, Frame};
pub use mask::Mask;
pub use shape::{Axis, Const, Dyn, IntoShape, RemoveAxis, Shape};
pub use slice::{NewAxis, Slice};
pub use tensor::{Tensor, Tensor1, Tensor2, Tensor3, Tensor4, TensorD};

pub use ::synapse_common::{
    error::ShapeError, Error, MaskedValue, Result, TensorType, TensorValue,
};

#[cfg(test)]
#[macro_export]
macro_rules! assert_tensor_eq {
    ($a:expr, $b:expr) => {
        match ($a, $b) {
            (a, b) => assert!(a.eq(&b).all(), "{:?} != {:?}", a, b),
        }
    };
}
