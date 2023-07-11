mod data;
mod fmt;
mod iter;

use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef, FixedSizeListArray};
pub use data::TensorData;
pub(crate) use iter::ShapedIter;
pub use iter::TensorIter;
use synapse_common::array::flatten;

use crate::{Column, Const, Dyn, Mask, Shape, TensorValue};

pub type Tensor1<T> = Tensor<T, Const<1>>;
pub type Tensor2<T> = Tensor<T, Const<2>>;
pub type Tensor3<T> = Tensor<T, Const<3>>;
pub type Tensor4<T> = Tensor<T, Const<4>>;
pub type TensorD<T> = Tensor<T, Dyn>;

#[derive(Clone)]
pub struct Tensor<T: TensorValue, S> {
    shape: S,
    strides: S,
    values: TensorData<T, T::Array>,
}

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    #[inline]
    pub fn shape(&self) -> &S {
        &self.shape
    }

    #[inline]
    pub fn strides(&self) -> &S {
        &self.strides
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.shape.size()
    }

    #[inline]
    pub fn ndim(&self) -> usize {
        self.shape.ndim()
    }

    pub fn is_standard_layout(&self) -> bool {
        self.shape.is_standard_layout(&self.strides)
    }

    #[doc(hidden)]
    #[inline]
    pub fn values(&self) -> &TensorData<T, T::Array> {
        &self.values
    }

    #[doc(hidden)]
    #[inline]
    pub fn into_values(self) -> TensorData<T, T::Array> {
        self.values
    }

    pub(crate) fn new<A>(values: A, shape: S, strides: S) -> Self
    where
        A: Into<TensorData<T, T::Array>>,
    {
        Self {
            values: values.into(),
            shape,
            strides,
        }
    }

    pub fn try_from_arrow<R>(array: ArrayRef, row_shape: R) -> crate::Result<Self>
    where
        R: Shape<Larger = S>,
    {
        let mut shape = row_shape.insert_axis(crate::Axis(0));
        shape[0] = array.len();
        let array = flatten(array)?;
        if shape.size() != array.len() {
            return Err(crate::ShapeError::ArraySize(array.len(), shape.slice().to_vec()).into());
        }
        if array.data_type() != &T::TENSOR_TYPE.to_arrow() {
            return Err(crate::Error::DataType(array.data_type().clone()));
        }
        let strides = shape.default_strides();
        Ok(Self::new(
            T::from_array_data(array.to_data()),
            shape,
            strides,
        ))
    }

    pub fn into_arrow(self) -> ArrayRef {
        let this = self.to_standard_layout();
        let dtype = this.arrow_type();
        if this.shape().ndim() > 1 {
            let data = unsafe {
                ArrayData::builder(dtype)
                    .add_child_data(this.values.into_values().to_data())
                    .len(this.shape[0])
                    .build_unchecked()
            };
            Arc::new(FixedSizeListArray::from(data))
        } else {
            let data = unsafe {
                this.values
                    .into_values()
                    .to_data()
                    .into_builder()
                    .data_type(dtype)
                    .build_unchecked()
            };
            make_array(data)
        }
    }

    pub fn to_standard_layout(mut self) -> Self {
        if self.is_standard_layout() {
            self
        } else {
            self.values = unsafe { T::from_trusted_len_iter(self.iter()).into() };
            self
        }
    }

    pub(crate) fn mask_inner(&self) -> Mask<S> {
        Mask::new(
            self.values.mask(),
            self.shape().clone(),
            self.strides().clone(),
        )
    }

    pub fn map<F, O>(&self, f: F) -> Tensor<O, S>
    where
        O: TensorValue,
        F: Fn(T) -> O,
    {
        unsafe { Tensor::from_trusted_len_iter(self.iter().map(f), self.shape().clone()) }
    }
}

#[macro_export]
macro_rules! tensor {
    ($([$([$($x:expr),* $(,)*]),+ $(,)*]),+ $(,)*) => {{
        $crate::Tensor3::from(vec![$([$([$($x,)*],)*],)*])
    }};
    ($([$($x:expr),* $(,)*]),+ $(,)*) => {{
        $crate::Tensor2::from(vec![$([$($x,)*],)*])
    }};
    ($($x:expr),* $(,)*) => {{
        $crate::Tensor::from(vec![$($x,)*])
    }};
}
