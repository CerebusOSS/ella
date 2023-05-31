mod data;
mod fmt;
mod iter;

pub use data::TensorData;
pub use iter::TensorIter;

use crate::{mask::MaskData, Const, Dyn, Mask, Shape, TensorValue};

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

    pub fn is_contiguous(&self) -> bool {
        self.shape.is_contiguous(&self.strides)
    }

    #[inline]
    pub(crate) fn values(&self) -> &TensorData<T, T::Array> {
        &self.values
    }

    #[inline]
    pub(crate) fn into_values(self) -> TensorData<T, T::Array> {
        self.values
    }

    // pub(crate) fn shape_mut(&mut self) -> &mut S {
    //     &mut self.shape
    // }

    // pub(crate) fn strides_mut(&mut self) -> &mut S {
    //     &mut self.strides
    // }

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

    pub fn make_contiguous(mut self) -> Self {
        if self.is_contiguous() {
            self
        } else {
            self.values = unsafe { T::from_trusted_len_iter(self.iter()).into() };
            self
        }
    }

    pub(crate) fn mask_inner(&self) -> Mask<'_, S> {
        Mask::borrowed(
            MaskData::borrowed(self.values.nulls(), self.values.len()),
            self.shape(),
            self.strides(),
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
