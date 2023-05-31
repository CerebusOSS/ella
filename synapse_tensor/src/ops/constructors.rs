use arrow::array::Array;
use num_traits::{Float, Num, One, Zero};

use crate::{Const, IntoShape, Shape, Tensor, TensorValue};

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn full<I, V>(shape: I, value: V) -> Self
    where
        I: IntoShape<Shape = S>,
        V: Into<T>,
    {
        let shape = shape.into_shape();
        unsafe {
            Tensor::from_trusted_len_iter(std::iter::repeat(value.into()).take(shape.size()), shape)
        }
    }

    pub fn zeros<I>(shape: I) -> Self
    where
        I: IntoShape<Shape = S>,
        T: Zero,
    {
        Self::full(shape, T::zero())
    }

    pub fn ones<I>(shape: I) -> Self
    where
        I: IntoShape<Shape = S>,
        T: One,
    {
        Self::full(shape, T::one())
    }

    pub(crate) unsafe fn from_trusted_len_iter<I>(iter: I, shape: S) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let values = unsafe { T::from_trusted_len_iter(iter) };
        let strides = shape.default_strides();
        Tensor::new(values, shape, strides)
    }
}

/// 1-D constructors
impl<T> Tensor<T, Const<1>>
where
    T: TensorValue,
{
    pub fn linspace(start: T, end: T, steps: usize) -> Self
    where
        T: Float,
    {
        let step_size = (end - start) / T::from(steps).unwrap();
        let values = (0..steps).map(|i| start + step_size * T::from(i).unwrap());
        let shape = Const([steps]);
        unsafe { Tensor::from_trusted_len_iter(values, shape) }
    }

    pub fn range(start: T, end: T, step: T) -> Self
    where
        T: Num,
    {
        let values = std::iter::successors(Some(start), |&x| {
            let value = x + step;
            if value < end {
                Some(value)
            } else {
                None
            }
        });
        values.collect()
    }
}

/// 2-D constructors
impl<T> Tensor<T, Const<2>>
where
    T: TensorValue,
{
    pub fn eye(size: usize) -> Self
    where
        T: One + Zero,
    {
        let shape = Const([size, size]);
        let iter = shape
            .indices()
            .map(|Const([r, c])| if r == c { T::one() } else { T::zero() });
        unsafe { Tensor::from_trusted_len_iter(iter, shape) }
    }
}

impl<T> From<Vec<T>> for Tensor<T, Const<1>>
where
    T: TensorValue,
{
    fn from(value: Vec<T>) -> Self {
        let shape = Const([value.len()]);
        let strides = shape.default_strides();
        Tensor::new(T::from_vec(value), shape, strides)
    }
}

impl<T, const D: usize> From<Vec<[T; D]>> for Tensor<T, Const<2>>
where
    T: TensorValue,
{
    fn from(value: Vec<[T; D]>) -> Self {
        let shape = Const([value.len(), D]);
        let strides = shape.default_strides();
        let values = unsafe { T::from_trusted_len_iter(value.into_iter().flatten()) };
        Tensor::new(values, shape, strides)
    }
}

impl<T, const D1: usize, const D2: usize> From<Vec<[[T; D2]; D1]>> for Tensor<T, Const<3>>
where
    T: TensorValue,
{
    fn from(value: Vec<[[T; D2]; D1]>) -> Self {
        let shape = Const([value.len(), D1, D2]);
        let strides = shape.default_strides();
        let values = unsafe {
            T::from_trusted_len_iter(
                value
                    .into_iter()
                    .flat_map(|inner| inner.into_iter().flatten()),
            )
        };
        Tensor::new(values, shape, strides)
    }
}

impl<T> From<T> for Tensor<T, Const<0>>
where
    T: TensorValue,
{
    fn from(value: T) -> Self {
        let values = unsafe { T::from_trusted_len_iter(std::iter::once(value)) };
        let shape = Const([]);
        let strides = Const([]);

        Tensor::new(values, shape, strides)
    }
}

impl<T> FromIterator<T> for Tensor<T, Const<1>>
where
    T: TensorValue,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let values = T::from_iter(iter);
        let shape = Const([values.len()]);
        let strides = shape.default_strides();
        Tensor::new(values, shape, strides)
    }
}
