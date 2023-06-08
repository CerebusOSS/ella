use crate::{
    mask::MaskIter,
    shape::{stride_offset, ShapeIndexIter},
    Axis, RemoveAxis, Shape, Tensor, TensorValue,
};

use super::TensorData;

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn iter(&self) -> TensorIter<T, S> {
        TensorIter::new(
            self.values.clone(),
            self.shape.clone(),
            self.strides.clone(),
        )
    }

    pub fn iter_valid(&self) -> TensorIterValid<T, S> {
        let mask = self.mask_inner();
        let valid = mask.num_valid();

        TensorIterValid::new(self.iter(), mask.into_iter(), valid)
    }
}

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape + RemoveAxis,
{
    pub fn axis_iter<A: Into<Axis>>(&self, axis: A) -> AxisIter<T, S> {
        AxisIter::new(self.clone(), axis.into())
    }
}

impl<'a, T, S> IntoIterator for &'a Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    type Item = T;
    type IntoIter = TensorIter<T, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T, S> IntoIterator for Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    type Item = T;
    type IntoIter = TensorIter<T, S>;

    fn into_iter(self) -> Self::IntoIter {
        TensorIter::new(self.values, self.shape, self.strides)
    }
}

pub struct TensorValuesIter<T: TensorValue> {
    inner: TensorData<T, T::Array>,
    index: usize,
}

impl<T> TensorValuesIter<T>
where
    T: TensorValue,
{
    pub(crate) fn new(inner: TensorData<T, T::Array>) -> Self {
        Self { inner, index: 0 }
    }
}

impl<T> Iterator for TensorValuesIter<T>
where
    T: TensorValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.inner.len() {
            let value = unsafe { self.inner.value_unchecked(self.index as isize) };
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

impl<T> ExactSizeIterator for TensorValuesIter<T>
where
    T: TensorValue,
{
    fn len(&self) -> usize {
        self.inner.len() - self.index
    }
}

pub enum TensorIter<T: TensorValue, S> {
    Flat(TensorValuesIter<T>),
    Shaped {
        inner: TensorData<T, T::Array>,
        shape: ShapeIndexIter<S>,
        strides: S,
    },
}

impl<T, S> TensorIter<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub(crate) fn new(inner: TensorData<T, T::Array>, shape: S, strides: S) -> Self {
        if shape.is_standard_layout(&strides) {
            Self::Flat(TensorValuesIter::new(inner.slice(0, shape.size())))
        } else {
            let shape = ShapeIndexIter::new(shape);
            Self::Shaped {
                inner,
                shape,
                strides,
            }
        }
    }
}

impl<T, S> Iterator for TensorIter<T, S>
where
    T: TensorValue,
    S: Shape,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TensorIter::Flat(inner) => inner.next(),
            TensorIter::Shaped {
                inner,
                shape,
                strides,
            } => {
                if let Some(index) = shape.next() {
                    let offset = S::stride_offset(&index, strides);
                    unsafe { Some(inner.value_unchecked(offset)) }
                } else {
                    None
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }

    fn fold<B, F>(self, init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        match self {
            TensorIter::Flat(inner) => inner.fold(init, f),
            TensorIter::Shaped {
                inner,
                mut shape,
                strides,
            } => {
                let mut accum = init;
                while let Some(mut index) = shape.index {
                    let stride = strides.last_elem();
                    let elem_index = index.last_elem();
                    let len = shape.shape.last_elem();
                    let offset = S::stride_offset(&index, &strides);
                    for i in 0..(len - elem_index) {
                        let idx = offset + stride_offset(i, stride);
                        accum = unsafe { f(accum, inner.value_unchecked(idx)) };
                    }
                    index.set_last_elem(len - 1);
                    shape.index = ShapeIndexIter::shape_next(&shape.shape, index);
                }
                accum
            }
        }
    }
}

impl<T, S> ExactSizeIterator for TensorIter<T, S>
where
    T: TensorValue,
    S: Shape,
{
    fn len(&self) -> usize {
        match self {
            TensorIter::Flat(inner) => inner.len(),
            TensorIter::Shaped { shape, .. } => shape.len(),
        }
    }
}

pub struct TensorIterValid<T: TensorValue, S: Shape> {
    values: TensorIter<T, S>,
    mask: MaskIter<S>,
    remaining: Option<usize>,
}

impl<T, S> TensorIterValid<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub(crate) fn new(values: TensorIter<T, S>, mask: MaskIter<S>, valid: Option<usize>) -> Self {
        Self {
            values,
            mask,
            remaining: valid,
        }
    }
}

impl<T, S> Iterator for TensorIterValid<T, S>
where
    T: TensorValue,
    S: Shape,
{
    type Item = T::Unmasked;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let value = self.values.next()?;
            let valid = self
                .mask
                .next()
                .expect("expected mask iter and tensor iter to have the same length");
            if valid {
                if let Some(remaining) = &mut self.remaining {
                    *remaining -= 1;
                }
                return Some(T::to_unmasked(value));
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(remaining) = self.remaining {
            (remaining, Some(remaining))
        } else {
            (0, Some(self.values.len()))
        }
    }
}

pub(crate) struct ShapedIter<I> {
    iter: I,
    remaining: usize,
}

impl<I: Iterator> ShapedIter<I> {
    pub fn new<S: Shape>(iter: I, shape: &S) -> Self {
        Self {
            iter,
            remaining: shape.size(),
        }
    }
}

impl<I: Iterator> Iterator for ShapedIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.iter.next()?;
        debug_assert!(self.remaining > 0);
        self.remaining -= 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<I: Iterator> ExactSizeIterator for ShapedIter<I> {}

pub struct AxisIter<T: TensorValue, S> {
    inner: Tensor<T, S>,
    axis: usize,
    index: usize,
}

impl<T, S> AxisIter<T, S>
where
    T: TensorValue,
    S: Shape + RemoveAxis,
{
    pub(crate) fn new(inner: Tensor<T, S>, axis: Axis) -> Self {
        let axis = axis.index(inner.shape());
        Self {
            inner,
            axis,
            index: 0,
        }
    }
}

impl<T, S> Iterator for AxisIter<T, S>
where
    T: TensorValue,
    S: Shape + RemoveAxis,
{
    type Item = Tensor<T, S::Smaller>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.inner.shape()[self.axis] {
            let value = self.inner.index_axis(self.axis.into(), self.index);
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

impl<T, S> ExactSizeIterator for AxisIter<T, S>
where
    T: TensorValue,
    S: Shape + RemoveAxis,
{
    fn len(&self) -> usize {
        self.inner.shape()[self.axis] - self.index
    }
}
