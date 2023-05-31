use arrow::array::Array;

use crate::{mask::MaskIter, shape::ShapeIndexIter, Shape, Tensor, TensorValue};

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn iter(&self) -> TensorIter<'_, T, S> {
        TensorIter::new(
            self.values.values().as_ref(),
            self.shape.clone(),
            self.strides.clone(),
        )
    }

    pub fn iter_valid(&self) -> TensorIterValid<'_, T, S> {
        let mask = self.mask_inner();
        let valid = mask.num_valid();

        TensorIterValid::new(self.iter(), mask.into_iter(), valid)
    }
}

pub struct TensorValuesIter<'a, T: TensorValue> {
    inner: &'a T::Array,
    index: usize,
}

impl<'a, T> TensorValuesIter<'a, T>
where
    T: TensorValue,
{
    pub(crate) fn new(inner: &'a T::Array) -> Self {
        Self { inner, index: 0 }
    }
}

impl<'a, T> Iterator for TensorValuesIter<'a, T>
where
    T: TensorValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.inner.len() {
            let value = unsafe { T::value_unchecked(&self.inner, self.index) };
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

impl<'a, T> ExactSizeIterator for TensorValuesIter<'a, T>
where
    T: TensorValue,
{
    fn len(&self) -> usize {
        self.inner.len() - self.index
    }
}

pub enum TensorIter<'a, T: TensorValue, S> {
    Flat(TensorValuesIter<'a, T>),
    Shaped {
        inner: &'a T::Array,
        shape: ShapeIndexIter<S>,
        strides: S,
    },
}

impl<'a, T, S> TensorIter<'a, T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub(crate) fn new(inner: &'a T::Array, shape: S, strides: S) -> Self {
        if shape.is_contiguous(&strides) {
            Self::Flat(TensorValuesIter::new(inner))
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

impl<'a, T, S> Iterator for TensorIter<'a, T, S>
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
                    unsafe { Some(T::value_unchecked(inner, offset)) }
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
                        let idx = offset + i * stride;
                        accum = unsafe { f(accum, T::value_unchecked(&inner, idx)) };
                    }
                    index.set_last_elem(len - 1);
                    shape.index = ShapeIndexIter::shape_next(&shape.shape, index);
                }
                accum
            }
        }
    }
}

impl<'a, T, S> ExactSizeIterator for TensorIter<'a, T, S>
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

pub struct TensorIterValid<'a, T: TensorValue, S: Shape> {
    values: TensorIter<'a, T, S>,
    mask: MaskIter<'a, S>,
    remaining: Option<usize>,
}

impl<'a, T, S> TensorIterValid<'a, T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub(crate) fn new(
        values: TensorIter<'a, T, S>,
        mask: MaskIter<'a, S>,
        valid: Option<usize>,
    ) -> Self {
        Self {
            values,
            mask,
            remaining: valid,
        }
    }
}

impl<'a, T, S> Iterator for TensorIterValid<'a, T, S>
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
