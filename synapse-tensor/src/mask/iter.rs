use std::borrow::Cow;

use arrow::buffer::NullBuffer;

use crate::{shape::ShapeIndexIter, Mask, Shape};

pub enum ValidityIter<'a> {
    Constant(usize, bool),
    Values {
        inner: Cow<'a, NullBuffer>,
        index: usize,
    },
}

impl<'a> ValidityIter<'a> {
    pub(crate) fn new(inner: Option<Cow<'a, NullBuffer>>, len: usize) -> Self {
        if let Some(inner) = inner {
            Self::as_constant(inner.as_ref()).unwrap_or_else(|| Self::Values { inner, index: 0 })
        } else {
            Self::Constant(len, true)
        }
    }

    fn as_constant(inner: &NullBuffer) -> Option<Self> {
        if inner.null_count() == 0 {
            Some(Self::Constant(inner.len(), true))
        } else if inner.null_count() == inner.len() {
            Some(Self::Constant(inner.len(), false))
        } else {
            None
        }
    }
}

impl<'a> Iterator for ValidityIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ValidityIter::Constant(remaining, value) => {
                if *remaining > 0 {
                    *remaining -= 1;
                    Some(*value)
                } else {
                    None
                }
            }
            ValidityIter::Values { inner, index } => {
                if *index < inner.len() {
                    let value = inner.is_valid(*index);
                    *index += 1;
                    Some(value)
                } else {
                    None
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

impl<'a> ExactSizeIterator for ValidityIter<'a> {
    fn len(&self) -> usize {
        match self {
            ValidityIter::Constant(remaining, _) => *remaining,
            ValidityIter::Values { inner, index } => inner.len() - *index,
        }
    }
}

pub enum MaskIter<'a, S: Shape> {
    Flat(ValidityIter<'a>),
    Shaped {
        inner: Mask<'a, S>,
        shape: ShapeIndexIter<S>,
    },
}

impl<'a, S> MaskIter<'a, S>
where
    S: Shape,
{
    pub(crate) fn new(inner: Mask<'a, S>) -> Self {
        if inner.is_contiguous() {
            Self::Flat(inner.values.into_iter())
        } else {
            let shape = ShapeIndexIter::new(inner.shape().clone());
            Self::Shaped { inner, shape }
        }
    }
}

impl<'a, S> Iterator for MaskIter<'a, S>
where
    S: Shape,
{
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MaskIter::Flat(inner) => inner.next(),
            MaskIter::Shaped { inner, shape } => {
                if let Some(index) = shape.next() {
                    Some(inner.index(index))
                } else {
                    None
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

impl<'a, S> ExactSizeIterator for MaskIter<'a, S>
where
    S: Shape,
{
    fn len(&self) -> usize {
        match self {
            MaskIter::Flat(inner) => inner.len(),
            MaskIter::Shaped { shape, .. } => shape.len(),
        }
    }
}
