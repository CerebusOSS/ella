use crate::{shape::ShapeIndexIter, Mask, Shape};

use super::MaskData;

pub enum ValidityIter {
    Constant(usize, bool),
    Values { inner: MaskData, index: usize },
}

impl ValidityIter {
    pub(crate) fn new(inner: MaskData) -> Self {
        Self::as_constant(&inner).unwrap_or_else(|| Self::Values { inner, index: 0 })
    }

    fn as_constant(inner: &MaskData) -> Option<Self> {
        if inner.num_masked() == 0 {
            Some(Self::Constant(inner.len(), true))
        } else if inner.num_valid() == 0 {
            Some(Self::Constant(inner.len(), false))
        } else {
            None
        }
    }
}

impl Iterator for ValidityIter {
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
                    let value = inner.is_valid(*index as isize);
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

impl ExactSizeIterator for ValidityIter {
    fn len(&self) -> usize {
        match self {
            ValidityIter::Constant(remaining, _) => *remaining,
            ValidityIter::Values { inner, index } => inner.len() - *index,
        }
    }
}

pub enum MaskIter<S: Shape> {
    Flat(ValidityIter),
    Shaped {
        inner: Mask<S>,
        shape: ShapeIndexIter<S>,
    },
}

impl<'a, S> MaskIter<S>
where
    S: Shape,
{
    pub(crate) fn new(inner: Mask<S>) -> Self {
        if inner.is_standard_layout() {
            Self::Flat(inner.values.into_iter())
        } else {
            let shape = ShapeIndexIter::new(inner.shape().clone());
            Self::Shaped { inner, shape }
        }
    }
}

impl<'a, S> Iterator for MaskIter<S>
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

impl<S> ExactSizeIterator for MaskIter<S>
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
