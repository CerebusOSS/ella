mod data;
mod fmt;
mod iter;

use arrow::array::BooleanArray;

use crate::{
    shape::{stride_offset, IndexValue, Indexer},
    Axis, Dyn, RemoveAxis, Shape, Tensor,
};

pub(crate) use self::data::MaskData;
pub use self::iter::{MaskIter, ValidityIter};

#[derive(Clone)]
pub struct Mask<S: Shape> {
    values: MaskData,
    shape: S,
    strides: S,
}

impl<S: Shape> From<&Tensor<bool, S>> for Mask<S> {
    fn from(t: &Tensor<bool, S>) -> Self {
        Mask::new(
            t.values().to_mask_data(),
            t.shape().clone(),
            t.strides().clone(),
        )
    }
}

impl<S: Shape> From<Tensor<bool, S>> for Mask<S> {
    fn from(t: Tensor<bool, S>) -> Self {
        (&t).into()
    }
}

impl<'a, S: Shape> From<Mask<S>> for Tensor<bool, S> {
    fn from(m: Mask<S>) -> Self {
        Tensor::new(
            BooleanArray::new(m.values.to_buffer(), None),
            m.shape.clone(),
            m.strides.clone(),
        )
    }
}

impl<'a, S: Shape> Mask<S> {
    pub(crate) fn new<D>(values: D, shape: S, strides: S) -> Self
    where
        D: Into<MaskData>,
    {
        Self {
            values: values.into(),
            shape,
            strides,
        }
    }

    pub fn shape(&self) -> &S {
        &self.shape
    }

    pub fn strides(&self) -> &S {
        &self.strides
    }

    pub fn ndim(&self) -> usize {
        self.shape.ndim()
    }

    pub fn size(&self) -> usize {
        self.shape.size()
    }

    pub fn as_dyn(&self) -> Mask<Dyn> {
        Mask::new(
            self.values.clone(),
            self.shape.as_dyn(),
            self.strides.as_dyn(),
        )
    }

    pub fn iter(&self) -> MaskIter<S> {
        MaskIter::new(self.clone())
    }

    pub fn index<I>(&self, i: I) -> bool
    where
        I: Indexer<S>,
    {
        let idx = match i.index_checked(self.shape(), self.strides()) {
            Some(idx) => idx,
            None => panic!("index {:?} out of bounds for shape {:?}", i, self.shape()),
        };
        self.values.is_valid(idx)
    }

    pub fn is_standard_layout(&self) -> bool {
        self.shape.is_standard_layout(&self.strides)
    }

    #[inline]
    pub fn all(&self) -> bool {
        self.values.all()
    }

    #[inline]
    pub fn any(&self) -> bool {
        self.values.any()
    }

    #[inline]
    pub fn none(&self) -> bool {
        !self.any()
    }

    pub(crate) fn into_values(self) -> MaskData {
        self.values
    }

    pub fn num_valid(&self) -> Option<usize> {
        if self.all() {
            Some(self.size())
        } else if self.none() {
            Some(0)
        } else if self.is_standard_layout() {
            Some(self.values.num_valid())
        } else {
            None
        }
    }
}

impl<S> Mask<S>
where
    S: Shape + RemoveAxis,
{
    pub fn index_axis<I: IndexValue>(&self, axis: Axis, index: I) -> Mask<S::Smaller> {
        let ax = axis.index(self.shape());
        let index = index.abs_index(ax);
        let offset = stride_offset(index, self.strides[ax]);
        let shape = self.shape().remove_axis(axis);
        let strides = self.strides().remove_axis(axis);
        let values = self.values.offset(offset);
        Mask::new(values, shape, strides)
    }
}

impl<S> IntoIterator for Mask<S>
where
    S: Shape,
{
    type Item = bool;
    type IntoIter = MaskIter<S>;

    fn into_iter(self) -> Self::IntoIter {
        MaskIter::new(self)
    }
}
