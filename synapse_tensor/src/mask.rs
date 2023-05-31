mod data;
mod fmt;
mod iter;

use std::borrow::Cow;

use arrow::array::BooleanArray;

use crate::{
    shape::{IndexValue, Indexer},
    Axis, Dyn, RemoveAxis, Shape, Tensor,
};

pub(crate) use self::data::MaskData;
pub use self::iter::{MaskIter, ValidityIter};

#[derive(Clone)]
pub struct Mask<'a, S: Shape> {
    values: MaskData<'a>,
    shape: Cow<'a, S>,
    strides: Cow<'a, S>,
}

impl<S: Shape> From<&Tensor<bool, S>> for Mask<'static, S> {
    fn from(t: &Tensor<bool, S>) -> Self {
        Mask::owned(
            MaskData::from(t.values().values().as_ref()),
            t.shape().clone(),
            t.strides().clone(),
        )
    }
}

impl<S: Shape> From<Tensor<bool, S>> for Mask<'static, S> {
    fn from(t: Tensor<bool, S>) -> Self {
        (&t).into()
    }
}

impl<'a, S: Shape> From<Mask<'a, S>> for Tensor<bool, S> {
    fn from(m: Mask<'a, S>) -> Self {
        Tensor::new(
            BooleanArray::new(m.values.to_buffer(), None),
            m.shape.into_owned(),
            m.strides.into_owned(),
        )
    }
}

impl<'a, S: Shape> Mask<'a, S> {
    pub(crate) fn owned<D>(values: D, shape: S, strides: S) -> Self
    where
        D: Into<MaskData<'a>>,
    {
        Self {
            values: values.into(),
            shape: Cow::Owned(shape),
            strides: Cow::Owned(strides),
        }
    }

    pub(crate) fn borrowed<D>(values: D, shape: &'a S, strides: &'a S) -> Self
    where
        D: Into<MaskData<'a>>,
    {
        Self {
            values: values.into(),
            shape: Cow::Borrowed(shape),
            strides: Cow::Borrowed(strides),
        }
    }

    pub fn shape(&self) -> &S {
        self.shape.as_ref()
    }

    pub fn strides(&self) -> &S {
        self.strides.as_ref()
    }

    pub fn ndim(&self) -> usize {
        self.shape.ndim()
    }

    pub fn size(&self) -> usize {
        self.shape.size()
    }

    pub fn as_dyn(&self) -> Mask<'a, Dyn> {
        Mask {
            values: self.values.clone(),
            shape: Cow::Owned(self.shape.as_dyn()),
            strides: Cow::Owned(self.strides.as_dyn()),
        }
    }

    pub fn iter(&self) -> MaskIter<'_, S> {
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

    pub fn is_contiguous(&self) -> bool {
        self.shape.is_contiguous(&self.strides)
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

    pub(crate) fn into_values(self) -> MaskData<'a> {
        self.values
    }

    pub fn num_valid(&self) -> Option<usize> {
        if self.all() {
            Some(self.size())
        } else if self.none() {
            Some(0)
        } else if self.is_contiguous() {
            Some(self.values.num_valid())
        } else {
            None
        }
    }
}

impl<'a, S> Mask<'a, S>
where
    S: Shape + RemoveAxis,
{
    pub fn index_axis<I: IndexValue>(&self, axis: Axis, index: I) -> Mask<'a, S::Smaller> {
        let ax = axis.index(self.shape());
        let index = index.abs_index(ax);
        let offset = index * self.strides[ax];
        let shape = self.shape().remove_axis(axis);
        let strides = self.strides().remove_axis(axis);
        let values = self.values.offset(offset);
        Mask::owned(values, shape, strides)
    }
}

impl<'a, S> IntoIterator for Mask<'a, S>
where
    S: Shape,
{
    type Item = bool;
    type IntoIter = MaskIter<'a, S>;

    fn into_iter(self) -> Self::IntoIter {
        MaskIter::new(self)
    }
}
