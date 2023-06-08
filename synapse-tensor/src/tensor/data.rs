use std::{marker::PhantomData, sync::Arc};

use arrow::{array::Array, buffer::NullBuffer};

use crate::{mask::MaskData, tensor_value::TensorValue};

#[derive(Debug)]
pub struct TensorData<T, A> {
    values: Arc<A>,
    offset: usize,
    len: usize,
    _type: PhantomData<T>,
}

impl<T, A> Clone for TensorData<T, A>
where
    T: TensorValue<Array = A>,
    A: Array,
{
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            offset: self.offset,
            len: self.len,
            _type: self._type.clone(),
        }
    }
}

impl<T, A> From<A> for TensorData<T, A>
where
    T: TensorValue<Array = A>,
    A: Array,
{
    fn from(values: A) -> Self {
        let len = values.len();
        Self {
            values: Arc::new(values),
            offset: 0,
            len,
            _type: PhantomData,
        }
    }
}

impl<T, A> From<Arc<A>> for TensorData<T, A>
where
    T: TensorValue<Array = A>,
    A: Array,
{
    fn from(values: Arc<A>) -> Self {
        let len = values.len();
        Self {
            values,
            offset: 0,
            len,
            _type: PhantomData,
        }
    }
}

impl<T, A> TensorData<T, A>
where
    T: TensorValue<Array = A>,
    A: Array + 'static,
{
    pub fn new(values: Arc<T::Array>, offset: usize, len: usize) -> Self {
        Self {
            values,
            offset,
            len,
            _type: PhantomData,
        }
    }

    pub fn into_values(self) -> Arc<T::Array> {
        self.shrink_to_fit().values
    }

    pub fn mask(&self) -> MaskData {
        MaskData::new(self.values.nulls().cloned(), self.offset, self.len())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[allow(dead_code)]
    pub fn value(&self, i: isize) -> T {
        let i = i + self.offset as isize;
        debug_assert!(i >= 0);
        T::value(&self.values, i as usize)
    }

    pub unsafe fn value_unchecked(&self, i: isize) -> T {
        let i = i + self.offset as isize;
        debug_assert!(i >= 0);
        T::value_unchecked(&self.values, i as usize)
    }

    pub fn slice_exact(&self, offset: isize, length: usize) -> Self {
        debug_assert!(self.offset as isize + offset >= 0);
        let offset = (self.offset as isize + offset) as usize;
        debug_assert!(length + offset <= self.values.len());
        let values = Arc::new(T::slice(&self.values, offset, length));
        Self::new(values, 0, length)
    }

    pub fn slice(&self, offset: isize, length: usize) -> Self {
        debug_assert!(self.offset as isize + offset >= 0);
        let offset = (self.offset as isize + offset) as usize;
        debug_assert!(
            length + offset <= self.values.len(),
            "length {} exceeds buffer length {}",
            length + offset,
            self.values.len()
        );
        Self::new(self.values.clone(), offset, length)
    }

    pub fn offset_exact(&self, offset: isize) -> Self {
        self.slice_exact(offset, (self.len as isize - offset) as usize)
    }

    pub fn offset(&self, offset: isize) -> Self {
        self.slice(offset, (self.len as isize - offset) as usize)
    }

    pub fn with_mask(self, mask: Option<MaskData>) -> Self {
        let mask = if let Some(mask) = mask {
            assert_eq!(self.len(), mask.len());
            mask.shrink_to_fit().into_values()
        } else {
            None
        };

        let values = unsafe {
            self.shrink_to_fit()
                .values
                .to_data()
                .into_builder()
                .nulls(mask)
                .build_unchecked()
        };
        let values = T::from_array_data(values);

        let len = values.len();
        TensorData::new(values.into(), 0, len)
    }

    pub fn cast<O>(self) -> TensorData<O, A>
    where
        O: TensorValue<Array = A>,
    {
        TensorData {
            values: self.values,
            offset: self.offset,
            len: self.len,
            _type: PhantomData,
        }
    }

    pub fn shrink_to_fit(self) -> Self {
        if self.offset != 0 || self.len != self.values.len() {
            self.slice_exact(0, self.len)
        } else {
            self
        }
    }
}

impl TensorData<bool, <bool as TensorValue>::Array> {
    pub fn to_mask_data(&self) -> MaskData {
        let values = NullBuffer::new(self.values.values().clone());
        MaskData::new(values, self.offset, self.len())
    }
}
