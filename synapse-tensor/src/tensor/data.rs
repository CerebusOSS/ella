use std::{marker::PhantomData, sync::Arc};

use arrow::{array::Array, buffer::NullBuffer};

use crate::tensor_value::TensorValue;

#[derive(Debug, Clone)]
pub struct TensorData<T, A> {
    values: Arc<A>,
    _type: PhantomData<T>,
}

impl<T, A> From<A> for TensorData<T, A>
where
    T: TensorValue<Array = A>,
    A: Array,
{
    fn from(values: A) -> Self {
        Self {
            values: Arc::new(values),
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
        Self {
            values,
            _type: PhantomData,
        }
    }
}

impl<T, A> TensorData<T, A>
where
    T: TensorValue<Array = A>,
    A: Array + 'static,
{
    pub fn new(values: Arc<T::Array>) -> Self {
        Self {
            values,
            _type: PhantomData,
        }
    }

    pub fn values(&self) -> &Arc<T::Array> {
        &self.values
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.values.nulls()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[allow(dead_code)]
    pub fn value(&self, i: usize) -> T {
        T::value(&self.values, i)
    }

    pub unsafe fn value_unchecked(&self, i: usize) -> T {
        T::value_unchecked(&self.values, i)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        T::slice(&self.values, offset, length).into()
    }

    pub fn offset(&self, offset: usize) -> Self {
        self.slice(offset, self.len() - offset)
    }

    pub fn with_mask(&self, mask: Option<NullBuffer>) -> Self {
        if let Some(mask) = &mask {
            assert_eq!(self.len(), mask.len())
        }

        let values = unsafe {
            self.values
                .to_data()
                .into_builder()
                .nulls(mask)
                .build_unchecked()
        };
        let values = T::from_array_data(values);

        TensorData::new(values.into())
    }

    pub fn cast<O>(self) -> TensorData<O, A>
    where
        O: TensorValue<Array = A>,
    {
        TensorData {
            values: self.values,
            _type: PhantomData,
        }
    }
}
