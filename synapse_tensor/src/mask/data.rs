use std::borrow::{Borrow, Cow};

use arrow::{
    array::BooleanArray,
    buffer::{BooleanBuffer, Buffer, NullBuffer},
    util::bit_util,
};

use super::ValidityIter;

#[derive(Debug, Clone)]
pub struct MaskData<'a> {
    values: Option<Cow<'a, NullBuffer>>,
    len: usize,
}

// #[derive(Debug, Clone)]
// pub struct MaskData {
//     values: Option<NullBuffer>,
//     len: usize,
// }

impl From<NullBuffer> for MaskData<'static> {
    fn from(value: NullBuffer) -> Self {
        let len = value.len();
        Self::owned(value, len)
    }
}

impl From<BooleanBuffer> for MaskData<'static> {
    fn from(value: BooleanBuffer) -> Self {
        let values = NullBuffer::new(value);
        let len = values.len();
        Self::owned(values, len)
    }
}

impl From<BooleanArray> for MaskData<'static> {
    fn from(value: BooleanArray) -> Self {
        let values = NullBuffer::new(value.values().clone());
        let len = values.len();
        Self::owned(values, len)
    }
}

impl<'a> From<&'a NullBuffer> for MaskData<'a> {
    fn from(value: &'a NullBuffer) -> Self {
        let len = value.len();
        Self::borrowed(value, len)
        // Self::owned(value, len)
    }
}

impl From<&BooleanBuffer> for MaskData<'static> {
    fn from(value: &BooleanBuffer) -> Self {
        let values = NullBuffer::new(value.clone());
        let len = values.len();
        Self::owned(values, len)
    }
}

impl From<&BooleanArray> for MaskData<'static> {
    fn from(value: &BooleanArray) -> Self {
        value.values().into()
    }
}

impl MaskData<'static> {
    pub fn owned<N>(values: N, len: usize) -> Self
    where
        N: Into<Option<NullBuffer>>,
    {
        let values: Option<NullBuffer> = values.into();
        Self {
            values: values.map(Cow::Owned),
            len,
        }
    }
}

impl<'a> MaskData<'a> {
    pub fn borrowed<N>(values: N, len: usize) -> Self
    where
        N: Into<Option<&'a NullBuffer>>,
    {
        let values: Option<&NullBuffer> = values.into();
        Self {
            values: values.map(Cow::Borrowed),
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn into_values(self) -> Option<NullBuffer> {
        self.values.map(|v| v.into_owned())
    }

    pub fn values(&self) -> Option<&NullBuffer> {
        self.values.as_ref().map(|v| v.borrow())
    }

    pub fn num_masked(&self) -> usize {
        self.values().map_or(0, |n| n.null_count())
    }

    pub fn num_valid(&self) -> usize {
        self.len - self.num_masked()
    }

    pub fn is_valid(&self, i: usize) -> bool {
        if let Some(values) = self.values.as_ref() {
            values.is_valid(i)
        } else {
            true
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> MaskData<'static> {
        if let Some(values) = self.values() {
            let values = values.slice(offset, length);
            MaskData {
                values: Some(Cow::Owned(values)),
                len: length,
            }
        } else {
            MaskData {
                values: None,
                len: length,
            }
        }
    }

    pub fn offset(&self, offset: usize) -> MaskData<'static> {
        self.slice(offset, self.len() - offset)
    }

    pub fn all(&self) -> bool {
        self.values.as_ref().map_or(true, |v| v.null_count() == 0)
    }

    pub fn any(&self) -> bool {
        self.values
            .as_ref()
            .map_or(true, |v| v.null_count() < v.len())
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> ValidityIter<'_> {
        ValidityIter::new(self.values.clone(), self.len)
    }

    pub fn to_buffer(self) -> BooleanBuffer {
        if let Some(values) = self.values {
            values.into_owned().into_inner()
        } else {
            let num_bytes = bit_util::ceil(self.len, 8);
            let values = Buffer::from(vec![0xFF; num_bytes]);
            BooleanBuffer::new(values, 0, self.len)
        }
    }
}

impl<'a> IntoIterator for MaskData<'a> {
    type Item = bool;
    type IntoIter = ValidityIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ValidityIter::new(self.values, self.len)
    }
}
