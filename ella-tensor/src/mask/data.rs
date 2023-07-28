use std::cmp::Ordering;

use arrow::{
    buffer::{BooleanBuffer, Buffer, NullBuffer},
    util::bit_util,
};

use super::ValidityIter;

#[derive(Debug, Clone)]
pub struct MaskData {
    values: Option<NullBuffer>,
    offset: usize,
    len: usize,
    num_masked: usize,
}

impl From<NullBuffer> for MaskData {
    fn from(value: NullBuffer) -> Self {
        let len = value.len();
        Self::new(value, 0, len)
    }
}

impl MaskData {
    pub fn new<N>(values: N, offset: usize, len: usize) -> Self
    where
        N: Into<Option<NullBuffer>>,
    {
        let values: Option<NullBuffer> = values.into();
        let num_masked = if let Some(values) = &values {
            Self::count_masked(values, offset, offset + len)
        } else {
            0
        };
        Self {
            values,
            offset,
            len,
            num_masked,
        }
    }
}

impl MaskData {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn into_values(self) -> Option<NullBuffer> {
        self.shrink_to_fit().values
    }

    pub fn num_masked(&self) -> usize {
        self.num_masked
    }

    pub fn num_valid(&self) -> usize {
        self.len - self.num_masked()
    }

    pub fn is_valid(&self, i: isize) -> bool {
        let i = i + self.offset as isize;
        debug_assert!(i >= 0);
        if let Some(values) = self.values.as_ref() {
            values.is_valid(i as usize)
        } else {
            true
        }
    }

    pub fn slice_exact(&self, offset: isize, length: usize) -> Self {
        debug_assert!(self.offset as isize + offset >= 0);
        let offset = (self.offset as isize + offset) as usize;

        if let Some(values) = &self.values {
            debug_assert!(length + offset <= values.len());
            let values = values.slice(offset, length);
            let num_masked = values.null_count();
            Self {
                values: Some(values),
                offset,
                len: length,
                num_masked,
            }
        } else {
            Self {
                values: None,
                offset: 0,
                len: length,
                num_masked: 0,
            }
        }
    }

    pub fn slice(&self, offset: isize, length: usize) -> Self {
        debug_assert!(self.offset as isize + offset >= 0);
        let offset_diff = offset;
        let offset = (self.offset as isize + offset) as usize;

        if let Some(values) = &self.values {
            debug_assert!(length + offset <= values.len());
            let mut num_masked = self.num_masked;
            match offset_diff.cmp(&0) {
                Ordering::Less => {
                    num_masked += Self::count_masked(values, offset, self.offset);
                }
                Ordering::Greater => {
                    num_masked -= Self::count_masked(values, self.offset, offset);
                }
                _ => {}
            }
            let old_end = self.offset + self.len;
            let new_end = offset + length;
            match new_end.cmp(&old_end) {
                Ordering::Greater => {
                    num_masked += Self::count_masked(values, old_end, new_end);
                }
                Ordering::Less => {
                    num_masked += Self::count_masked(values, new_end, old_end);
                }
                _ => {}
            }

            Self {
                values: Some(values.clone()),
                offset,
                len: length,
                num_masked,
            }
        } else {
            Self {
                values: None,
                offset: 0,
                len: length,
                num_masked: 0,
            }
        }
    }

    pub fn offset_exact(&self, offset: isize) -> Self {
        self.slice_exact(offset, (self.len as isize - offset) as usize)
    }

    pub fn offset(&self, offset: isize) -> Self {
        self.slice(offset, (self.len as isize - offset) as usize)
    }

    pub fn all(&self) -> bool {
        self.num_masked == 0
    }

    pub fn any(&self) -> bool {
        self.num_masked < self.len
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> ValidityIter {
        ValidityIter::new(self.clone())
    }

    fn count_masked(buffer: &NullBuffer, start: usize, end: usize) -> usize {
        if start == 0 && end == buffer.len() {
            buffer.null_count()
        } else if buffer.null_count() == buffer.len() {
            end - start
        } else if buffer.null_count() == 0 {
            0
        } else {
            let mut count = 0;
            for i in start..end {
                if buffer.is_null(i) {
                    count += 1;
                }
            }
            count
        }
    }

    pub fn into_buffer(self) -> BooleanBuffer {
        if let Some(values) = self.values {
            values.inner().slice(self.offset, self.len)
        } else {
            let num_bytes = bit_util::ceil(self.len, 8);
            let values = Buffer::from(vec![0xFF; num_bytes]);
            BooleanBuffer::new(values, 0, self.len)
        }
    }

    pub fn shrink_to_fit(self) -> Self {
        if self.offset != 0 || self.len != self.values.as_ref().map_or(self.len, |v| v.len()) {
            self.slice_exact(0, self.len)
        } else {
            self
        }
    }
}

impl IntoIterator for MaskData {
    type Item = bool;
    type IntoIter = ValidityIter;

    fn into_iter(self) -> Self::IntoIter {
        ValidityIter::new(self)
    }
}

impl IntoIterator for &MaskData {
    type Item = bool;
    type IntoIter = ValidityIter;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
