use std::sync::Arc;

use datafusion::arrow::{array::Array, datatypes::Field, error::ArrowError};

use crate::TensorValue;

use super::{format::RowViewIter, RowBatchBuilder, RowFormat, RowFormatView};

impl<T> RowFormat for T
where
    T: TensorValue,
{
    const COLUMNS: usize = 1;
    type Builder = ScalarBuilder<T>;
    type View = ScalarRowView<T>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        if fields.len() != 1 {
            return Err(crate::Error::ColumnCount(1, fields.len()));
        }
        if (T::NULLABLE && !fields[0].is_nullable())
            || !(fields[0].data_type().contains(&T::TENSOR_TYPE.to_arrow()))
        {
            return Err(crate::Error::IncompatibleRow(fields[0].clone()));
        }
        Ok(ScalarBuilder { values: Vec::new() })
    }

    fn view(
        rows: usize,
        _fields: &[Arc<Field>],
        arrays: &[datafusion::arrow::array::ArrayRef],
    ) -> crate::Result<Self::View> {
        if arrays.len() != 1 {
            return Err(crate::Error::ColumnCount(1, arrays.len()));
        }
        debug_assert_eq!(rows, arrays[0].len());

        let values = arrays[0]
            .as_any()
            .downcast_ref::<T::Array>()
            .cloned()
            .ok_or_else(|| {
                ArrowError::CastError(format!(
                    "unable to downcast array with type {:?} to column type {:?}",
                    arrays[0].data_type(),
                    T::TENSOR_TYPE
                ))
            })?;

        Ok(ScalarRowView(values))
    }
}

#[derive(Debug, Clone)]
pub struct ScalarBuilder<T> {
    values: Vec<T>,
}

impl<T> RowBatchBuilder<T> for ScalarBuilder<T>
where
    T: TensorValue,
{
    fn len(&self) -> usize {
        self.values.len()
    }

    fn push(&mut self, row: T) {
        self.values.push(row);
    }

    fn build_columns(&mut self) -> crate::Result<Vec<datafusion::arrow::array::ArrayRef>> {
        let array = T::from_vec(std::mem::take(&mut self.values));
        Ok(vec![Arc::new(array)])
    }
}

#[derive(Debug, Clone)]
pub struct ScalarRowView<T: TensorValue>(T::Array);

impl<T: TensorValue> RowFormatView<T> for ScalarRowView<T> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn row(&self, i: usize) -> T {
        T::value(&self.0, i)
    }

    unsafe fn row_unchecked(&self, i: usize) -> T {
        T::value_unchecked(&self.0, i)
    }
}

impl<T: TensorValue> IntoIterator for ScalarRowView<T> {
    type Item = T;
    type IntoIter = RowViewIter<T, Self>;

    fn into_iter(self) -> Self::IntoIter {
        RowViewIter::new(self)
    }
}
