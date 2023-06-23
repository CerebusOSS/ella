use std::sync::Arc;

use datafusion::arrow::datatypes::Field;

use crate::TensorValue;

use super::{RowBatchBuilder, RowFormat};

impl<T> RowFormat for T
where
    T: TensorValue,
{
    type Builder = ScalarBuilder<T>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        if fields.len() != 1 {
            return Err(crate::Error::FieldCount(1, fields.len()));
        }
        if (!T::NULLABLE && fields[0].is_nullable())
            || !(fields[0].data_type().contains(&T::TENSOR_TYPE.to_arrow()))
        {
            return Err(crate::Error::IncompatibleRow(fields[0].clone()));
        }
        Ok(ScalarBuilder { values: Vec::new() })
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
    const COLUMNS: usize = 1;

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
