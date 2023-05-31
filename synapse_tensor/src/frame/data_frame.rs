use std::{ops::Deref, sync::Arc};

use arrow::record_batch::RecordBatch;

use crate::{Column, Shape, Tensor, TensorValue};

use super::{batch_to_columns, frame_to_batch, Frame};

#[derive(Debug, Clone)]
pub struct DataFrame {
    columns: Arc<[Column]>,
}

impl DataFrame {
    pub fn col<T, S>(&self, name: &str) -> crate::Result<Tensor<T, S>>
    where
        T: TensorValue,
        S: Shape,
    {
        for col in self.columns.deref() {
            if name == col.name() {
                return col.typed();
            }
        }
        Err(crate::Error::ColumnLookup(name.to_string()))
    }

    pub fn icol<T, S>(&self, col: usize) -> crate::Result<Tensor<T, S>>
    where
        T: TensorValue,
        S: Shape,
    {
        self.columns[col].typed()
    }
}

impl Frame for DataFrame {
    fn ncols(&self) -> usize {
        self.columns.len()
    }

    fn column(&self, i: usize) -> &Column {
        &self.columns[i]
    }
}

impl From<&DataFrame> for RecordBatch {
    fn from(frame: &DataFrame) -> Self {
        frame_to_batch(frame)
    }
}

impl From<DataFrame> for RecordBatch {
    fn from(frame: DataFrame) -> Self {
        frame_to_batch(&frame)
    }
}

impl TryFrom<&RecordBatch> for DataFrame {
    type Error = crate::Error;

    fn try_from(rb: &RecordBatch) -> Result<Self, Self::Error> {
        let columns = batch_to_columns(rb)?;
        Ok(Self { columns })
    }
}

impl TryFrom<RecordBatch> for DataFrame {
    type Error = crate::Error;

    fn try_from(rb: RecordBatch) -> Result<Self, Self::Error> {
        let columns = batch_to_columns(&rb)?;
        Ok(Self { columns })
    }
}

impl FromIterator<Column> for DataFrame {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        let columns = iter.into_iter().collect::<Vec<_>>().into();
        Self { columns }
    }
}
