mod data_frame;

use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};
pub use data_frame::DataFrame;

use crate::{
    column::{array_to_column, column_to_array, column_to_field},
    Column,
};

pub trait Frame {
    fn ncols(&self) -> usize;
    fn column(&self, i: usize) -> &Column;
    fn columns(&self) -> FrameColIter<'_, Self> {
        FrameColIter {
            frame: self,
            index: 0,
        }
    }
}

pub(crate) fn batch_to_columns(rb: &RecordBatch) -> crate::Result<Arc<[Column]>> {
    let schema = rb.schema();
    let mut columns = Vec::with_capacity(rb.num_columns());

    for (array, field) in rb.columns().iter().zip(schema.fields()) {
        let col = array_to_column(field, array)?;
        columns.push(col);
    }
    Ok(columns.into())
}

pub(crate) fn frame_to_batch<F: Frame>(frame: &F) -> RecordBatch {
    let columns = frame
        .columns()
        .map(|col| column_to_array(col))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(frame_to_schema(frame)), columns).unwrap()
}

pub(crate) fn frame_to_schema<F: Frame>(frame: &F) -> Schema {
    Schema::new(
        frame
            .columns()
            .map(|col| column_to_field(col))
            .collect::<Vec<_>>(),
    )
}

pub struct FrameColIter<'a, F: ?Sized> {
    frame: &'a F,
    index: usize,
}

impl<'a, F: Frame> Iterator for FrameColIter<'a, F> {
    type Item = &'a Column;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.frame.ncols() {
            let col = self.frame.column(self.index);
            self.index += 1;
            Some(col)
        } else {
            None
        }
    }
}

#[macro_export]
macro_rules! frame {
    () => {
        $crate::DataFrame::new()
    };
    ($($name:ident=$col:expr),+ $(,)?) => {
        [$($crate::Column::new(stringify!($name), $col)),+]
            .into_iter()
            .collect::<$crate::DataFrame>()
    };
}
