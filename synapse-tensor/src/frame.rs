mod data_frame;

use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};
pub use data_frame::DataFrame;

use crate::{column::array_to_column, tensor_schema, NamedColumn};

pub trait Frame {
    fn ncols(&self) -> usize;
    fn column(&self, i: usize) -> &NamedColumn;
    fn columns(&self) -> FrameColIter<'_, Self> {
        FrameColIter {
            frame: self,
            index: 0,
        }
    }
}

pub(crate) fn batch_to_columns(rb: &RecordBatch) -> crate::Result<Arc<[NamedColumn]>> {
    let schema = rb.schema();
    let mut columns = Vec::with_capacity(rb.num_columns());

    for (array, field) in rb.columns().iter().zip(schema.fields()) {
        let col = array_to_column(field, array.clone())?;
        columns.push(NamedColumn::new(field.name().clone(), col));
    }
    Ok(columns.into())
}

pub(crate) fn frame_to_batch<F: Frame>(frame: &F) -> RecordBatch {
    let columns = frame.columns().map(|c| c.to_arrow()).collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(frame_to_schema(frame)), columns).unwrap()
}

pub(crate) fn frame_to_schema<F: Frame>(frame: &F) -> Schema {
    Schema::new(
        frame
            .columns()
            .map(|c| {
                tensor_schema(
                    c.name().to_string(),
                    c.tensor_type(),
                    c.row_shape(),
                    c.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    )
}

pub struct FrameColIter<'a, F: ?Sized> {
    frame: &'a F,
    index: usize,
}

impl<'a, F: Frame> Iterator for FrameColIter<'a, F> {
    type Item = &'a NamedColumn;

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
    ($($($name:tt).+ = $col:expr),+ $(,)?) => {
        [$($crate::NamedColumn::new(stringify!($($name).+), std::sync::Arc::new($col) as $crate::ColumnRef)),+]
            .into_iter()
            .collect::<$crate::DataFrame>()
    };
}
