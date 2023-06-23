pub mod array;
mod format;
pub mod scalar;
pub mod tuple;

use std::sync::Arc;

pub use format::{RowBatchBuilder, RowFormat};

use crate::Time;
use datafusion::arrow::{array::ArrayRef, datatypes::Field};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row<R>(pub Time, pub R);

impl<R: RowFormat> RowFormat for Row<R> {
    type Builder = RowBuilder<R::Builder>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        let time = Time::builder(&fields[..1])?;
        let values = R::builder(&fields[1..])?;
        Ok(RowBuilder { time, values })
    }
}

#[derive(Debug, Clone)]
pub struct RowBuilder<R> {
    time: scalar::ScalarBuilder<Time>,
    values: R,
}

impl<R> RowBatchBuilder<Row<R>> for RowBuilder<R::Builder>
where
    R: RowFormat,
{
    const COLUMNS: usize = <R::Builder as RowBatchBuilder<R>>::COLUMNS + 1;

    #[inline]
    fn len(&self) -> usize {
        self.time.len()
    }

    fn push(&mut self, row: Row<R>) {
        self.time.push(row.0);
        self.values.push(row.1);
    }

    fn build_columns(&mut self) -> crate::Result<Vec<ArrayRef>> {
        let mut cols = Vec::with_capacity(<Self as RowBatchBuilder<Row<R>>>::COLUMNS);
        cols.extend(self.time.build_columns()?);
        cols.extend(self.values.build_columns()?);

        Ok(cols)
    }
}
