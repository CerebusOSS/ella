pub mod array;
mod format;
pub mod scalar;
mod sink;
mod stream;
pub mod tuple;

use std::sync::Arc;

pub use format::{RowBatchBuilder, RowFormat, RowFormatView, RowViewIter};
pub use sink::RowSink;
pub use stream::RowStream;

use crate::Time;
use datafusion::arrow::{array::ArrayRef, datatypes::Field};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row<R>(pub Time, pub R);

impl<R: RowFormat> RowFormat for Row<R> {
    const COLUMNS: usize = R::COLUMNS + 1;
    type Builder = RowBuilder<R::Builder>;
    type View = RowView<R>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        if fields.len() != Self::COLUMNS {
            return Err(crate::Error::ColumnCount(Self::COLUMNS, fields.len()));
        }
        let time = Time::builder(&fields[..1])?;
        let values = R::builder(&fields[1..])?;
        Ok(RowBuilder { time, values })
    }

    fn view(rows: usize, fields: &[Arc<Field>], arrays: &[ArrayRef]) -> crate::Result<Self::View> {
        if arrays.len() != Self::COLUMNS {
            return Err(crate::Error::ColumnCount(Self::COLUMNS, arrays.len()));
        }
        let time = Time::view(rows, &fields[..1], &arrays[..1])?;
        let values = R::view(rows, &fields[1..], &arrays[1..])?;
        debug_assert_eq!(values.len(), rows);

        Ok(RowView { time, values })
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
    #[inline]
    fn len(&self) -> usize {
        self.time.len()
    }

    fn push(&mut self, row: Row<R>) {
        self.time.push(row.0);
        self.values.push(row.1);
    }

    fn build_columns(&mut self) -> crate::Result<Vec<ArrayRef>> {
        let mut cols = Vec::with_capacity(<Row<R> as RowFormat>::COLUMNS);
        cols.extend(self.time.build_columns()?);
        cols.extend(self.values.build_columns()?);

        Ok(cols)
    }
}

#[derive(Debug, Clone)]
pub struct RowView<R: RowFormat> {
    time: scalar::ScalarRowView<Time>,
    values: R::View,
}

impl<R: RowFormat> RowFormatView<Row<R>> for RowView<R> {
    fn len(&self) -> usize {
        self.time.len()
    }

    fn row(&self, i: usize) -> Row<R> {
        Row(self.time.row(i), self.values.row(i))
    }

    unsafe fn row_unchecked(&self, i: usize) -> Row<R> {
        Row(self.time.row_unchecked(i), self.values.row_unchecked(i))
    }
}

impl<R: RowFormat> IntoIterator for RowView<R> {
    type Item = Row<R>;
    type IntoIter = RowViewIter<Row<R>, Self>;

    fn into_iter(self) -> Self::IntoIter {
        RowViewIter::new(self)
    }
}
