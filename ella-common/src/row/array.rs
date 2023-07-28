use std::{marker::PhantomData, sync::Arc};

use datafusion::arrow::datatypes::Field;

use super::{format::RowViewIter, RowBatchBuilder, RowFormat, RowFormatView};

impl<T, const N: usize> RowFormat for [T; N]
where
    T: RowFormat,
{
    const COLUMNS: usize = T::COLUMNS * N;
    type Builder = ArrayBuilder<T::Builder, N>;
    type View = ArrayRowView<[T; N], T::View, N>;

    fn builder(mut fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        if fields.len() != Self::COLUMNS {
            return Err(crate::Error::ColumnCount(Self::COLUMNS, fields.len()));
        }

        let mut builders = Vec::with_capacity(N);
        let cols = T::COLUMNS;
        for _ in 0..N {
            builders.push(T::builder(&fields[..cols])?);
            fields = &fields[cols..];
        }
        let builders = builders.try_into().unwrap();
        Ok(ArrayBuilder { builders, len: 0 })
    }

    fn view(
        rows: usize,
        mut fields: &[Arc<Field>],
        mut arrays: &[datafusion::arrow::array::ArrayRef],
    ) -> crate::Result<Self::View> {
        if arrays.len() != Self::COLUMNS {
            return Err(crate::Error::ColumnCount(Self::COLUMNS, arrays.len()));
        }
        let mut values = Vec::with_capacity(N);
        let cols = T::COLUMNS;
        for _ in 0..N {
            values.push(T::view(rows, &fields[..cols], &arrays[..cols])?);
            arrays = &arrays[cols..];
            fields = &fields[cols..];
        }
        let values = values.try_into().unwrap();
        Ok(ArrayRowView {
            values,
            len: rows,
            _type: PhantomData,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ArrayBuilder<T, const N: usize> {
    builders: [T; N],
    len: usize,
}

impl<T, const N: usize> RowBatchBuilder<[T; N]> for ArrayBuilder<T::Builder, N>
where
    T: RowFormat,
{
    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, row: [T; N]) {
        for (builder, col) in self.builders.iter_mut().zip(row) {
            builder.push(col);
        }
        self.len += 1;
    }

    fn build_columns(&mut self) -> crate::Result<Vec<datafusion::arrow::array::ArrayRef>> {
        let mut cols = Vec::with_capacity(<[T; N] as RowFormat>::COLUMNS);
        for builder in &mut self.builders {
            cols.extend(builder.build_columns()?);
        }
        self.len = 0;
        Ok(cols)
    }
}

#[derive(Debug, Clone)]
pub struct ArrayRowView<T, V, const N: usize> {
    values: [V; N],
    _type: PhantomData<T>,
    len: usize,
}

impl<T: RowFormat, const N: usize> RowFormatView<[T; N]> for ArrayRowView<[T; N], T::View, N> {
    fn len(&self) -> usize {
        self.len
    }

    fn row(&self, i: usize) -> [T; N] {
        std::array::from_fn(|c| self.values[c].row(i))
    }

    unsafe fn row_unchecked(&self, i: usize) -> [T; N] {
        std::array::from_fn(|c| self.values[c].row_unchecked(i))
    }
}

impl<T: RowFormat, const N: usize> IntoIterator for ArrayRowView<[T; N], T::View, N> {
    type Item = [T; N];
    type IntoIter = RowViewIter<[T; N], Self>;

    fn into_iter(self) -> Self::IntoIter {
        RowViewIter::new(self)
    }
}
