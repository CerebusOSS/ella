use std::sync::Arc;

use datafusion::arrow::datatypes::Field;

use super::{RowBatchBuilder, RowFormat};

impl<T, const N: usize> RowFormat for [T; N]
where
    T: RowFormat,
{
    type Builder = ArrayBuilder<T::Builder, N>;

    fn builder(mut fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        if fields.len() != <Self::Builder as RowBatchBuilder<Self>>::COLUMNS {
            return Err(crate::Error::FieldCount(
                <Self::Builder as RowBatchBuilder<Self>>::COLUMNS,
                fields.len(),
            ));
        }

        let mut builders = Vec::with_capacity(N);
        let cols = <T::Builder as RowBatchBuilder<T>>::COLUMNS;
        for _ in 0..N {
            builders.push(T::builder(&fields[..cols])?);
            fields = &fields[cols..];
        }
        let builders = builders.try_into().unwrap();
        Ok(ArrayBuilder { builders, len: 0 })
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
    const COLUMNS: usize = <T::Builder as RowBatchBuilder<T>>::COLUMNS * N;

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
        let mut cols = Vec::with_capacity(<Self as RowBatchBuilder<[T; N]>>::COLUMNS);
        for builder in &mut self.builders {
            cols.extend(builder.build_columns()?);
        }
        self.len = 0;
        Ok(cols)
    }
}
