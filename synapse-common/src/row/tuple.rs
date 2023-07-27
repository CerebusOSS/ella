use super::{RowBatchBuilder, RowFormat, RowFormatView, RowViewIter};
use datafusion::arrow::datatypes::Field;
use std::{marker::PhantomData, sync::Arc};

#[derive(Debug, Clone)]
pub struct TupleBuilder<T> {
    builders: T,
    len: usize,
}

#[derive(Debug, Clone)]
pub struct TupleView<T, V> {
    values: V,
    len: usize,
    _type: PhantomData<T>,
}

macro_rules! impl_tuple_row {
    ($([$i:literal $($t:ident)*])+) => {
        paste::paste! {
        $(
        impl<$($t),*> RowFormat for ($($t,)*)
        where $($t: RowFormat),*
        {
            const COLUMNS: usize = $(<$t as RowFormat>::COLUMNS + )* 0;
            type Builder = TupleBuilder<($($t::Builder,)*)>;
            type View = TupleView<($($t,)*), ($($t::View,)*)>;

            #[allow(unused_assignments, unused_mut)]
            fn builder(mut fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
                if fields.len() != Self::COLUMNS {
                    return Err(crate::Error::ColumnCount(Self::COLUMNS, fields.len()));
                }
                $(
                    let cols = <$t as RowFormat>::COLUMNS;
                    let [< $t:lower >] = $t::builder(&fields[..cols])?;
                    fields = &fields[cols..];
                )*

                Ok(TupleBuilder {
                    builders: ($([< $t:lower >],)*),
                    len: 0,
                })
            }

            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn view(rows: usize, mut fields: &[Arc<Field>], mut arrays: &[datafusion::arrow::array::ArrayRef]) -> crate::Result<Self::View> {
                if arrays.len() != Self::COLUMNS {
                    return Err(crate::Error::ColumnCount(Self::COLUMNS, arrays.len()));
                }
                $(
                    let cols = <$t as RowFormat>::COLUMNS;
                    let [< $t:lower >] = $t::view(rows, &fields[..cols], &arrays[..cols])?;
                    debug_assert_eq!([< $t:lower >].len(), rows);
                    fields = &fields[cols..];
                    arrays = &arrays[cols..];
                )*

                Ok(TupleView {
                    values: ($([< $t:lower >],)*),
                    _type: PhantomData,
                    len: rows,
                })
            }
        }

        impl<$($t),*> RowBatchBuilder<($($t,)*)> for TupleBuilder<($($t::Builder,)*)>
        where $($t: RowFormat),*
        {
            #[inline]
            fn len(&self) -> usize {
                self.len
            }

            fn push(&mut self, ($([<row_ $t:lower>],)*): ($($t,)*)) {
                let ($(ref mut [< $t:lower >],)*) = &mut self.builders;
                $(
                    [< $t:lower >].push([<row_ $t:lower>]);
                )*
                self.len += 1;
            }

            #[allow(unused_mut)]
            fn build_columns(&mut self) -> crate::Result<Vec<datafusion::arrow::array::ArrayRef>> {
                let mut cols = Vec::with_capacity(<($($t,)*) as RowFormat>::COLUMNS);
                let ($(ref mut [< $t:lower >],)*) = &mut self.builders;
                $(
                    cols.extend([< $t:lower >].build_columns()?);
                )*
                self.len = 0;
                Ok(cols)
            }
        }

        impl<$($t),*> RowFormatView<($($t,)*)> for TupleView<($($t,)*), ($($t::View,)*)>
        where $($t: RowFormat),*
        {
            #[inline]
            fn len(&self) -> usize {
                self.len
            }

            #[allow(unused_variables, clippy::unused_unit)]
            fn row(&self, i: usize) -> ($($t,)*) {
                let ($(ref [< $t:lower >],)*) = &self.values;
                ($(
                    [< $t:lower >].row(i),
                )*)
            }

            #[allow(unused_variables, clippy::unused_unit)]
            unsafe fn row_unchecked(&self, i: usize) -> ($($t,)*) {
                let ($(ref [< $t:lower >],)*) = &self.values;
                ($(
                    [< $t:lower >].row_unchecked(i),
                )*)
            }
        }

        impl<$($t),*> IntoIterator for TupleView<($($t,)*), ($($t::View,)*)>
        where $($t: RowFormat),*
        {
            type Item = ($($t,)*);
            type IntoIter = RowViewIter<($($t,)*), Self>;

            fn into_iter(self) -> Self::IntoIter {
                RowViewIter::new(self)
            }
        }
        )+
        }
    };
}

impl_tuple_row!(
    [0]
    [1 T1]
    [2 T1 T2]
    [3 T1 T2 T3]
    [4 T1 T2 T3 T4]
    [5 T1 T2 T3 T4 T5]
    [6 T1 T2 T3 T4 T5 T6]
);
