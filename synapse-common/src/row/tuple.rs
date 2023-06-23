use super::{RowBatchBuilder, RowFormat};
use datafusion::arrow::datatypes::Field;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TupleBuilder<T> {
    builders: T,
    len: usize,
}

macro_rules! impl_tuple_row {
    ($([$($t:ident)*])+) => {
        paste::paste! {
        $(
        impl<$($t),*> RowFormat for ($($t,)*)
        where $($t: RowFormat),*
        {
            type Builder = TupleBuilder<($($t::Builder,)*)>;

            #[allow(unused_assignments, unused_mut)]
            fn builder(mut fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
                if fields.len() != <Self::Builder as RowBatchBuilder<Self>>::COLUMNS {
                    return Err(crate::Error::FieldCount(<Self::Builder as RowBatchBuilder<Self>>::COLUMNS, fields.len()));
                }
                $(
                    let cols = <$t::Builder as RowBatchBuilder<$t>>::COLUMNS;
                    let [< $t:lower >] = $t::builder(&fields[..cols])?;
                    fields = &fields[cols..];
                )*

                Ok(TupleBuilder {
                    builders: ($([< $t:lower >],)*),
                    len: 0,
                })
            }
        }

        impl<$($t),*> RowBatchBuilder<($($t,)*)> for TupleBuilder<($($t::Builder,)*)>
        where $($t: RowFormat),*
        {
            const COLUMNS: usize = $(<$t::Builder as RowBatchBuilder<$t>>::COLUMNS + )* 0;

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
                let mut cols = Vec::with_capacity(<Self as RowBatchBuilder<($($t,)*)>>::COLUMNS);
                let ($(ref mut [< $t:lower >],)*) = &mut self.builders;
                $(
                    cols.extend([< $t:lower >].build_columns()?);
                )*
                self.len = 0;
                Ok(cols)
            }
        }
        )+
        }
    };
}

impl_tuple_row!(
    []
    [T1]
    [T1 T2]
    [T1 T2 T3]
    [T1 T2 T3 T4]
    [T1 T2 T3 T4 T5]
    [T1 T2 T3 T4 T5 T6]
);
