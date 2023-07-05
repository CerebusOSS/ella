use std::sync::Arc;

use crate::{
    arrow::ExtensionType, column::array_to_column, Axis, Column, Dyn, Shape, Tensor, TensorValue,
};

use arrow::datatypes::{DataType, Field};
use synapse_common::{
    row::{RowBatchBuilder, RowFormat, RowFormatView, RowViewIter},
    TensorType,
};

impl<T, S> RowFormat for Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    const COLUMNS: usize = 1;
    type Builder = TensorBuilder<T, S>;
    type View = TensorRowView<T, S>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        if fields.len() != 1 {
            return Err(crate::Error::ColumnCount(1, fields.len()));
        }
        let field = fields.first().unwrap();
        let (dtype, row_shape) = match field.data_type() {
            DataType::FixedSizeList(inner, row_size) => {
                let dtype = TensorType::from_arrow(inner.data_type())?;
                let row_shape = if let Some(ExtensionType::FixedShapeTensor(tensor)) =
                    ExtensionType::decode(field.metadata())?
                {
                    if tensor.permutation.is_some() {
                        unimplemented!();
                    }
                    tensor.row_shape
                } else {
                    Dyn::from([*row_size as usize])
                };
                (dtype, row_shape)
            }
            dtype => {
                let dtype = TensorType::from_arrow(dtype)?;
                let row_shape = Dyn::from([]);
                (dtype, row_shape)
            }
        };
        if dtype != T::TENSOR_TYPE
            || row_shape.ndim() != S::NDIM.unwrap_or_else(|| row_shape.ndim())
        {
            return Err(crate::Error::IncompatibleRow(field.clone()));
        }

        Ok(TensorBuilder {
            values: Vec::new(),
            row_shape,
        })
    }

    fn view(
        rows: usize,
        fields: &[Arc<Field>],
        arrays: &[arrow::array::ArrayRef],
    ) -> synapse_common::Result<Self::View> {
        if fields.len() != 1 {
            return Err(crate::Error::ColumnCount(1, fields.len()));
        }
        debug_assert_eq!(arrays.len(), 1);
        let values: Tensor<T, S::Larger> = array_to_column(&fields[0], &arrays[0])?.typed()?;
        debug_assert_eq!(rows, values.shape()[0]);

        Ok(TensorRowView(values))
    }
}

#[derive(Debug, Clone)]
pub struct TensorBuilder<T: TensorValue, S: Shape> {
    values: Vec<Tensor<T, S>>,
    row_shape: Dyn,
}

impl<T, S> TensorBuilder<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn row_shape(&self) -> &Dyn {
        &self.row_shape
    }
}

impl<T, S> RowBatchBuilder<Tensor<T, S>> for TensorBuilder<T, S>
where
    T: TensorValue,
    S: Shape,
{
    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn push(&mut self, row: Tensor<T, S>) {
        self.values.push(row);
    }

    fn build_columns(&mut self) -> crate::Result<Vec<arrow::array::ArrayRef>> {
        let values = Tensor::stack(Axis(0), &std::mem::take(&mut self.values))?;
        Ok(vec![Column::new(String::new(), values).to_arrow()])
    }
}

#[derive(Debug, Clone)]
pub struct TensorRowView<T: TensorValue, S: Shape>(Tensor<T, S::Larger>);

impl<T, S> RowFormatView<Tensor<T, S>> for TensorRowView<T, S>
where
    T: TensorValue,
    S: Shape,
{
    fn len(&self) -> usize {
        self.0.shape()[0]
    }

    fn row(&self, i: usize) -> Tensor<T, S> {
        self.0.index_axis(Axis(0), i).as_shape().unwrap()
    }

    unsafe fn row_unchecked(&self, i: usize) -> Tensor<T, S> {
        self.0.index_axis(Axis(0), i).as_shape().unwrap()
    }
}

impl<T, S> IntoIterator for TensorRowView<T, S>
where
    T: TensorValue,
    S: Shape,
{
    type Item = Tensor<T, S>;
    type IntoIter = RowViewIter<Tensor<T, S>, Self>;

    fn into_iter(self) -> Self::IntoIter {
        RowViewIter::new(self)
    }
}
