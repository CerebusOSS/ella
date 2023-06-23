use std::sync::Arc;

use crate::{arrow::ExtensionType, Axis, Column, Dyn, Shape, Tensor, TensorValue};

use arrow::datatypes::{DataType, Field};
use synapse_common::{
    row::{RowBatchBuilder, RowFormat},
    TensorType,
};

impl<T, S> RowFormat for Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    type Builder = TensorBuilder<T, S>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder> {
        if fields.len() != 1 {
            return Err(crate::Error::FieldCount(1, fields.len()));
        }
        let field = &fields[0];
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
                let dtype = TensorType::from_arrow(&dtype)?;
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
    const COLUMNS: usize = 1;

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
