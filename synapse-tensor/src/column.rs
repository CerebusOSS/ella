use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{make_array, Array, ArrayData, ArrayRef, AsArray, FixedSizeListArray},
    datatypes::{DataType, Field},
};

use crate::{arrow::ExtensionType, Axis, Dyn, RemoveAxis, Shape, Tensor, TensorType, TensorValue};
use synapse_time::{Duration, Time};

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data: Arc<dyn ColumnData + 'static>,
}

impl Column {
    pub fn new<S, T>(name: S, data: T) -> Self
    where
        S: Into<String>,
        T: ColumnData + 'static,
    {
        let name = name.into();
        let data = Arc::new(data);
        Self { name, data }
    }

    #[inline]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[inline]
    pub fn shape(&self) -> Dyn {
        self.data.shape()
    }

    #[inline]
    pub fn strides(&self) -> Dyn {
        self.data.strides()
    }

    #[inline]
    pub fn dtype(&self) -> TensorType {
        self.data.tensor_type()
    }

    #[inline]
    pub fn nullable(&self) -> bool {
        self.data.nullable()
    }

    #[inline]
    pub fn typed<T: TensorValue, S: Shape>(&self) -> crate::Result<Tensor<T, S>> {
        column_to_tensor(self)
    }

    pub fn with_name<S>(mut self, name: S) -> Self
    where
        S: Into<String>,
    {
        self.name = name.into();
        self
    }
}

pub trait ColumnData: Debug {
    fn tensor_type(&self) -> TensorType;
    fn shape(&self) -> Dyn;
    fn strides(&self) -> Dyn;
    fn nullable(&self) -> bool;
    fn data(&self) -> ArrayRef;
}

impl<T, S> ColumnData for Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    fn tensor_type(&self) -> TensorType {
        T::TENSOR_TYPE
    }

    fn shape(&self) -> Dyn {
        self.shape().as_dyn()
    }

    fn strides(&self) -> Dyn {
        self.strides().as_dyn()
    }

    fn nullable(&self) -> bool {
        T::NULLABLE
    }

    fn data(&self) -> ArrayRef {
        self.values().clone().into_values()
    }
}

pub fn tensor_schema(
    name: String,
    dtype: TensorType,
    row_shape: Option<Dyn>,
    nullable: bool,
) -> Field {
    if let Some(row_shape) = row_shape {
        let row_len = row_shape.size();

        let dtype = DataType::FixedSizeList(
            Arc::new(Field::new("item", dtype.to_arrow(), true)),
            row_len as i32,
        );
        let ext = ExtensionType::tensor(row_shape);
        Field::new(name, dtype, nullable).with_metadata(ext.encode())
    } else {
        Field::new(name, dtype.to_arrow(), nullable)
    }
}

fn column_to_tensor<T, S>(col: &Column) -> crate::Result<Tensor<T, S>>
where
    T: TensorValue,
    S: Shape,
{
    let shape = S::from_shape(&col.shape())?;
    let strides = S::from_shape(&col.strides())?;
    if T::TENSOR_TYPE == col.dtype() {
        Ok(Tensor::new(
            T::from_array_data(col.data.data().to_data()),
            shape,
            strides,
        ))
    } else {
        Err(crate::Error::cast(col.dtype().clone(), T::TENSOR_TYPE))
    }
}

pub(crate) fn column_to_field(col: &Column) -> Field {
    let row_shape = if col.shape().ndim() > 1 {
        Some(col.shape().remove_axis(Axis(0)))
    } else {
        None
    };
    tensor_schema(col.name().clone(), col.dtype(), row_shape, col.nullable())
}

pub(crate) fn column_to_array(col: &Column) -> ArrayRef {
    let field = column_to_field(col);

    if !col.shape().is_standard_layout(&col.strides()) {
        todo!()
    }

    if col.shape().ndim() > 1 {
        let data = unsafe {
            ArrayData::builder(field.data_type().clone())
                .add_child_data(col.data.data().to_data())
                .len(col.shape()[0])
                .build_unchecked()
        };
        Arc::new(FixedSizeListArray::from(data))
    } else {
        let data = unsafe {
            col.data
                .data()
                .to_data()
                .into_builder()
                .data_type(field.data_type().to_owned())
                .build_unchecked()
        };
        make_array(data)
    }
}

pub(crate) fn array_to_column(field: &Field, array: &ArrayRef) -> crate::Result<Column> {
    match field.data_type() {
        DataType::FixedSizeList(inner, row_size) => {
            let dtype = TensorType::from_arrow(inner.data_type())?;
            let shape = if let Some(ExtensionType::FixedShapeTensor(tensor)) =
                ExtensionType::decode(field.metadata())?
            {
                if tensor.permutation.is_some() {
                    unimplemented!();
                }
                let mut shape = tensor.row_shape.insert_axis(Axis(0));
                shape[0] = array.len();
                shape
            } else {
                Dyn::from([array.len(), *row_size as usize])
            };
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("expected fixed-size list array")
                .values();
            Ok(make_column(field.name().clone(), dtype, shape, array))
        }
        dtype => {
            let dtype = TensorType::from_arrow(dtype)?;
            let shape = Dyn::from([array.len()]);
            Ok(make_column(field.name().clone(), dtype, shape, &array))
        }
    }
}

macro_rules! impl_make_column {
    ($([$t:ident $tensor_type:tt $arrow_type:tt])+) => {
        fn make_column(name: String, dtype: TensorType, shape: Dyn, array: &ArrayRef) -> Column {
            use ::arrow::datatypes::*;

            let strides = shape.default_strides();

            match dtype {
                TensorType::Bool => Column::new(name, Tensor::<bool, Dyn>::new(array.as_boolean().clone(), shape, strides)),
                TensorType::String => Column::new(name, Tensor::<String, Dyn>::new(array.as_string().clone(), shape, strides)),
                $(
                    TensorType::$tensor_type => Column::new(name, Tensor::<$t, Dyn>::new(array.as_primitive::<$arrow_type>().clone(), shape, strides)),
                )+
            }
        }
    };
}

impl_make_column!(
    [i8  Int8    Int8Type]
    [i16 Int16   Int16Type]
    [i32 Int32   Int32Type]
    [i64 Int64   Int64Type]
    [u8  UInt8   UInt8Type]
    [u16 UInt16  UInt16Type]
    [u32 UInt32  UInt32Type]
    [u64 UInt64  UInt64Type]
    [f32 Float32 Float32Type]
    [f64 Float64 Float64Type]
    [Duration Duration Int64Type]
    // [OffsetDateTime Timestamp Int64Type]
    [Time Timestamp Int64Type]
);
