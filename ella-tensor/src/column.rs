use std::{fmt::Debug, ops::Deref, sync::Arc};

use arrow::{
    array::{Array, ArrayData, ArrayRef},
    datatypes::{DataType, Field},
};

use crate::{
    arrow::ExtensionType,
    tensor::fmt::{RowDisplay, RowValue},
    Axis, Dyn, RemoveAxis, Shape, Tensor, TensorType, TensorValue,
};
use ella_common::{Duration, Time};

pub type ColumnRef = Arc<dyn Column + 'static>;

#[derive(Debug, Clone)]
pub struct NamedColumn {
    name: String,
    col: ColumnRef,
}

impl NamedColumn {
    pub fn new(name: String, col: ColumnRef) -> Self {
        Self { name, col }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn into_inner(self) -> ColumnRef {
        self.col
    }
}

impl Deref for NamedColumn {
    type Target = ColumnRef;

    fn deref(&self) -> &Self::Target {
        &self.col
    }
}

impl From<(String, ColumnRef)> for NamedColumn {
    fn from((name, col): (String, ColumnRef)) -> Self {
        NamedColumn::new(name, col)
    }
}

impl From<NamedColumn> for (String, ColumnRef) {
    fn from(NamedColumn { name, col }: NamedColumn) -> Self {
        (name, col)
    }
}

pub trait Column: Debug + Send + Sync {
    fn tensor_type(&self) -> TensorType;
    fn shape(&self) -> Dyn;
    fn strides(&self) -> Dyn;
    fn nullable(&self) -> bool;
    fn data(&self) -> ArrayData;
    fn to_arrow(&self) -> ArrayRef;

    fn arrow_type(&self) -> DataType {
        let row_shape = if self.shape().ndim() > 1 {
            Some(self.shape().remove_axis(Axis(0)))
        } else {
            None
        };
        let dtype = self.tensor_type();

        if let Some(row_shape) = row_shape {
            let row_len = row_shape.size();

            DataType::FixedSizeList(
                Arc::new(Field::new("item", dtype.to_arrow(), true)),
                row_len as i32,
            )
        } else {
            dtype.to_arrow()
        }
    }

    fn row_shape(&self) -> Option<Dyn> {
        let shape = self.shape();
        if shape.ndim() > 0 {
            Some(shape.remove_axis(Axis(0)))
        } else {
            None
        }
    }

    #[doc(hidden)]
    fn format_row(&self, idx: usize) -> RowValue<'_>;
}

impl<T, S> Column for Tensor<T, S>
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

    fn to_arrow(&self) -> ArrayRef {
        self.clone().into_arrow()
    }

    fn data(&self) -> ArrayData {
        self.values().values().to_data()
    }

    fn format_row(&self, idx: usize) -> RowValue<'_> {
        self.value(idx)
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

pub(crate) fn array_to_column(field: &Field, array: ArrayRef) -> crate::Result<ColumnRef> {
    match field.data_type() {
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
            make_column(dtype, row_shape, array)
        }
        dtype => {
            let dtype = TensorType::from_arrow(dtype)?;
            make_column(dtype, Dyn::from([]), array)
        }
    }
}

pub fn cast<T, S>(col: &ColumnRef) -> crate::Result<Tensor<T, S>>
where
    T: TensorValue,
    S: Shape,
{
    if T::TENSOR_TYPE.to_arrow() == col.tensor_type().to_arrow() {
        Ok(Tensor::new(
            T::from_array_data(col.data()),
            S::from_shape(&col.shape())?,
            S::from_shape(&col.strides())?,
        ))
    } else {
        Err(crate::Error::Cast {
            to: T::TENSOR_TYPE,
            from: col.tensor_type(),
        })
    }
}

macro_rules! impl_make_column {
    ($([$t:ident $tensor_type:tt $arrow_type:tt])+) => {
        fn make_column(dtype: TensorType, row_shape: Dyn, array: ArrayRef) -> crate::Result<ColumnRef> {
            Ok(match dtype {
                TensorType::Bool => Arc::new(Tensor::<bool, Dyn>::try_from_arrow(array, row_shape)?),
                TensorType::String => Arc::new(Tensor::<String, Dyn>::try_from_arrow(array, row_shape)?),
                $(
                    TensorType::$tensor_type => Arc::new(Tensor::<$t, Dyn>::try_from_arrow(array, row_shape)?),
                )+
            })
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
    [Duration Duration DurationNanosecondType]
    // [OffsetDateTime Timestamp Int64Type]
    [Time Timestamp TimestampNanosecondType]
);
