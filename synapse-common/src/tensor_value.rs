use crate::TensorType;
use crate::{Duration, OffsetDateTime, Time};
use datafusion::arrow::{
    array::{Array, ArrayData, BooleanArray, PrimitiveArray, StringArray},
    datatypes::*,
};
use std::fmt::{Debug, Write};
use time::format_description::well_known::Rfc3339;

/// A trait for data that can be stored as in a tensor.
pub trait TensorValue: Debug + Clone + PartialEq + PartialOrd + 'static {
    /// Arrow array type used to store raw values.
    type Array: Array + Clone + 'static;
    /// Masked value type. For `Option<T>` this is `Option<T>`.
    /// For all other `T` this should be `Option<T>`.
    type Masked: TensorValue<Array = Self::Array>;
    /// Unmasked value type. For Option<T> shis is `T`. For all other `T` this should be `T`.
    type Unmasked: TensorValue<Array = Self::Array>;

    const TENSOR_TYPE: TensorType;
    /// Whether this type is nullable/maskable. Should be `false` for all types except `Option<T>`.
    const NULLABLE: bool;

    /// Returns the value at index `i` in `array`.
    /// Panics if `i >= array.len()`.
    fn value(array: &Self::Array, i: usize) -> Self;
    /// Returns the value at index `i` without bounds checking.
    ///
    /// # Safety
    /// Calling this method where `i >= array.len()` is undefined behavior.
    unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self;

    /// Wrap `value` in its masked type.
    ///
    /// For `Option<T>` this is a no-op. For all other types this returns `Some(T)`.
    fn to_masked(value: Self) -> Self::Masked;
    /// Unwrap the inner value from its masked type.
    ///
    /// For `Option<T>` this method panics unless `value` is `Some(T)`.
    /// For all other types this is a no-op.
    fn to_unmasked(value: Self) -> Self::Unmasked;

    /// Constructs an array from an iterator of masked values.
    fn from_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>;

    /// Constructs an array from an iterator of values.
    fn from_iter<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self>,
    {
        Self::from_iter_masked(iter.into_iter().map(Self::to_masked))
    }

    fn from_vec(values: Vec<Self>) -> Self::Array;

    /// Constructs an array from an iterator of masked values.
    ///
    /// `iter` must implement [`ExactSizeIterator`].
    ///
    /// # Safety
    /// Calling this method with an iterator that doesn't correctly report its
    /// length is undefined behavior.
    unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>;

    /// Constructs an array from an iterator of values.
    ///
    /// `iter` must implement [`ExactSizeIterator`].
    ///
    /// # Safety
    /// Calling this method with an iterator that doesn't correctly report its
    /// length is undefined behavior.
    unsafe fn from_trusted_len_iter<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self>,
    {
        Self::from_trusted_len_iter_masked(iter.into_iter().map(Self::to_masked))
    }

    /// Returns a slice of `array` from `offset` to `offset + length`.
    ///
    /// Panics if `offset + length` > `array.len()`.
    fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array;
    /// Constructs an array from [`ArrowData`].
    ///
    /// Panics if `data` is not convertable to [`Self::Array`].
    fn from_array_data(data: ArrayData) -> Self::Array;

    /// Writes the value of `self` to formatter `f`.
    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
}

/// Trait that allows casting between [`TensorValue::Masked`] and [`Option<TensorValue>`]
pub trait MaskedValue: TensorValue {
    fn to_option(self) -> Option<Self::Unmasked>;
    fn from_option(value: Option<Self::Unmasked>) -> Self;
}

macro_rules! impl_tensor_value_primitive {
    ($([$t:ident $arrow:ident $dtype:tt])+) => {
        $(
        impl TensorValue for $t {
            type Array = PrimitiveArray<$arrow>;
            type Masked = Option<$t>;
            type Unmasked = Self;

            const TENSOR_TYPE: TensorType = TensorType::$dtype;
            const NULLABLE: bool = false;

            #[inline]
            fn value(array: &Self::Array, i: usize) -> Self {
                array.value(i)
            }

            #[inline]
            unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self {
                array.value_unchecked(i)
            }

            #[inline]
            fn to_masked(value: Self) -> Self::Masked {
                Some(value)
            }

            #[inline]
            fn to_unmasked(value: Self) -> Self::Unmasked {
                value
            }

            fn from_iter_masked<I>(iter: I) -> Self::Array
                where I: IntoIterator<Item=Self::Masked>
            {
                PrimitiveArray::<$arrow>::from_iter(iter)
            }

            fn from_vec(values: Vec<Self>) -> Self::Array {
                PrimitiveArray::<$arrow>::from(values)
            }

            unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
                where I: IntoIterator<Item=Self::Masked>
            {
                PrimitiveArray::<$arrow>::from_trusted_len_iter(iter)
            }

            fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array {
                array.slice(offset, length)
            }

            #[inline]
            fn from_array_data(data: ArrayData) -> Self::Array {
                PrimitiveArray::<$arrow>::from(data)
            }

            fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                <Self as std::fmt::Display>::fmt(self, f)
            }
        }
        )+
    };
}

impl_tensor_value_primitive!(
    [f32 Float32Type Float32]
    [f64 Float64Type Float64]
    [i8  Int8Type    Int8]
    [i16 Int16Type   Int16]
    [i32 Int32Type   Int32]
    [i64 Int64Type   Int64]
    [u8  UInt8Type   UInt8]
    [u16 UInt16Type  UInt16]
    [u32 UInt32Type  UInt32]
    [u64 UInt64Type  UInt64]
);

impl TensorValue for bool {
    type Array = BooleanArray;
    type Masked = Option<bool>;
    type Unmasked = Self;

    const TENSOR_TYPE: TensorType = TensorType::Bool;
    const NULLABLE: bool = false;

    #[inline]
    fn value(array: &Self::Array, i: usize) -> Self {
        array.value(i)
    }

    #[inline]
    unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self {
        array.value_unchecked(i)
    }

    #[inline]
    fn to_masked(value: Self) -> Self::Masked {
        Some(value)
    }

    #[inline]
    fn to_unmasked(value: Self) -> Self::Unmasked {
        value
    }

    #[inline]
    fn from_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        BooleanArray::from_iter(iter)
    }

    #[inline]
    fn from_vec(values: Vec<Self>) -> Self::Array {
        BooleanArray::from(values)
    }

    #[inline]
    unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        BooleanArray::from_iter(iter)
    }

    #[inline]
    fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array {
        array.slice(offset, length)
    }

    #[inline]
    fn from_array_data(data: ArrayData) -> Self::Array {
        BooleanArray::from(data)
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl<T> TensorValue for Option<T>
where
    T: TensorValue<Masked = Self>,
{
    type Array = T::Array;
    type Masked = Self;
    type Unmasked = T;

    const TENSOR_TYPE: TensorType = T::TENSOR_TYPE;
    const NULLABLE: bool = true;

    fn value(array: &Self::Array, i: usize) -> Self {
        if array.is_valid(i) {
            Some(T::value(array, i))
        } else {
            None
        }
    }

    #[inline]
    unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self {
        if array.is_valid(i) {
            Some(T::value_unchecked(array, i))
        } else {
            None
        }
    }

    #[inline]
    fn to_masked(value: Self) -> Self::Masked {
        value
    }

    #[inline]
    fn to_unmasked(value: Self) -> Self::Unmasked {
        value.unwrap()
    }

    unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        T::from_trusted_len_iter_masked(iter)
    }

    unsafe fn from_trusted_len_iter<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self>,
    {
        T::from_trusted_len_iter_masked(iter)
    }

    fn from_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        T::from_iter_masked(iter)
    }

    fn from_vec(values: Vec<Self>) -> Self::Array {
        unsafe { T::from_trusted_len_iter_masked(values) }
    }

    #[inline]
    fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array {
        T::slice(array, offset, length)
    }

    #[inline]
    fn from_array_data(data: ArrayData) -> Self::Array {
        T::from_array_data(data)
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(value) = self {
            value.format(f)
        } else {
            f.write_char('-')
        }
    }
}

impl<T> MaskedValue for Option<T>
where
    T: TensorValue<Masked = Self, Unmasked = T>,
{
    #[inline]
    fn to_option(self) -> Option<Self::Unmasked> {
        self
    }

    #[inline]
    fn from_option(value: Option<Self::Unmasked>) -> Self {
        value
    }
}

impl TensorValue for Duration {
    type Array = PrimitiveArray<DurationNanosecondType>;
    type Masked = Option<Self>;
    type Unmasked = Self;

    const TENSOR_TYPE: TensorType = TensorType::Duration;
    const NULLABLE: bool = false;

    fn value(array: &Self::Array, i: usize) -> Self {
        Duration::nanoseconds(array.value(i))
    }

    #[inline]
    unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self {
        Duration::nanoseconds(array.value_unchecked(i))
    }

    #[inline]
    fn to_masked(value: Self) -> Self::Masked {
        Some(value)
    }

    #[inline]
    fn to_unmasked(value: Self) -> Self::Unmasked {
        value
    }

    fn from_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        PrimitiveArray::from_iter(
            iter.into_iter()
                .map(|d| d.map(|d| d.whole_nanoseconds() as i64)),
        )
    }

    fn from_vec(values: Vec<Self>) -> Self::Array {
        unsafe { Self::from_trusted_len_iter(values) }
    }

    unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        PrimitiveArray::from_trusted_len_iter(
            iter.into_iter()
                .map(|d| d.map(|d| d.whole_nanoseconds() as i64)),
        )
    }

    fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array {
        array.slice(offset, length)
    }

    fn from_array_data(data: ArrayData) -> Self::Array {
        data.into()
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_seconds_f64().format(f)
    }
}

impl TensorValue for OffsetDateTime {
    type Array = PrimitiveArray<TimestampNanosecondType>;
    type Masked = Option<Self>;
    type Unmasked = Self;

    const TENSOR_TYPE: TensorType = TensorType::Timestamp;
    const NULLABLE: bool = false;

    fn value(array: &Self::Array, i: usize) -> Self {
        OffsetDateTime::from_unix_timestamp_nanos(array.value(i).into()).unwrap()
    }

    unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self {
        OffsetDateTime::from_unix_timestamp_nanos(array.value_unchecked(i).into()).unwrap()
    }

    #[inline]
    fn to_masked(value: Self) -> Self::Masked {
        Some(value)
    }

    #[inline]
    fn to_unmasked(value: Self) -> Self::Unmasked {
        value
    }

    fn from_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        PrimitiveArray::from_iter(
            iter.into_iter()
                .map(|t| t.map(|t| t.unix_timestamp_nanos() as i64)),
        )
    }

    fn from_vec(values: Vec<Self>) -> Self::Array {
        unsafe { Self::from_trusted_len_iter(values) }
    }

    unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        PrimitiveArray::from_trusted_len_iter(
            iter.into_iter()
                .map(|t| t.map(|t| t.unix_timestamp_nanos() as i64)),
        )
    }

    fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array {
        array.slice(offset, length)
    }

    fn from_array_data(data: ArrayData) -> Self::Array {
        data.into()
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&OffsetDateTime::format(*self, &Rfc3339).unwrap())
    }
}

impl TensorValue for Time {
    type Array = PrimitiveArray<TimestampNanosecondType>;
    type Masked = Option<Self>;
    type Unmasked = Self;

    const TENSOR_TYPE: TensorType = TensorType::Timestamp;
    const NULLABLE: bool = false;

    fn value(array: &Self::Array, i: usize) -> Self {
        Time::from_timestamp(array.value(i))
    }

    unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self {
        Time::from_timestamp(array.value_unchecked(i))
    }

    #[inline]
    fn to_masked(value: Self) -> Self::Masked {
        Some(value)
    }

    #[inline]
    fn to_unmasked(value: Self) -> Self::Unmasked {
        value
    }

    fn from_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        PrimitiveArray::from_iter(iter.into_iter().map(|t| t.map(|t| t.timestamp())))
            .with_data_type(TensorType::Timestamp.to_arrow())
    }

    fn from_vec(values: Vec<Self>) -> Self::Array {
        unsafe { Self::from_trusted_len_iter(values) }
    }

    unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        PrimitiveArray::from_trusted_len_iter(iter.into_iter().map(|t| t.map(|t| t.timestamp())))
            .with_data_type(TensorType::Timestamp.to_arrow())
    }

    fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array {
        array.slice(offset, length)
    }

    fn from_array_data(data: ArrayData) -> Self::Array {
        data.into()
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <_ as std::fmt::Display>::fmt(self, f)
    }
}

impl TensorValue for String {
    type Array = StringArray;
    type Masked = Option<String>;
    type Unmasked = String;
    const TENSOR_TYPE: TensorType = TensorType::String;
    const NULLABLE: bool = false;

    fn value(array: &Self::Array, i: usize) -> Self {
        array.value(i).to_string()
    }

    unsafe fn value_unchecked(array: &Self::Array, i: usize) -> Self {
        array.value_unchecked(i).to_string()
    }

    fn to_masked(value: Self) -> Self::Masked {
        Some(value)
    }

    fn to_unmasked(value: Self) -> Self::Unmasked {
        value
    }

    fn from_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        StringArray::from_iter(iter)
    }

    fn from_vec(values: Vec<Self>) -> Self::Array {
        StringArray::from_iter_values(values)
    }

    unsafe fn from_trusted_len_iter_masked<I>(iter: I) -> Self::Array
    where
        I: IntoIterator<Item = Self::Masked>,
    {
        StringArray::from_iter(iter)
    }

    fn slice(array: &Self::Array, offset: usize, length: usize) -> Self::Array {
        array.slice(offset, length)
    }

    fn from_array_data(data: ArrayData) -> Self::Array {
        data.into()
    }

    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('"')?;
        f.write_str(self)?;
        f.write_char('"')
    }
}
