use crate::TensorValue;

pub trait TensorOp<Rhs: TensorValue>: TensorValue {
    type Output<Out>;

    fn apply<F, O>(self, other: Rhs, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked, Rhs::Unmasked) -> O,
        O: TensorValue,
        Self::Output<O>: TensorValue;
}

pub trait TensorUnaryOp: TensorValue {
    type Output<Out>;

    fn apply<F, O>(self, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked) -> O,
        O: TensorValue,
        Self::Output<O>: TensorValue;
}

// Implement T <op> T
impl<T> TensorOp<T> for T
where
    T: TensorValue<Unmasked = Self>,
{
    type Output<Out> = Out;

    #[inline]
    fn apply<F, O>(self, other: T, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked, <T as TensorValue>::Unmasked) -> O,
        Self::Output<O>: TensorValue,
    {
        op(self, other)
    }
}

// Implement T <op> Option<T>
impl<T> TensorOp<Option<T>> for T
where
    T: TensorValue<Unmasked = T, Masked = Option<T>>,
    Option<T>: TensorValue<Unmasked = T>,
{
    type Output<Out> = Option<Out>;

    #[inline]
    fn apply<F, O>(self, other: Option<T>, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked, <Option<T> as TensorValue>::Unmasked) -> O,
        O: TensorValue,
        Self::Output<O>: TensorValue,
    {
        other.map(|other| op(self, other))
    }
}

// Implement Option<T> <op> T
impl<T> TensorOp<T> for Option<T>
where
    T: TensorValue<Unmasked = T, Masked = Option<T>>,
    Option<T>: TensorValue<Unmasked = T>,
{
    type Output<Out> = Option<Out>;

    #[inline]
    fn apply<F, O>(self, other: T, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked, <T as TensorValue>::Unmasked) -> O,
        O: TensorValue,
        Self::Output<O>: TensorValue,
    {
        self.map(|this| op(this, other))
    }
}

// Implement Option<T> <op> Option<T>
impl<T> TensorOp<Option<T>> for Option<T>
where
    T: TensorValue<Unmasked = T, Masked = Option<T>>,
    Option<T>: TensorValue<Unmasked = T>,
{
    type Output<Out> = Option<Out>;

    #[inline]
    fn apply<F, O>(self, other: Option<T>, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked, <Option<T> as TensorValue>::Unmasked) -> O,
        O: TensorValue,
        Self::Output<O>: TensorValue,
    {
        self.zip(other).map(|(a, b)| op(a, b))
    }
}

impl<T> TensorUnaryOp for T
where
    T: TensorValue<Unmasked = T, Masked = Option<T>>,
{
    type Output<Out> = Out;

    fn apply<F, O>(self, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked) -> O,
        O: TensorValue,
        Self::Output<O>: TensorValue,
    {
        op(self)
    }
}

impl<T> TensorUnaryOp for Option<T>
where
    T: TensorValue<Unmasked = T, Masked = Option<T>>,
{
    type Output<Out> = Option<Out>;

    fn apply<F, O>(self, op: F) -> Self::Output<O>
    where
        F: Fn(Self::Unmasked) -> O,
        O: TensorValue,
        Self::Output<O>: TensorValue,
    {
        self.map(op)
    }
}
