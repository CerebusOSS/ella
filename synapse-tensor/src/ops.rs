mod binary_arith;
mod boolean;
mod builtin_arith;
mod cmp;
mod combine;
mod constructors;
mod index;
mod masked;
mod reduce;
mod scatter;
mod shape;
mod slice;
mod unary_arith;

use crate::{shape::NdimMax, Shape, Tensor, TensorValue};

fn unary_op<T, O, S, F>(t: &Tensor<T, S>, f: F) -> Tensor<O, S>
where
    T: TensorValue,
    O: TensorValue,
    S: Shape,
    F: Fn(T) -> O,
{
    unsafe { Tensor::from_trusted_len_iter(t.iter().map(|t| f(t)), t.shape().clone()) }
}

fn binary_op<T1, T2, O, S1, S2, F>(
    a: &Tensor<T1, S1>,
    b: &Tensor<T2, S2>,
    op: F,
) -> Tensor<O, <S1 as NdimMax<S2>>::Output>
where
    T1: TensorValue,
    T2: TensorValue,
    O: TensorValue,
    S1: Shape + NdimMax<S2>,
    S2: Shape,
    F: Fn(T1, T2) -> O,
{
    if a.ndim() == b.ndim() && a.shape().slice() == b.shape().slice() {
        let shape = <<S1 as NdimMax<S2>>::Output as Shape>::from_shape(a.shape()).unwrap();
        unsafe {
            Tensor::from_trusted_len_iter(a.iter().zip(b.iter()).map(|(a, b)| op(a, b)), shape)
        }
    } else {
        let (a, b) = a.broadcast_with(b).unwrap();
        let shape = a.shape().clone();
        unsafe {
            Tensor::from_trusted_len_iter(a.iter().zip(b.iter()).map(|(a, b)| op(a, b)), shape)
        }
    }
}

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
