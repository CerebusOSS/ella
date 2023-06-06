use crate::{shape::NdimMax, Shape, Tensor, TensorValue};

use super::{binary_op, unary_op, TensorOp};

pub trait TensorCompare<Rhs> {
    type Output;

    fn eq(&self, other: &Rhs) -> Self::Output;
    fn ne(&self, other: &Rhs) -> Self::Output;
    fn lt(&self, other: &Rhs) -> Self::Output;
    fn gt(&self, other: &Rhs) -> Self::Output;
    fn lte(&self, other: &Rhs) -> Self::Output;
    fn gte(&self, other: &Rhs) -> Self::Output;
}

macro_rules! impl_tensor_compare {
    ($([$op:ident $kernel:tt])+) => {
        // Implement scalar compare
        impl<T1, T2, S> TensorCompare<Tensor<T2, S>> for T1
            where T1: TensorOp<T2>,
                T2: TensorValue,
                S: Shape,
                T1::Unmasked: PartialOrd<T2::Unmasked>,
                T1::Output<bool>: TensorValue,
        {
            type Output = Tensor<T1::Output<bool>, S>;

            $(
                fn $op(&self, other: &Tensor<T2, S>) -> Self::Output {
                    unary_op(&other, |t| T1::apply(self.clone(), t, |a, b| a $kernel b))
                }
            )+
        }

        // Implement tensor compare
        impl<T1, T2, S1, S2> TensorCompare<Tensor<T2, S2>> for Tensor<T1, S1>
            where T1: TensorOp<T2>,
                T2: TensorValue,
                S1: Shape + NdimMax<S2>,
                S2: Shape,
                T1::Unmasked: PartialOrd<T2::Unmasked>,
                T1::Output<bool>: TensorValue,
        {
            type Output = Tensor<T1::Output<bool>, <S1 as NdimMax<S2>>::Output>;

            $(
            fn $op(&self, other: &Tensor<T2, S2>) -> Self::Output {
                binary_op(self, other, |a, b| a.apply(b, |a, b| a $kernel b))
            }
            )+
        }

        // Implement tensor compare by reference
        impl<T1, T2, S1, S2> TensorCompare<Tensor<T2, S2>> for &Tensor<T1, S1>
            where T1: TensorOp<T2>,
                T2: TensorValue,
                S1: Shape + NdimMax<S2>,
                S2: Shape,
                T1::Unmasked: PartialOrd<T2::Unmasked>,
                T1::Output<bool>: TensorValue,
        {
            type Output = Tensor<T1::Output<bool>, <S1 as NdimMax<S2>>::Output>;

            $(
            fn $op(&self, other: &Tensor<T2, S2>) -> Self::Output {
                binary_op(self, other, |a, b| a.apply(b, |a, b| a $kernel b))
            }
            )+
        }
    };
}

impl_tensor_compare!(
    [eq ==]
    [ne !=]
    [lt <]
    [gt >]
    [lte <=]
    [gte >=]
);

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn eq<C>(&self, other: C) -> C::Output
    where
        C: TensorCompare<Self>,
    {
        other.eq(self)
    }

    pub fn ne<C>(&self, other: C) -> C::Output
    where
        C: TensorCompare<Self>,
    {
        other.ne(self)
    }

    pub fn lt<C>(&self, other: C) -> C::Output
    where
        C: TensorCompare<Self>,
    {
        other.gte(self)
    }

    pub fn gt<C>(&self, other: C) -> C::Output
    where
        C: TensorCompare<Self>,
    {
        other.lte(self)
    }

    pub fn lte<C>(&self, other: C) -> C::Output
    where
        C: TensorCompare<Self>,
    {
        other.gt(self)
    }

    pub fn gte<C>(&self, other: C) -> C::Output
    where
        C: TensorCompare<Self>,
    {
        other.lt(self)
    }

    // pub fn minimum(&self, other: )
}
