use std::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Rem, Sub};

use crate::{shape::NdimMax, Shape, Tensor, TensorValue};

use super::{binary_op, unary_op, TensorOp};

macro_rules! impl_scalar_ops {
    ($op:ident $kernel:tt [float $($scalar:tt)*]) => { impl_scalar_ops!($op $kernel [$($scalar)* f32 f64]); };
    ($op:ident $kernel:tt [int $($scalar:tt)*]) => { impl_scalar_ops!($op $kernel [$($scalar)* u8 u16 u32 u64 i8 i16 i32 i64]); };
    ($op:ident $kernel:tt [bit_ops $($scalar:tt)*]) => { impl_scalar_ops!($op $kernel [int $($scalar)* bool]); };
    ($op:ident $kernel:tt [$($scalar:tt)*]) => {
        $(impl_scalar_ops!($op $kernel $scalar); )*
    };
    ($op:ident $kernel:tt $scalar:tt) => {
        paste::paste! {
            // Implement scalar <op> tensor
            impl<T, S> $op<Tensor<T, S>> for $scalar
                where $scalar: TensorOp<T>,
                    T: TensorValue<Unmasked = $scalar>,
                    <$scalar as TensorOp<T>>::Output<$scalar>: TensorValue,
                    S: Shape,
            {
                type Output = Tensor<<$scalar as TensorOp<T>>::Output<$scalar>, S>;

                fn [<$op:lower>](self, rhs: Tensor<T, S>) -> Self::Output {
                    unary_op(&rhs, |x| self.apply(x, |a, b| a $kernel b))
                }
            }

            // Implement scalar <op> &tensor
            impl<T, S> $op<&Tensor<T, S>> for $scalar
                where $scalar: TensorOp<T>,
                    T: TensorValue<Unmasked = $scalar>,
                    <$scalar as TensorOp<T>>::Output<$scalar>: TensorValue,
                    S: Shape,
            {
                type Output = Tensor<<$scalar as TensorOp<T>>::Output<$scalar>, S>;

                fn [<$op:lower>](self, rhs: &Tensor<T, S>) -> Self::Output {
                    unary_op(rhs, |x| self.apply(x, |a, b| a $kernel b))
                }
            }

            // Implement tensor <op> scalar
            impl<T, S> $op<$scalar> for Tensor<T, S>
                where T: TensorOp<$scalar>,
                    T::Unmasked: $op<$scalar>,
                    <T::Unmasked as $op<$scalar>>::Output: TensorValue,
                    T::Output<<T::Unmasked as $op<$scalar>>::Output>: TensorValue,
                    S: Shape,
            {
                type Output = Tensor<T::Output<<T::Unmasked as $op<$scalar>>::Output>, S>;

                fn [<$op:lower>](self, rhs: $scalar) -> Self::Output {
                    unary_op(&self, |a| T::apply(a, rhs, |a, b| a $kernel b))
                }
            }

            // Implement &tensor <op> scalar
            impl<T, S> $op<$scalar> for &Tensor<T, S>
                where T: TensorOp<$scalar>,
                    T::Unmasked: $op<$scalar>,
                    <T::Unmasked as $op<$scalar>>::Output: TensorValue,
                    T::Output<<T::Unmasked as $op<$scalar>>::Output>: TensorValue,
                    S: Shape,
            {
                type Output = Tensor<T::Output<<T::Unmasked as $op<$scalar>>::Output>, S>;

                fn [<$op:lower>](self, rhs: $scalar) -> Self::Output {
                    unary_op(self, |a| T::apply(a, rhs, |a, b| a $kernel b))
                }
            }
        }
    };
}

macro_rules! impl_binary_op {
    ($([$op:ident $kernel:tt [$($scalars:ident)*]])+) => {
        $(
            impl_binary_op!($op $kernel [$($scalars)*]);
        )+
    };
    ($op:ident $kernel:tt [$($scalar:tt)*]) => {
        paste::paste! {
            // Implement tensor <op> tensor
            impl<T1, T2, S1, S2> $op<Tensor<T2, S2>> for Tensor<T1, S1>
                where T1: TensorOp<T2>,
                    T2: TensorValue,
                    T1::Unmasked: $op<T2::Unmasked>,
                    <T1::Unmasked as $op<T2::Unmasked>>::Output: TensorValue,
                    T1::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>: TensorValue,
                    S1: Shape + NdimMax<S2>,
                    S2: Shape,
            {
                type Output = Tensor<
                    <T1 as TensorOp<T2>>::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>,
                    <S1 as NdimMax<S2>>::Output
                >;

                fn [<$op:lower>](self, rhs: Tensor<T2, S2>) -> Self::Output {
                    binary_op(&self, &rhs, |a, b| a.apply(b, |a, b| a $kernel b))
                }
            }
            // Implement tensor <op> &tensor
            impl<T1, T2, S1, S2> $op<&Tensor<T2, S2>> for Tensor<T1, S1>
                where T1: TensorOp<T2>,
                    T2: TensorValue,
                    T1::Unmasked: $op<T2::Unmasked>,
                    <T1::Unmasked as $op<T2::Unmasked>>::Output: TensorValue,
                    T1::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>: TensorValue,
                    S1: Shape + NdimMax<S2>,
                    S2: Shape,
            {
                type Output = Tensor<
                    <T1 as TensorOp<T2>>::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>,
                    <S1 as NdimMax<S2>>::Output
                >;

                fn [<$op:lower>](self, rhs: &Tensor<T2, S2>) -> Self::Output {
                    binary_op(&self, rhs, |a, b| a.apply(b, |a, b| a $kernel b))
                }
            }

            // Implement &tensor <op> tensor
            impl<T1, T2, S1, S2> $op<Tensor<T2, S2>> for &Tensor<T1, S1>
                where T1: TensorOp<T2>,
                    T2: TensorValue,
                    T1::Unmasked: $op<T2::Unmasked>,
                    <T1::Unmasked as $op<T2::Unmasked>>::Output: TensorValue,
                    T1::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>: TensorValue,
                    S1: Shape + NdimMax<S2>,
                    S2: Shape
            {
                type Output = Tensor<
                    <T1 as TensorOp<T2>>::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>,
                    <S1 as NdimMax<S2>>::Output
                >;

                fn [<$op:lower>](self, rhs: Tensor<T2, S2>) -> Self::Output {
                    binary_op(self, &rhs, |a, b| a.apply(b, |a, b| a $kernel b))
                }
            }

            // Implement &tensor <op> &tensor
            impl<T1, T2, S1, S2> $op<&Tensor<T2, S2>> for &Tensor<T1, S1>
                where T1: TensorOp<T2>,
                    T2: TensorValue,
                    T1::Unmasked: $op<T2::Unmasked>,
                    <T1::Unmasked as $op<T2::Unmasked>>::Output: TensorValue,
                    T1::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>: TensorValue,
                    S1: Shape + NdimMax<S2>,
                    S2: Shape,
            {
                type Output = Tensor<
                    <T1 as TensorOp<T2>>::Output<<T1::Unmasked as $op<T2::Unmasked>>::Output>,
                    <S1 as NdimMax<S2>>::Output
                >;

                fn [<$op:lower>](self, rhs: &Tensor<T2, S2>) -> Self::Output {
                    binary_op(self, rhs, |a, b| a.apply(b, |a, b| a $kernel b))
                }
            }

            impl_scalar_ops!($op $kernel [$($scalar)*]);
        }
    };
}

impl_binary_op!(
    [BitAnd & [int bool]]
    [BitOr  | [int bool]]
    [BitXor ^ [int bool]]
    [Add    + [int float]]
    [Sub    - [int float]]
    [Mul    * [int float]]
    [Div    / [int float]]
    [Rem    % [int float]]
);
