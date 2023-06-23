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
use synapse_common::ops::{TensorOp, TensorUnaryOp};

fn unary_op<T, O, S, F>(t: &Tensor<T, S>, f: F) -> Tensor<O, S>
where
    T: TensorValue,
    O: TensorValue,
    S: Shape,
    F: Fn(T) -> O,
{
    unsafe { Tensor::from_trusted_len_iter(t.iter().map(f), t.shape().clone()) }
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
