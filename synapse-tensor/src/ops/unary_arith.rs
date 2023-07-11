use num_traits::{Float, Signed};

use crate::{Shape, Tensor, TensorValue};

use super::{unary_op, TensorUnaryOp};

macro_rules! impl_unary_ops {
    ($([$op:tt $kernel:path])+) => {
        $(
        pub fn $op(&self) -> Tensor<T::Output<T::Unmasked>, S> {
            unary_op(self, |x| x.apply($kernel))
        }
        )+
    };
}

impl<T, S> Tensor<T, S>
where
    T: TensorUnaryOp<Output<<T as TensorValue>::Unmasked> = T>,
    S: Shape,
    T::Unmasked: Float,
{
    impl_unary_ops!(
        [sin   Float::sin]
        [cos   Float::cos]
        [tan   Float::tan]
        [acos  Float::acos]
        [asin  Float::asin]
        [atan  Float::atan]
        [exp   Float::exp]
        [exp2  Float::exp2]
        [ln    Float::ln]
        [log2  Float::log2]
        [log10 Float::log10]
    );
}

impl<T, S> Tensor<T, S>
where
    T: TensorUnaryOp<Output<<T as TensorValue>::Unmasked> = T>,
    S: Shape,
    T::Unmasked: Signed,
{
    pub fn abs(&self) -> Tensor<T::Output<T::Unmasked>, S> {
        unary_op(self, |x| x.apply(|x| x.abs()))
    }
}
