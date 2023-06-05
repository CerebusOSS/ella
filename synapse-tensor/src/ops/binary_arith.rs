use num_traits::Float;

use crate::{shape::NdimMax, Shape, Tensor, TensorValue};

use super::{binary_op, TensorOp};

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    T::Unmasked: Float,
    S: Shape,
{
    pub fn atan2<T2, S2>(
        &self,
        x: &Tensor<T2, S2>,
    ) -> Tensor<T::Output<T::Unmasked>, <S as NdimMax<S2>>::Output>
    where
        T2: TensorValue<Unmasked = T::Unmasked>,
        S2: Shape,
        T: TensorOp<T2>,
        S: NdimMax<S2>,
        T::Output<T::Unmasked>: TensorValue,
    {
        binary_op(self, x, |y, x| y.apply(x, Float::atan2))
    }
}
