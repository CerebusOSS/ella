use synapse_common::TensorValue;

use crate::{Const, Tensor};

impl<T: TensorValue> Tensor<T, Const<0>> {
    pub fn scalar(&self) -> T {
        debug_assert_eq!(self.values().len(), 1);
        unsafe { self.values().value_unchecked(0) }
    }
}

impl<T: TensorValue> From<Tensor<T, Const<1>>> for Vec<T> {
    fn from(value: Tensor<T, Const<1>>) -> Self {
        value.into_iter().collect::<Vec<_>>()
    }
}
