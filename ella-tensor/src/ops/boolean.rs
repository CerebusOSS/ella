use crate::{Shape, Tensor, TensorValue};

impl<T, S> Tensor<T, S>
where
    T: TensorValue<Unmasked = bool>,
    S: Shape,
{
    pub fn all(&self) -> bool {
        self.iter_valid().all(|v| v)
    }

    pub fn any(&self) -> bool {
        self.iter_valid().any(|v| v)
    }
}
