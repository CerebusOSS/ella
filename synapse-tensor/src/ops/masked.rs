use crate::{tensor_value::MaskedValue, Const, Mask, Shape, Tensor, TensorValue};

pub trait AsMask<S: Shape> {
    fn as_mask(&self) -> Mask<S>;
}

impl<S: Shape> AsMask<S> for Tensor<bool, S> {
    fn as_mask(&self) -> Mask<S> {
        self.into()
    }
}

impl<S: Shape> AsMask<S> for &Tensor<bool, S> {
    fn as_mask(&self) -> Mask<S> {
        (*self).into()
    }
}

impl<S: Shape> AsMask<S> for Mask<S> {
    fn as_mask(&self) -> Mask<S> {
        self.clone()
    }
}

impl<T, S> Tensor<T, S>
where
    T: MaskedValue,
    S: Shape,
{
    pub fn mask(&self) -> Mask<S> {
        self.mask_inner()
    }

    pub fn fill_masked(&self, value: T::Unmasked) -> Tensor<T::Unmasked, S> {
        self.map(|x| T::to_option(x).unwrap_or(value.clone()))
    }

    pub fn drop_mask(&self) -> Tensor<T::Unmasked, S> {
        let values = self.values().clone().with_mask(None);
        Tensor::new(values.cast(), self.shape().clone(), self.strides().clone())
    }

    pub fn compress(&self) -> Tensor<T::Unmasked, Const<1>> {
        self.iter_valid().collect()
    }
}

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn with_mask<M>(&self, mask: M) -> Tensor<T::Masked, S>
    where
        M: AsMask<S>,
    {
        let mask = mask.as_mask();
        let values = self.values().clone().with_mask(Some(mask.into_values()));
        Tensor::new(values.cast(), self.shape().clone(), self.strides().clone())
    }
}
