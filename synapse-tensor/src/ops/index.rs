use crate::{
    shape::{IndexValue, Indexer},
    Axis, RemoveAxis, Shape, Tensor, TensorValue,
};

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn index<I>(&self, i: I) -> T
    where
        I: Indexer<S>,
    {
        let idx = match i.index_checked(self.shape(), self.strides()) {
            Some(idx) => idx,
            None => panic!("index {:?} out of bounds for shape {:?}", i, self.shape()),
        };
        unsafe { self.values().value_unchecked(idx) }
    }
}

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape + RemoveAxis,
{
    pub fn index_axis<I: IndexValue>(&self, axis: Axis, index: I) -> Tensor<T, S::Smaller> {
        let this = self.collapse_axis(axis, index);
        let shape = this.shape().remove_axis(axis);
        let strides = this.strides().remove_axis(axis);
        Tensor::new(this.into_values(), shape, strides)
    }
}
