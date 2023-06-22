use crate::{
    shape::{stride_offset, IndexValue},
    slice::{do_slice, AxisSliceSpec, Slice, SliceShape},
    Axis, Const, Shape, Tensor, TensorValue,
};

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn slice_axis<I>(&self, axis: Axis, slice: I) -> Self
    where
        I: Into<Slice>,
    {
        let mut shape = self.shape().clone();
        let mut strides = self.strides().clone();
        let ax_idx = axis.index(&shape);
        let slice: Slice = slice.into();

        let offset = do_slice(
            &mut shape.slice_mut()[ax_idx],
            &mut strides.slice_mut()[ax_idx],
            slice.clone(),
        );
        let values = self.values().offset(offset);
        Tensor::new(values, shape, strides)
    }

    pub fn collapse_axis<I: IndexValue>(&self, axis: Axis, index: I) -> Self {
        let ax = axis.index(self.shape());
        let index = index.abs_index(ax);
        let mut shape = self.shape().clone();
        shape.slice_mut()[ax] = 1;
        let offset = stride_offset(index, self.strides()[ax]);
        let values = self.values().offset(offset);
        Tensor::new(values, shape, self.strides().clone())
    }

    pub fn slice<I: SliceShape<S>>(&self, slice: I) -> Tensor<T, I::Out> {
        let mut this = self.clone();
        let mut shape = <I::Out as Shape>::zeros(slice.out_ndim());
        let mut strides = <I::Out as Shape>::zeros(slice.out_ndim());
        let mut in_dim: usize = 0;
        let mut out_dim = 0;

        for ax in slice.slice() {
            match ax {
                AxisSliceSpec::NewAxis => {
                    shape[out_dim] = 1;
                    strides[out_dim] = 0;
                    out_dim += 1;
                }
                &AxisSliceSpec::Index(idx) => {
                    this = this.collapse_axis(Axis(in_dim as isize), idx);
                    in_dim += 1;
                }
                AxisSliceSpec::Slice(s) => {
                    this = this.slice_axis(Axis(in_dim as isize), s.clone());
                    shape[out_dim] = this.shape()[in_dim];
                    strides[out_dim] = this.strides()[in_dim];
                    in_dim += 1;
                    out_dim += 1;
                }
            }
        }
        Tensor::new(this.into_values(), shape, strides)
    }

    pub fn diag(&self) -> Tensor<T, Const<1>> {
        let len = self.shape().slice().iter().copied().min().unwrap_or(1);
        let stride = self.strides().slice().iter().sum();

        Tensor::new(self.values().clone(), Const([len]), Const([stride]))
    }
}

#[cfg(test)]
mod test {
    use crate::{Axis, NewAxis};

    #[test]
    fn test_slice_axis() {
        let x = crate::tensor![[1, 2, 3, 4], [5, 6, 7, 8],];
        crate::assert_tensor_eq!(x.slice_axis(Axis(1), ..=1), crate::tensor![[1, 2], [5, 6]]);
        crate::assert_tensor_eq!(x.slice_axis(Axis(1), 2..), crate::tensor![[3, 4], [7, 8]]);
        crate::assert_tensor_eq!(x.slice_axis(Axis(1), 1..-2), crate::tensor![[2], [6]]);
        crate::assert_tensor_eq!(x.slice_axis(Axis(0), 1..), crate::tensor![[5, 6, 7, 8]]);
    }

    #[test]
    fn test_slice() {
        let x = crate::tensor![[1, 2, 3, 4], [5, 6, 7, 8],];

        crate::assert_tensor_eq!(x.slice(crate::slice![..1, 1..=2]), crate::tensor![[2, 3]]);
        crate::assert_tensor_eq!(
            x.slice(crate::slice![NewAxis, ..;-1, 0..3;2]),
            crate::tensor![[[5, 7], [1, 3]]]
        );
    }
}
