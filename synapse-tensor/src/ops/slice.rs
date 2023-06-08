use crate::{
    shape::{stride_offset, IndexValue},
    slice::{do_slice, AxisSliceSpec, Slice, Slicer},
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

        let offset = do_slice(
            &mut shape.slice_mut()[ax_idx],
            &mut strides.slice_mut()[ax_idx],
            slice.into(),
        );
        let values = self.values().offset_exact(offset);
        Tensor::new(values, shape, strides)
    }

    pub fn collapse_axis<I: IndexValue>(&self, axis: Axis, index: I) -> Self {
        let ax = axis.index(self.shape());
        let index = index.abs_index(ax);
        let mut shape = self.shape().clone();
        shape.slice_mut()[ax] = 1;
        let offset = stride_offset(index, self.strides()[ax]);
        let values = self.values().offset_exact(offset);
        Tensor::new(values, shape, self.strides().clone())
    }

    pub fn slice<I: Slicer<S>>(&self, slice: I) -> Tensor<T, I::Shape> {
        let mut this = self.clone();
        let mut shape = <I::Shape as Shape>::zeros(slice.out_ndim());
        let mut strides = <I::Shape as Shape>::zeros(slice.out_ndim());
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
