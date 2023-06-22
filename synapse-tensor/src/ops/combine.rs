use crate::{
    shape::ShapeIndexIter, tensor::ShapedIter, Axis, RemoveAxis, Shape, Tensor, TensorValue,
};

#[macro_export]
macro_rules! concat {
    ($axis:expr, $($tensor:expr),+,) => {
        $crate::concat!($axis, $($tensor),+)
    };
    ($axis:expr, $($tensor:expr),+) => {
        $crate::Tensor::concat($crate::Axis::from($axis), &[$($crate::Tensor::from($tensor)),*]).unwrap()
    };
}

#[macro_export]
macro_rules! stack {
    ($axis:expr, $($tensor:expr),+,) => {
        $crate::stack!($axis, $($tensor),+)
    };
    ($axis:expr, $($tensor:expr),+) => {
        $crate::Tensor::stack($crate::Axis::from($axis), &[$($crate::Tensor::from($tensor)),*]).unwrap()
    };
}

impl<T, S> Tensor<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub fn concat(axis: Axis, tensors: &[Tensor<T, S>]) -> crate::Result<Self>
    where
        S: RemoveAxis,
    {
        if tensors.is_empty() {
            return Err(crate::Error::EmptyList);
        }
        let mut shape = tensors[0].shape().clone();
        let ax = axis.index(&shape);
        if ax >= shape.ndim() {
            return Err(crate::Error::AxisOutOfBounds(axis, shape.ndim()));
        }
        let common_shape = shape.remove_axis(axis);
        if tensors
            .iter()
            .any(|t| t.shape().remove_axis(axis) != common_shape)
        {
            return Err(crate::Error::IncompatibleShape(common_shape.as_dyn()));
        }
        let ax_size = tensors.iter().fold(0, |sz, t| sz + t.shape()[ax]);
        shape[ax] = ax_size;

        let t = if ax == 0 {
            unsafe {
                Tensor::from_trusted_len_iter(
                    ShapedIter::new(tensors.iter().flatten(), &shape),
                    shape,
                )
            }
        } else {
            let iter = CombineConcat::new(tensors, shape.clone(), ax);
            unsafe { Tensor::from_trusted_len_iter(iter, shape) }
        };
        Ok(t)
    }

    pub fn stack(axis: Axis, tensors: &[Tensor<T, S>]) -> crate::Result<Tensor<T, S::Larger>>
    where
        S::Larger: Shape<Smaller = S>,
    {
        if tensors.is_empty() {
            return Err(crate::Error::EmptyList);
        }
        let common_shape = tensors[0].shape();
        let ax = axis.insert_index(common_shape);
        if ax > common_shape.ndim() {
            return Err(crate::Error::AxisOutOfBounds(axis, common_shape.ndim()));
        }
        let mut shape = common_shape.insert_axis(axis);
        if tensors.iter().any(|t| t.shape() != common_shape) {
            return Err(crate::Error::IncompatibleShape(common_shape.as_dyn()));
        }

        shape[ax] = tensors.len();
        let t = if ax == 0 {
            unsafe {
                Tensor::from_trusted_len_iter(
                    ShapedIter::new(tensors.iter().flatten(), &shape),
                    shape,
                )
            }
        } else {
            let iter = CombineStack::new(tensors, shape.clone(), ax);
            unsafe { Tensor::from_trusted_len_iter(iter, shape) }
        };
        Ok(t)
    }
}

struct CombineConcat<'a, T: TensorValue, S> {
    tensors: &'a [Tensor<T, S>],
    shape: ShapeIndexIter<S>,
    axis: usize,
    sizes: Vec<usize>,
}

impl<'a, T, S> CombineConcat<'a, T, S>
where
    T: TensorValue,
    S: Shape,
{
    fn new(tensors: &'a [Tensor<T, S>], shape: S, axis: usize) -> Self {
        let sizes = tensors
            .iter()
            .scan(0, |agg, t| {
                *agg += t.shape()[axis];
                Some(*agg)
            })
            .collect::<Vec<_>>();
        Self {
            tensors,
            axis,
            shape: shape.indices(),
            sizes,
        }
    }

    fn map_axis(&mut self, idx: &mut usize) -> usize {
        let tidx = match self.sizes.binary_search(idx) {
            Ok(end) => end + 1,
            Err(tidx) => tidx,
        };

        if tidx > 0 {
            *idx -= self.sizes[tidx - 1];
        }
        tidx
    }
}

impl<'a, T, S> Iterator for CombineConcat<'a, T, S>
where
    T: TensorValue,
    S: Shape,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut idx = self.shape.next()?;
        let tensor = self.map_axis(&mut idx[self.axis]);
        Some(self.tensors[tensor].index(idx))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

impl<'a, T, S> ExactSizeIterator for CombineConcat<'a, T, S>
where
    T: TensorValue,
    S: Shape,
{
    fn len(&self) -> usize {
        self.shape.len()
    }
}

struct CombineStack<'a, T: TensorValue, S: Shape> {
    tensors: &'a [Tensor<T, S>],
    shape: ShapeIndexIter<S::Larger>,
    axis: usize,
}

impl<'a, T, S> CombineStack<'a, T, S>
where
    T: TensorValue,
    S: Shape,
    S::Larger: Shape<Smaller = S>,
{
    fn new(tensors: &'a [Tensor<T, S>], shape: S::Larger, axis: usize) -> Self {
        Self {
            tensors,
            axis,
            shape: shape.indices(),
        }
    }
}

impl<'a, T, S> Iterator for CombineStack<'a, T, S>
where
    T: TensorValue,
    S: Shape,
    S::Larger: Shape<Smaller = S>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.shape.next()?;
        let tensor = idx[self.axis];
        let idx = idx.remove_axis(self.axis.into());
        Some(self.tensors[tensor].index(idx))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

impl<'a, T, S> ExactSizeIterator for CombineStack<'a, T, S>
where
    T: TensorValue,
    S: Shape,
    S::Larger: Shape<Smaller = S>,
{
    fn len(&self) -> usize {
        self.shape.len()
    }
}

#[cfg(test)]
mod test {
    use crate::{Axis, Tensor};

    #[test]
    fn test_stack() {
        let x = crate::tensor![
            [[1, 2, 3], [4, 5, 6]],
            [[7, 8, 9], [10, 11, 12]],
            [[13, 14, 15], [16, 17, 18]],
        ];
        for ax in 0..3 {
            let mut parts = Vec::new();
            for i in 0..x.shape()[ax] {
                parts.push(x.index_axis(Axis(ax as isize), i));
            }
            let c = Tensor::stack(Axis(ax as isize), &parts).unwrap();
            assert!(x.eq(&c).all(), "{:?} != {:?}", x, c);
        }

        let c = crate::stack!(-1, x.index_axis(Axis(1), 0), x.index_axis(Axis(1), 1));
        let swapped = x.swap_axes(Axis(1), Axis(2));
        assert!(swapped.eq(&c).all(), "{:?} != {:?}", swapped, c);
    }

    #[test]
    fn test_concat() {
        let x = crate::tensor![
            [[1, 2, 3], [4, 5, 6]],
            [[7, 8, 9], [10, 11, 12]],
            [[13, 14, 15], [16, 17, 18]],
        ];
        for ax in 0..3 {
            let mut parts = Vec::new();
            for i in 0..x.shape()[ax] {
                parts.push(x.collapse_axis(Axis(ax as isize), i));
            }
            let c = Tensor::concat(Axis(ax as isize), &parts).unwrap();
            assert!(x.eq(&c).all(), "{:?} != {:?}", x, c);
        }

        let c = crate::concat!(2, x.slice_axis(Axis(2), 0..2), x.slice_axis(Axis(2), 2..3));
        assert!(x.eq(&c).all(), "{:?} != {:?}", x, c);

        let c = crate::concat!(-1, x.index_axis(Axis(1), 0), x.index_axis(Axis(1), 1));
        let reshaped = x.reshape((3, 6));
        assert!(reshaped.eq(&c).all(), "{:?} != {:?}", reshaped, c);
    }
}
