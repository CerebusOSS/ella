// use crate::tensor::{types::SynapseIntType, TensorValueIter};

use super::{Const, Dyn, Shape};
use std::fmt::Debug;

pub trait Indexer<S: Shape>: Debug {
    fn index_checked(&self, shape: &S, strides: &S) -> Option<usize>;
    fn index_unchecked(&self, shape: &S, strides: &S) -> usize;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Flat(pub usize);

impl<S> Indexer<S> for Flat
where
    S: Shape,
{
    fn index_checked(&self, shape: &S, strides: &S) -> Option<usize> {
        if self.0 < shape.size() {
            Some(self.index_unchecked(shape, strides))
        } else {
            None
        }
    }

    fn index_unchecked(&self, shape: &S, strides: &S) -> usize {
        if shape.is_contiguous(strides) {
            self.0
        } else {
            let mut idx = 0;
            let mut remaining = self.0;
            let counts = shape.slice().iter().rev().scan(1_usize, |total, &dim| {
                let out = *total;
                *total *= dim;
                Some(out)
            });
            let iter = counts.zip(strides.slice());
            for (count, &stride) in iter {
                let take = remaining / count;
                remaining -= take;
                idx += take * stride;
                if remaining == 0 {
                    break;
                }
            }
            idx
        }
    }
}

impl<I> Indexer<Const<1>> for I
where
    I: IndexValue,
{
    #[inline]
    fn index_checked(&self, shape: &Const<1>, strides: &Const<1>) -> Option<usize> {
        [*self].index_checked(shape, strides)
    }

    #[inline]
    fn index_unchecked(&self, shape: &Const<1>, strides: &Const<1>) -> usize {
        [*self].index_unchecked(shape, strides)
    }
}

impl<S> Indexer<S> for S
where
    S: Shape,
{
    #[inline]
    fn index_checked(&self, shape: &S, strides: &S) -> Option<usize> {
        shape.stride_offset_checked(self, strides)
    }

    #[inline]
    fn index_unchecked(&self, _shape: &S, strides: &S) -> usize {
        S::stride_offset(self, strides)
    }
}

macro_rules! impl_indexer_array {
    ($([$n:expr, $($index:tt)*])+) => {
        $(
        #[allow(unused_variables)]
        impl<I> Indexer<Const<$n>> for [I; $n]
            where I: IndexValue,
        {
            #[inline]
            fn index_checked(&self, shape: &Const<$n>, strides: &Const<$n>) -> Option<usize> {
                if self.len() != shape.ndim() {
                    return None
                }
                Some($(offset_checked(shape[$index], strides[$index], self[$index].abs_index(shape[$index]))? + )* 0)
            }

            #[inline]
            fn index_unchecked(&self, shape: &Const<$n>, strides: &Const<$n>) -> usize {
                $(self[$index].abs_index(shape[$index]) * strides[$index] + )* 0
            }
        }

        #[allow(unused_variables)]
        impl<I> Indexer<Dyn> for [I; $n]
            where I: IndexValue,
        {
            #[inline]
            fn index_checked(&self, shape: &Dyn, strides: &Dyn) -> Option<usize> {
                debug_assert_eq!(strides.ndim(), $n, "index {:?} doesn't match array with {} dimensions", self, strides.ndim());
                if self.len() != shape.ndim() {
                    return None
                }
                Some($(offset_checked(shape[$index], strides[$index], self[$index].abs_index(shape[$index]))? + )* 0)
            }

            #[inline]
            fn index_unchecked(&self, shape: &Dyn, strides: &Dyn) -> usize {
                debug_assert_eq!(strides.ndim(), $n, "index {:?} doesn't match array with {} dimensions", self, strides.ndim());
                $(self[$index].abs_index(shape[$index]) * strides[$index] + )* 0
            }
        }
        )+
    };
}

impl_indexer_array!(
    [0, ]
    [1, 0]
    [2, 0 1]
    [3, 0 1 2]
    [4, 0 1 2 3]
    [5, 0 1 2 3 4]
    [6, 0 1 2 3 4 5]
);

macro_rules! impl_tuple_index {
    ($([$n:literal, $($t:ident),*])+) => {
        $(
        #[allow(non_snake_case)]
        impl<$($t),*> Indexer<Const<$n>> for ($($t, )*)
            where $($t: IndexValue),*
        {
            #[inline]
            fn index_checked(&self, shape: &Const<$n>, strides: &Const<$n>) -> Option<usize> {
                let ($($t, )*) = *self;
                <[isize; $n] as Indexer<Const<$n>>>::index_checked(&[$($t.index(), )*], shape, strides)
            }
            #[inline]
            fn index_unchecked(&self, shape: &Const<$n>, strides: &Const<$n>) -> usize {
                let ($($t, )*) = *self;
                <[isize; $n] as Indexer<Const<$n>>>::index_unchecked(&[$($t.index(), )*], shape, strides)
            }
        }

        #[allow(non_snake_case)]
        impl<$($t),*> Indexer<Dyn> for ($($t, )*)
            where $($t: IndexValue),*
        {
            #[inline]
            fn index_checked(&self, shape: &Dyn, strides: &Dyn) -> Option<usize> {
                let ($($t, )*) = *self;
                <[isize; $n] as Indexer<Dyn>>::index_checked(&[$($t.index(), )*], shape, strides)
            }
            #[inline]
            fn index_unchecked(&self, shape: &Dyn, strides: &Dyn) -> usize {
                let ($($t, )*) = *self;
                <[isize; $n] as Indexer<Dyn>>::index_unchecked(&[$($t.index(), )*], shape, strides)
            }
        }
        )+
    };
}

impl_tuple_index!(
    [0, ]
    [1, A]
    [2, A, B]
    [3, A, B, C]
    [4, A, B, C, D]
    [5, A, B, C, D, E]
    [6, A, B, C, D, E, F]
);

pub trait IndexValue: Copy + Debug {
    fn index(self) -> isize;
    fn abs_index(self, len: usize) -> usize;
}

macro_rules! impl_index_value {
    (signed [$($t:ty)+]) => {
        $(
        impl IndexValue for $t {
            #[inline(always)]
            fn index(self) -> isize {
                self as isize
            }
            #[inline]
            fn abs_index(self, len: usize) -> usize {
                if self < 0 {
                    len - (-self as usize)
                } else {
                    self as usize
                }
            }
        }
        )+
    };
    (unsigned [$($t:ty)+]) => {
        $(
        impl IndexValue for $t {
            #[inline(always)]
            fn index(self) -> isize {
                self as isize
            }
            #[inline(always)]
            fn abs_index(self, _len: usize) -> usize {
                self as usize
            }
        }
        )+
    }
}

impl_index_value!(signed   [i32 i64 isize]);
impl_index_value!(unsigned [u32 u64 usize]);

// pub trait IndexMap<In: Shape, Out: Shape> {
//     type Index: Indexer<Out>;

//     fn map_index<I>(&self, i: I) -> Self::Index
//         where I: Indexer<In>;
// }

// impl IndexMap<Const<1>

// pub trait IntoIndexIter<S: Shape> {
//     type Index: Indexer<S>;
//     type Iter: Iterator<Item = Self::Index>;

//     fn into_index_iter(self) -> Self::Iter;
// }

// impl<I, S> IntoIndexIter<S> for I
// where
//     I: IntoIterator,
//     S: Shape,
//     I::Item: Indexer<S>,
// {
//     type Index = <Self as IntoIterator>::Item;
//     type Iter = <Self as IntoIterator>::IntoIter;

//     fn into_index_iter(self) -> Self::Iter {
//         self.into_iter()
//     }
// }

// impl<T, S> IntoIndexIter<Const<1>> for crate::Tensor<T, S>
//     where T: SynapseIntType,
//           S: Shape,
// {
//     type Index = T;
//     type Iter = TensorValueIter<T, S, T::Array>;

//     fn into_index_iter(self) -> Self::Iter {
//         self.iter()
//     }
// }

// pub struct TensorIndexIter<T: SynapseIntType, S> {
//     inner: TensorValueIter<T, S, T::Array>,

// }

// impl<T, S> Iterator for TensorIndexIter<T, S>
//     where T: SynapseIntType,
//           S: Shape,
// {
//     type Item = ;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

// pub trait IndexList<S: Shape> {
//     type Index: Indexer<S>;

//     fn num_indices(&self) -> usize;
//     fn index(&self, i: usize) -> Self::Index;
// }

// impl<I, S> IndexList<S> for I
//     where S: Shape,
//           I: Indexer<S> + Clone,
// {
//     type Index = Self;

//     fn num_indices(&self) -> usize {
//         1
//     }

//     fn index(&self, i: usize) -> Self::Index {
//         self.clone()
//     }
// }

// impl<I, S> IndexList<S> for I
//     where S: Shape,
//           I:

// impl IndexValue for usize {
//     #[inline(always)]
//     fn index(self) -> isize {
//         self as isize
//     }
//     #[inline(always)]
//     fn abs_index(self, _len: usize) -> usize {
//         self
//     }
// }

pub(crate) fn offset_checked(dim: usize, stride: usize, index: usize) -> Option<usize> {
    if index >= dim {
        None
    } else {
        Some(index * stride)
    }
}
