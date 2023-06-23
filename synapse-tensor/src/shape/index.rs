use super::{stride_offset, stride_offset_checked, Const, Dyn, Shape};
use std::fmt::Debug;

pub trait Indexer<S: Shape>: Debug {
    fn index_checked(&self, shape: &S, strides: &S) -> Option<isize>;
    fn index_unchecked(&self, shape: &S, strides: &S) -> isize;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Flat(pub usize);

impl<S> Indexer<S> for Flat
where
    S: Shape,
{
    fn index_checked(&self, shape: &S, strides: &S) -> Option<isize> {
        if self.0 < shape.size() {
            Some(self.index_unchecked(shape, strides))
        } else {
            None
        }
    }

    fn index_unchecked(&self, shape: &S, strides: &S) -> isize {
        if shape.is_standard_layout(strides) {
            self.0 as isize
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
                idx += stride_offset(take, stride);
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
    fn index_checked(&self, shape: &Const<1>, strides: &Const<1>) -> Option<isize> {
        [*self].index_checked(shape, strides)
    }

    #[inline]
    fn index_unchecked(&self, shape: &Const<1>, strides: &Const<1>) -> isize {
        [*self].index_unchecked(shape, strides)
    }
}

impl<S> Indexer<S> for S
where
    S: Shape,
{
    #[inline]
    fn index_checked(&self, shape: &S, strides: &S) -> Option<isize> {
        shape.stride_offset_checked(self, strides)
    }

    #[inline]
    fn index_unchecked(&self, _shape: &S, strides: &S) -> isize {
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
            fn index_checked(&self, shape: &Const<$n>, strides: &Const<$n>) -> Option<isize> {
                if self.len() != shape.ndim() {
                    return None
                }
                Some($(stride_offset_checked(shape[$index], strides[$index], self[$index].abs_index(shape[$index]))? + )* 0)
            }

            #[inline]
            fn index_unchecked(&self, shape: &Const<$n>, strides: &Const<$n>) -> isize {
                $(stride_offset(self[$index].abs_index(shape[$index]), strides[$index]) + )* 0
            }
        }

        #[allow(unused_variables)]
        impl<I> Indexer<Dyn> for [I; $n]
            where I: IndexValue,
        {
            #[inline]
            fn index_checked(&self, shape: &Dyn, strides: &Dyn) -> Option<isize> {
                debug_assert_eq!(strides.ndim(), $n, "index {:?} doesn't match array with {} dimensions", self, strides.ndim());
                if self.len() != shape.ndim() {
                    return None
                }
                Some($(stride_offset_checked(shape[$index], strides[$index], self[$index].abs_index(shape[$index]))? + )* 0)
            }

            #[inline]
            fn index_unchecked(&self, shape: &Dyn, strides: &Dyn) -> isize {
                debug_assert_eq!(strides.ndim(), $n, "index {:?} doesn't match array with {} dimensions", self, strides.ndim());
                $(stride_offset(self[$index].abs_index(shape[$index]), strides[$index]) + )* 0
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
            fn index_checked(&self, shape: &Const<$n>, strides: &Const<$n>) -> Option<isize> {
                let ($($t, )*) = *self;
                <[isize; $n] as Indexer<Const<$n>>>::index_checked(&[$($t.index(), )*], shape, strides)
            }
            #[inline]
            fn index_unchecked(&self, shape: &Const<$n>, strides: &Const<$n>) -> isize {
                let ($($t, )*) = *self;
                <[isize; $n] as Indexer<Const<$n>>>::index_unchecked(&[$($t.index(), )*], shape, strides)
            }
        }

        #[allow(non_snake_case)]
        impl<$($t),*> Indexer<Dyn> for ($($t, )*)
            where $($t: IndexValue),*
        {
            #[inline]
            fn index_checked(&self, shape: &Dyn, strides: &Dyn) -> Option<isize> {
                let ($($t, )*) = *self;
                <[isize; $n] as Indexer<Dyn>>::index_checked(&[$($t.index(), )*], shape, strides)
            }
            #[inline]
            fn index_unchecked(&self, shape: &Dyn, strides: &Dyn) -> isize {
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

impl<T: IndexValue> Indexer<Dyn> for &[T] {
    #[inline]
    fn index_checked(&self, shape: &Dyn, strides: &Dyn) -> Option<isize> {
        IndexUnchecked(self).index_checked(shape, strides)
    }

    #[inline]
    fn index_unchecked(&self, shape: &Dyn, strides: &Dyn) -> isize {
        IndexUnchecked(self).index_unchecked(shape, strides)
    }
}

impl<T: IndexValue> Indexer<Dyn> for Vec<T> {
    #[inline]
    fn index_checked(&self, shape: &Dyn, strides: &Dyn) -> Option<isize> {
        self.as_slice().index_checked(shape, strides)
    }

    #[inline]
    fn index_unchecked(&self, shape: &Dyn, strides: &Dyn) -> isize {
        self.as_slice().index_unchecked(shape, strides)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexUnchecked<'a, T>(pub &'a [T]);

impl<'a, T, S> Indexer<S> for IndexUnchecked<'a, T>
where
    T: IndexValue,
    S: Shape,
{
    fn index_checked(&self, shape: &S, strides: &S) -> Option<isize> {
        let mut offset = 0;
        let iter = shape.slice().iter().zip(self.0).zip(strides.slice());
        for ((&dim, &i), &s) in iter {
            let i = i.abs_index(dim);
            offset += stride_offset_checked(dim, s, i)?;
        }
        Some(offset)
    }

    fn index_unchecked(&self, shape: &S, strides: &S) -> isize {
        let mut offset = 0;
        let iter = shape.slice().iter().zip(self.0).zip(strides.slice());
        for ((&dim, &i), &s) in iter {
            let i = i.abs_index(dim);
            offset += stride_offset(i, s);
        }
        offset
    }
}
