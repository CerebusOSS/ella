mod add;
mod index;
mod iter;
mod max;
pub use add::NdimAdd;
pub use index::{Flat, IndexValue, Indexer};
pub use iter::ShapeIndexIter;
pub use max::NdimMax;

pub use index::IndexUnchecked;

use smallvec::SmallVec;
use std::{fmt::Debug, ops::IndexMut};

use crate::error::ShapeError;

pub trait Shape:
    Debug
    + Send
    + Sync
    + Clone
    + Eq
    + PartialEq
    + IndexMut<usize, Output = usize>
    + AsRef<[usize]>
    + AsMut<[usize]>
    + 'static
{
    const NDIM: Option<usize>;
    type Smaller: Shape;
    type Larger: Shape + RemoveAxis;

    fn ndim(&self) -> usize;
    fn size(&self) -> usize;
    fn zeros(ndim: usize) -> Self;

    fn slice(&self) -> &[usize] {
        self.as_ref()
    }

    fn slice_mut(&mut self) -> &mut [usize] {
        self.as_mut()
    }

    fn as_dyn(&self) -> Dyn {
        self.slice().into()
    }

    fn broadcast<In, Out>(&self, other: &In) -> crate::Result<Out>
    where
        In: Shape,
        Out: Shape,
    {
        let (k, overflow) = self.ndim().overflowing_sub(other.ndim());
        if overflow {
            return other.broadcast(self);
        }
        let mut dst = Out::zeros(self.ndim());
        for (out, s) in dst.slice_mut().iter_mut().zip(self.slice()) {
            *out = *s;
        }
        for (out, s) in (dst.slice_mut()[k..]).iter_mut().zip(other.slice()) {
            if *out != *s {
                if *out == 1 {
                    *out = *s;
                } else if *s != 1 {
                    return Err(ShapeError::broadcast(self.as_ref(), other.as_ref()).into());
                    // return Err(crate::Error::Broadcast(self.as_dyn(), other.as_dyn()));
                }
            }
        }
        Ok(dst)
    }

    fn indices(&self) -> ShapeIndexIter<Self> {
        ShapeIndexIter::new(self.clone())
    }

    fn axis(&self, axis: Axis) -> usize {
        self[axis.index(self)]
    }

    #[doc(hidden)]
    fn default_strides(&self) -> Self {
        let mut strides = Self::zeros(self.ndim());
        if self.as_ref().iter().all(|&d| d != 0) {
            let mut it = strides.as_mut().iter_mut().rev();
            if let Some(s) = it.next() {
                *s = 1;
            }
            let mut prod = 1;
            for (s, d) in it.zip(self.as_ref().iter().rev()) {
                prod *= *d;
                *s = prod;
            }
        }
        strides
    }

    #[doc(hidden)]
    fn insert_axis(&self, axis: Axis) -> Self::Larger {
        let axis = axis.insert_index(self);
        debug_assert!(axis <= self.ndim());

        let mut out = <Self::Larger as Shape>::zeros(self.ndim() + 1);
        let dst = out.as_mut();
        let src = self.as_ref();
        dst[0..axis].copy_from_slice(&src[0..axis]);
        dst[axis] = 1;
        dst[axis + 1..=self.ndim()].copy_from_slice(&src[axis..self.ndim()]);
        out
    }

    #[doc(hidden)]
    fn stride_offset(index: &Self, strides: &Self) -> isize {
        let mut offset = 0;
        for (&i, &s) in index.slice().iter().zip(strides.slice()) {
            offset += stride_offset(i, s);
        }
        offset
    }

    #[doc(hidden)]
    fn stride_offset_checked(&self, index: &Self, strides: &Self) -> Option<isize> {
        let mut offset = 0;
        let iter = self.slice().iter().zip(index.slice()).zip(strides.slice());
        for ((&dim, &i), &s) in iter {
            offset += stride_offset_checked(dim, s, i)?;
        }
        Some(offset)
    }

    #[doc(hidden)]
    fn is_standard_layout(&self, strides: &Self) -> bool {
        if let Some(1) = Self::NDIM {
            return strides[0] == 1 || self[0] <= 1;
        }

        for &sz in self.slice() {
            if sz == 0 {
                return true;
            }
        }

        let mut cstride = 1;
        for (&ax, &stride) in self.slice().iter().zip(strides.slice()).rev() {
            if ax != 1 {
                if stride as isize != cstride {
                    return false;
                }
                cstride *= ax as isize;
            }
        }
        true
    }

    #[doc(hidden)]
    fn from_shape<S: Shape>(shape: &S) -> crate::Result<Self> {
        let mut s = Self::zeros(shape.ndim());
        if s.ndim() == shape.ndim() {
            for i in 0..shape.ndim() {
                s[i] = shape[i];
            }
            Ok(s)
        } else {
            Err(ShapeError::ndim(shape.ndim(), s.ndim()).into())
        }
    }

    #[doc(hidden)]
    fn strided_size(&self, strides: &Self) -> usize {
        if self.ndim() < 1 {
            1
        } else {
            self[0] * strides[0]
        }
    }

    #[doc(hidden)]
    fn last_elem(&self) -> usize {
        if self.ndim() == 0 {
            0
        } else {
            self.slice()[self.ndim() - 1]
        }
    }

    #[doc(hidden)]
    fn set_last_elem(&mut self, i: usize) {
        let n = self.ndim();
        self.slice_mut()[n - 1] = i;
    }
}

pub trait RemoveAxis: Shape {
    fn remove_axis(&self, axis: Axis) -> Self::Smaller;
}

impl RemoveAxis for Const<1> {
    #[inline]
    fn remove_axis(&self, axis: Axis) -> Self::Smaller {
        debug_assert!(axis.index(self) < self.ndim());
        Const::default()
    }
}

impl RemoveAxis for Dyn {
    #[inline]
    fn remove_axis(&self, axis: Axis) -> Self::Smaller {
        debug_assert!(axis.index(self) < self.ndim());
        let mut out = self.clone();
        out.0.remove(axis.index(self));
        out
    }
}

macro_rules! impl_remove_axis {
    ($($n:expr),*) => {
        $(
        impl RemoveAxis for Const<$n> {
            #[inline]
            fn remove_axis(&self, axis: Axis) -> Self::Smaller {
                let axis = axis.index(self);
                debug_assert!(axis < self.ndim());

                let mut result = Const([0; $n - 1]);
                let src = self.as_ref();
                let dst = result.as_mut();
                dst[..axis].copy_from_slice(&src[..axis]);
                dst[axis..].copy_from_slice(&src[axis + 1..]);
                result
            }
        }
        )*
    };
}

impl_remove_axis!(2, 3, 4, 5, 6);

#[derive(Debug, Clone, Copy, derive_more::From)]
pub struct Axis(pub isize);

impl From<usize> for Axis {
    fn from(value: usize) -> Self {
        Self(value as isize)
    }
}

impl From<i32> for Axis {
    fn from(value: i32) -> Self {
        Self(value as isize)
    }
}

impl Axis {
    #[doc(hidden)]
    #[inline]
    pub fn index<S>(&self, shape: &S) -> usize
    where
        S: Shape,
    {
        self.0.abs_index(shape.ndim())
    }

    #[doc(hidden)]
    pub fn insert_index<S>(&self, shape: &S) -> usize
    where
        S: Shape,
    {
        let mut idx = self.0.abs_index(shape.ndim());
        if self.0 < 0 {
            idx += 1;
        }
        idx
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    derive_more::From,
    derive_more::Into,
    derive_more::IntoIterator,
    derive_more::Index,
    derive_more::IndexMut,
    derive_more::AsRef,
    derive_more::AsMut,
)]
#[as_ref(forward)]
#[as_mut(forward)]
#[into_iterator(owned, ref, ref_mut)]
pub struct Const<const D: usize>(pub [usize; D]);

impl Default for Const<0> {
    fn default() -> Self {
        Self([])
    }
}

macro_rules! index {
    ($m:ident $arg:tt 0) => ($m!($arg));
    ($m:ident $arg:tt 1) => ($m!($arg 0));
    ($m:ident $arg:tt 2) => ($m!($arg 0 1));
    ($m:ident $arg:tt 3) => ($m!($arg 0 1 2));
    ($m:ident $arg:tt 4) => ($m!($arg 0 1 2 3));
    ($m:ident $arg:tt 5) => ($m!($arg 0 1 2 3 4));
    ($m:ident $arg:tt 6) => ($m!($arg 0 1 2 3 4 5));
    ($m:ident $arg:tt 7) => ($m!($arg 0 1 2 3 4 5 6));
}

macro_rules! index_item {
    ($m:ident $arg:tt 0) => ();
    ($m:ident $arg:tt 1) => ($m!($arg 0););
    ($m:ident $arg:tt 2) => ($m!($arg 0 1););
    ($m:ident $arg:tt 3) => ($m!($arg 0 1 2););
    ($m:ident $arg:tt 4) => ($m!($arg 0 1 2 3););
    ($m:ident $arg:tt 5) => ($m!($arg 0 1 2 3 4););
    ($m:ident $arg:tt 6) => ($m!($arg 0 1 2 3 4 5););
    ($m:ident $arg:tt 7) => ($m!($arg 0 1 2 3 4 5 6););
}

macro_rules! sub {
    ($_x:tt $y:tt) => {
        $y
    };
}

macro_rules! tuple_type {
    ([$T:ident] $($index:tt)*) => {
        ( $(sub!($index $T), )* )
    };
}

macro_rules! array_expr {
    ([$self_:expr] $($index:tt)*) => {
        [$($self_ . $index, )*]
    };
}

macro_rules! larger {
    (6) => {
        Dyn
    };
    ($n:tt) => {
        Const<{$n + 1}>
    };
}

macro_rules! smaller {
    (0) => {
        Const<0>
    };
    ($n:tt) => {
        Const<{$n - 1}>
    };
}

macro_rules! impl_const {
    ([] $($n:tt)*) => {
        $(
        impl IntoShape for [usize; $n] {
            type Shape = Const<$n>;
            #[inline]
            fn into_shape(self) -> Self::Shape {
                Const(self)
            }
        }

        impl IntoShape for index!(tuple_type [usize] $n) {
            type Shape = Const<$n>;
            #[inline]
            fn into_shape(self) -> Self::Shape {
                Const(index!(array_expr [self] $n))
            }
        }

        impl Shape for Const<$n> {
            const NDIM: Option<usize> = Some($n);
            type Smaller = smaller!($n);
            type Larger = larger!($n);

            fn ndim(&self) -> usize {
                $n
            }

            fn size(&self) -> usize {
                self.0.iter().product::<usize>()
            }

            fn zeros(_ndim: usize) -> Self {
                Const([0; $n])
            }
        }
        )*
    };
}

index_item!(impl_const [] 7);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    derive_more::IntoIterator,
    derive_more::Index,
    derive_more::IndexMut,
    derive_more::AsRef,
    derive_more::AsMut,
    serde::Serialize,
    serde::Deserialize,
)]
#[as_ref(forward)]
#[as_mut(forward)]
#[into_iterator(owned, ref, ref_mut)]
pub struct Dyn(pub SmallVec<[usize; 4]>);

impl Dyn {
    pub fn to_vec(&self) -> Vec<usize> {
        self.0.to_vec()
    }
}

impl Shape for Dyn {
    const NDIM: Option<usize> = None;
    type Smaller = Self;
    type Larger = Self;

    fn ndim(&self) -> usize {
        self.0.len()
    }

    fn size(&self) -> usize {
        self.0.iter().product::<usize>()
    }

    fn zeros(ndim: usize) -> Self {
        Self(std::iter::repeat(0).take(ndim).collect())
    }
}

impl From<Vec<usize>> for Dyn {
    fn from(value: Vec<usize>) -> Self {
        Self(value.into())
    }
}

impl From<&[usize]> for Dyn {
    fn from(value: &[usize]) -> Self {
        Self(value.into())
    }
}

impl<const D: usize> From<[usize; D]> for Dyn {
    fn from(value: [usize; D]) -> Self {
        Self::from(&value[..])
    }
}

impl<const D: usize> From<Const<D>> for Dyn {
    fn from(value: Const<D>) -> Self {
        Dyn(value.0.as_slice().into())
    }
}

pub trait IntoShape: Debug + Clone {
    type Shape: Shape;

    fn into_shape(self) -> Self::Shape;
}

impl IntoShape for Vec<usize> {
    type Shape = Dyn;

    fn into_shape(self) -> Self::Shape {
        self.into()
    }
}

impl IntoShape for &[usize] {
    type Shape = Dyn;

    fn into_shape(self) -> Self::Shape {
        self.into()
    }
}

impl IntoShape for usize {
    type Shape = Const<1>;

    fn into_shape(self) -> Self::Shape {
        Const([self])
    }
}

impl<S> IntoShape for S
where
    S: Shape,
{
    type Shape = Self;

    fn into_shape(self) -> Self::Shape {
        self
    }
}

#[inline(always)]
pub fn stride_offset(index: usize, stride: usize) -> isize {
    (index as isize) * (stride as isize)
}

pub fn stride_offset_checked(dim: usize, stride: usize, index: usize) -> Option<isize> {
    if index >= dim {
        None
    } else {
        Some(stride_offset(index, stride))
    }
}
