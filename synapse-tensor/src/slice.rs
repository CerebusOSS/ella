use std::{
    marker::PhantomData,
    ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive},
};

use crate::{
    shape::{stride_offset, IndexValue, NdimAdd},
    Const, Shape,
};

pub trait SliceShape<In: Shape>: AsRef<[AxisSliceSpec]> {
    type Out: Shape;

    fn in_ndim(&self) -> usize;
    fn out_ndim(&self) -> usize;
    fn slice(&self) -> &[AxisSliceSpec] {
        self.as_ref()
    }
}

#[derive(Debug, Clone, derive_more::AsRef)]
pub struct SliceSpec<T: AsRef<[AxisSliceSpec]>, In, Out> {
    #[as_ref(forward)]
    args: T,
    ndim_in: PhantomData<In>,
    ndim_out: PhantomData<Out>,
}

impl<T, In, Out> SliceSpec<T, In, Out>
where
    T: AsRef<[AxisSliceSpec]>,
    In: Shape,
    Out: Shape,
{
    fn in_ndim(&self) -> usize {
        if let Some(ndim) = In::NDIM {
            ndim
        } else {
            self.args
                .as_ref()
                .iter()
                .map(|ax| ax.in_ndim())
                .sum::<usize>()
        }
    }

    fn out_ndim(&self) -> usize {
        if let Some(ndim) = Out::NDIM {
            ndim
        } else {
            self.args
                .as_ref()
                .iter()
                .map(|ax| ax.out_ndim())
                .sum::<usize>()
        }
    }

    #[doc(hidden)]
    pub unsafe fn new_unchecked(
        args: T,
        ndim_in: PhantomData<In>,
        ndim_out: PhantomData<Out>,
    ) -> Self {
        Self {
            args,
            ndim_in,
            ndim_out,
        }
    }
}

impl<T, In, Out> SliceShape<In> for SliceSpec<T, In, Out>
where
    T: AsRef<[AxisSliceSpec]>,
    In: Shape,
    Out: Shape,
{
    type Out = Out;

    fn in_ndim(&self) -> usize {
        self.in_ndim()
    }

    fn out_ndim(&self) -> usize {
        self.out_ndim()
    }
}

impl<T, S> SliceShape<S> for &T
where
    T: SliceShape<S> + ?Sized,
    S: Shape,
{
    type Out = T::Out;

    fn in_ndim(&self) -> usize {
        (*self).in_ndim()
    }

    fn out_ndim(&self) -> usize {
        (*self).out_ndim()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NewAxis;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slice {
    pub start: isize,
    pub end: Option<isize>,
    pub step: isize,
}

impl Slice {
    #[inline]
    pub fn step_by(mut self, step: isize) -> Self {
        debug_assert_ne!(step, 0, "slice step must be non-zero");
        self.step *= step;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AxisSliceSpec {
    NewAxis,
    Index(isize),
    Slice(Slice),
}

impl AxisSliceSpec {
    fn in_ndim(&self) -> usize {
        match self {
            AxisSliceSpec::NewAxis => 0,
            _ => 1,
        }
    }

    fn out_ndim(&self) -> usize {
        match self {
            AxisSliceSpec::Index(_) => 0,
            _ => 1,
        }
    }
}

impl From<NewAxis> for AxisSliceSpec {
    fn from(_value: NewAxis) -> Self {
        Self::NewAxis
    }
}

macro_rules! impl_from_index {
    ($($t:ty),+) => {
        $(
            impl From<$t> for AxisSliceSpec {
                fn from(value: $t) -> Self {
                    Self::Index(value as isize)
                }
            }
        )+
    };
}

impl_from_index!(usize, isize, i32, i64, u32, u64);

impl<S> From<S> for AxisSliceSpec
where
    S: Into<Slice>,
{
    fn from(value: S) -> Self {
        Self::Slice(value.into())
    }
}

macro_rules! impl_slice {
    ($($t:ty),+) => {
        $(
        impl From<Range<$t>> for Slice {
            fn from(value: Range<$t>) -> Self {
                Self {
                    start: value.start as isize,
                    end: Some(value.end as isize),
                    step: 1,
                }
            }
        }
        impl From<RangeInclusive<$t>> for Slice {
            fn from(value: RangeInclusive<$t>) -> Self {
                let end = *value.end() as isize;
                Self {
                    start: *value.start() as isize,
                    end: if end == -1 { None } else { Some(end + 1) },
                    step: 1,
                }
            }
        }
        impl From<RangeFrom<$t>> for Slice {
            fn from(value: RangeFrom<$t>) -> Self {
                Self {
                    start: value.start as isize,
                    end: None,
                    step: 1,
                }
            }
        }
        impl From<RangeTo<$t>> for Slice {
            fn from(value: RangeTo<$t>) -> Self {
                Self {
                    start: 0,
                    end: Some(value.end as isize),
                    step: 1,
                }
            }
        }
        impl From<RangeToInclusive<$t>> for Slice {
            fn from(value: RangeToInclusive<$t>) -> Self {
                let end = value.end as isize;
                Self {
                    start: 0,
                    end: if end == -1 { None } else { Some(end + 1) },
                    step: 1,
                }
            }
        }
        )+
    };
}

impl_slice!(usize, isize, i32);

impl From<RangeFull> for Slice {
    fn from(_: RangeFull) -> Self {
        Self {
            start: 0,
            end: None,
            step: 1,
        }
    }
}

pub(crate) fn do_slice(axis: &mut usize, stride: &mut usize, slice: Slice) -> isize {
    let (start, end, step) = to_abs_slice(*axis, slice);

    let m = end - start;
    let s = (*stride) as isize;
    let offset = if m == 0 {
        0
    } else if step < 0 {
        stride_offset(end - 1, *stride)
    } else {
        stride_offset(start, *stride)
    };

    let abs_step = step.abs() as usize;
    *axis = if abs_step == 1 {
        m
    } else {
        let d = m / abs_step;
        let r = m % abs_step;
        d + if r > 0 { 1 } else { 0 }
    };
    *stride = if *axis <= 1 { 0 } else { (s * step) as usize };

    offset
}

fn to_abs_slice(axis_len: usize, slice: Slice) -> (usize, usize, isize) {
    let Slice { start, end, step } = slice;
    let start = start.abs_index(axis_len);
    let mut end = end.unwrap_or(axis_len as isize).abs_index(axis_len);
    if end < start {
        end = start;
    }
    assert!(start <= axis_len, "slice start index > axis length");
    assert!(end <= axis_len, "slice end index > axis length");
    assert_ne!(step, 0, "slice step must not be zero");
    (start, end, step)
}

#[macro_export]
macro_rules! slice {
    (@parse $in:expr, $out:expr, [$($stack:tt)*] $r:expr;$s:expr $(,)?) => {
        #[allow(unsafe_code)]
        unsafe {
            $crate::slice::SliceSpec::new_unchecked(
                [$($stack)* $crate::slice!(@convert $r, $s)],
                $crate::slice::SliceNextShape::next_in_shape(&$r, $in),
                $crate::slice::SliceNextShape::next_out_shape(&$r, $out),
            )
        }
    };
    (@parse $in:expr, $out:expr, [$($stack:tt)*] $r:expr $(,)?) => {
        #[allow(unsafe_code)]
        unsafe {
            $crate::slice::SliceSpec::new_unchecked(
                [$($stack)* $crate::slice!(@convert $r)],
                $crate::slice::SliceNextShape::next_in_shape(&$r, $in),
                $crate::slice::SliceNextShape::next_out_shape(&$r, $out),
            )
        }
    };
    (@parse $in:expr, $out:expr, [$($stack:tt)*] $r:expr;$s:expr, $($t:tt)+) => {
        $crate::slice![@parse
            $crate::slice::SliceNextShape::next_in_shape(&$r, $in),
            $crate::slice::SliceNextShape::next_out_shape(&$r, $out),
            [$($stack)* $crate::slice!(@convert $r, $s),]
            $($t)+
        ]
    };
    (@parse $in:expr, $out:expr, [$($stack:tt)*] $r:expr, $($t:tt)+) => {
        $crate::slice![@parse
            $crate::slice::SliceNextShape::next_in_shape(&$r, $in),
            $crate::slice::SliceNextShape::next_out_shape(&$r, $out),
            [$($stack)* $crate::slice!(@convert $r),]
            $($t)+
        ]
    };
    (@parse ::core::marker::PhantomData::<$crate::Const<0>>, ::core::marker::PhantomData::<$crate::Const<0>>, []) => {
        #[allow(unsafe_code)]
        unsafe {
            $crate::slice::SliceSpec::new_unchecked(
                [],
                ::core::marker::PhantomData::<$crate::Const<0>>,
                ::core::marker::PhantomData::<$crate::Const<0>>,
            )
        }
    };
    (@parse $($t:tt)*) => { compile_error!("invalid slice syntax") };
    (@convert $r:expr) => {
        <$crate::slice::AxisSliceSpec as ::core::convert::From<_>>::from($r)
    };
    (@convert $r:expr, $s:expr) => {
        <$crate::slice::AxisSliceSpec as ::core::convert::From<_>>::from(
            <$crate::slice::Slice as ::core::convert::From<_>>::from($r).step_by($s)
        )
    };
    ($($t:tt)*) => {
        $crate::slice![@parse
            ::core::marker::PhantomData::<$crate::Const<0>>,
            ::core::marker::PhantomData::<$crate::Const<0>>,
            []
            $($t)*
        ]
    };
}

#[doc(hidden)]
pub trait SliceNextShape {
    type In: Shape;
    type Out: Shape;

    fn next_in_shape<S>(&self, _: PhantomData<S>) -> PhantomData<<S as NdimAdd<Self::In>>::Output>
    where
        S: Shape + NdimAdd<Self::In>,
    {
        PhantomData
    }

    fn next_out_shape<S>(&self, _: PhantomData<S>) -> PhantomData<<S as NdimAdd<Self::Out>>::Output>
    where
        S: Shape + NdimAdd<Self::Out>,
    {
        PhantomData
    }
}

macro_rules! impl_slicenextshape {
    (($($generics:tt)*), $self:ty, $in:ty, $out:ty) => {
        impl<$($generics)*> SliceNextShape for $self {
            type In = $in;
            type Out = $out;
        }
    };
}

impl_slicenextshape!((), isize, Const<1>, Const<0>);
impl_slicenextshape!((), usize, Const<1>, Const<0>);
impl_slicenextshape!((), i32, Const<1>, Const<0>);
impl_slicenextshape!((T), Range<T>, Const<1>, Const<1>);
impl_slicenextshape!((T), RangeInclusive<T>, Const<1>, Const<1>);
impl_slicenextshape!((T), RangeFrom<T>, Const<1>, Const<1>);
impl_slicenextshape!((T), RangeTo<T>, Const<1>, Const<1>);
impl_slicenextshape!((T), RangeToInclusive<T>, Const<1>, Const<1>);
impl_slicenextshape!((), RangeFull, Const<1>, Const<1>);
impl_slicenextshape!((), NewAxis, Const<0>, Const<1>);
