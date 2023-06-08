use std::{
    marker::PhantomData,
    ops::{Range, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive},
};

use crate::{
    shape::{stride_offset, IndexValue},
    Dyn, Shape,
};

pub trait Slicer<S: Shape>: AsRef<[AxisSliceSpec]> {
    type Shape: Shape;

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
    _in: PhantomData<fn() -> In>,
    _out: PhantomData<fn() -> Out>,
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
}

// impl<Out, const D: usize> Slicer<Const<D>> for SliceSpec<[AxisSliceSpec; D], Const<D>, Out>
//     where Out: Shape,
//           Const<D>: Shape,
// {
//     type Shape = Out;

//     fn in_ndim(&self) -> usize {
//         self.in_ndim()
//     }

//     fn out_ndim(&self) -> usize {
//         self.out_ndim()
//     }
// }

impl<T, Out> Slicer<Dyn> for SliceSpec<T, Dyn, Out>
where
    Out: Shape,
    T: AsRef<[AxisSliceSpec]>,
{
    type Shape = Out;

    fn in_ndim(&self) -> usize {
        self.in_ndim()
    }

    fn out_ndim(&self) -> usize {
        self.out_ndim()
    }
}

// impl<T, Out> Slicer<Const<2>> for SliceSpec<T, Const<2>, Out>
// where
//     T: AsRef<[AxisSliceSpec]>,
//     Out: Shape,
// {
//     type Shape = Out;

//     fn in_ndim(&self) -> usize {
//         self.in_ndim()
//     }

//     fn out_ndim(&self) -> usize {
//         self.out_ndim()
//     }
// }

#[derive(Debug, Clone, Copy)]
pub struct NewAxis;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slice {
    pub start: isize,
    pub end: Option<isize>,
    pub step: isize,
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

impl From<isize> for AxisSliceSpec {
    fn from(value: isize) -> Self {
        Self::Index(value)
    }
}

impl From<usize> for AxisSliceSpec {
    fn from(value: usize) -> Self {
        Self::Index(value as isize)
    }
}

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

pub trait SliceStep {
    fn step(self, step: isize) -> Slice;
}

impl<S> SliceStep for S
where
    S: Into<Slice>,
{
    fn step(self, step: isize) -> Slice {
        let mut slice: Slice = self.into();
        slice.step = step;
        slice
    }
}

// trait SliceNextDim {
//     type
// }

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

macro_rules! slice {
    () => {};
}
