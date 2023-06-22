use crate::{Const, Dyn, Shape};

pub trait NdimAdd<Other: Shape> {
    type Output: Shape;
}

impl<S: Shape> NdimAdd<S> for Const<0> {
    type Output = S;
}

macro_rules! impl_add_const {
    ($([$($t:tt)*])+) => {
        $(
            impl_add_const!($($t)*);
        )+
    };
    ($lhs:literal) => {
        impl_add_const!(Const<$lhs>, Dyn, Dyn);
    };
    ($lhs:literal, $rhs:literal) => {
        impl_add_const!(Const<$lhs>, Const<$rhs>, Const<{$lhs + $rhs}>);
    };
    ($lhs:literal, $rhs:literal, $out:ty) => {
        impl_add_const!(Const<$lhs>, Const<$rhs>, $out);
    };
    ($lhs:ty, $rhs:ty, $out:ty) => {
        impl NdimAdd<$rhs> for $lhs {
            type Output = $out;
        }
    };
}

impl_add_const!(
    [1, 0]
    [1, 1]
    [1, 2]
    [1, 3]
    [1, 4]
    [1, 5]
    [1, 6, Dyn]
    [1]

    [2, 0]
    [2, 1]
    [2, 2]
    [2, 3]
    [2, 4]
    [2, 5, Dyn]
    [2, 6, Dyn]
    [2]

    [3, 0]
    [3, 1]
    [3, 2]
    [3, 3]
    [3, 4, Dyn]
    [3, 5, Dyn]
    [3, 6, Dyn]
    [3]

    [4, 0]
    [4, 1]
    [4, 2]
    [4, 3, Dyn]
    [4, 4, Dyn]
    [4, 5, Dyn]
    [4, 6, Dyn]
    [4]

    [5, 0]
    [5, 1]
    [5, 2, Dyn]
    [5, 3, Dyn]
    [5, 4, Dyn]
    [5, 5, Dyn]
    [5, 6, Dyn]
    [5]

    [6, 0]
    [6, 1, Dyn]
    [6, 2, Dyn]
    [6, 3, Dyn]
    [6, 4, Dyn]
    [6, 5, Dyn]
    [6, 6, Dyn]
    [6]
);

impl<S: Shape> NdimAdd<S> for Dyn {
    type Output = Dyn;
}
