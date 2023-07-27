use super::{Const, Dyn, Shape};

pub trait NdimMax<Other: Shape> {
    type Output: Shape;
}

impl<S: Shape> NdimMax<S> for S {
    type Output = S;
}

macro_rules! impl_ndim_max {
    ($([$smaller:ty, $larger:ty])+) => {
        $(
        impl NdimMax<$larger> for $smaller {
            type Output = $larger;
        }

        impl NdimMax<$smaller> for $larger {
            type Output = $larger;
        }
        )+
    };
}

impl_ndim_max!(
    [Const<0>, Const<1>]
    [Const<0>, Const<2>]
    [Const<0>, Const<3>]
    [Const<0>, Const<4>]
    [Const<0>, Const<5>]
    [Const<0>, Const<6>]
    [Const<1>, Const<2>]
    [Const<1>, Const<3>]
    [Const<1>, Const<4>]
    [Const<1>, Const<5>]
    [Const<1>, Const<6>]
    [Const<2>, Const<3>]
    [Const<2>, Const<4>]
    [Const<2>, Const<5>]
    [Const<2>, Const<6>]
    [Const<3>, Const<4>]
    [Const<3>, Const<5>]
    [Const<3>, Const<6>]
    [Const<4>, Const<5>]
    [Const<4>, Const<6>]
    [Const<5>, Const<6>]
    [Const<1>, Dyn]
    [Const<2>, Dyn]
    [Const<3>, Dyn]
    [Const<4>, Dyn]
    [Const<5>, Dyn]
    [Const<6>, Dyn]
);
