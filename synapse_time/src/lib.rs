pub use ::time::{Duration, OffsetDateTime};
use time::format_description::well_known::Rfc3339;

use std::{
    fmt::Display,
    ops::{Add, AddAssign, Sub, SubAssign},
};

#[inline]
pub fn now() -> Time {
    Time::now()
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::Into,
)]
pub struct Time(time::OffsetDateTime);

impl Time {
    #[inline]
    pub fn now() -> Self {
        Self(time::OffsetDateTime::now_utc())
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.0.unix_timestamp_nanos() as i64
    }

    #[inline]
    pub fn from_timestamp(t: i64) -> Self {
        Self(time::OffsetDateTime::from_unix_timestamp_nanos(t.into()).unwrap())
    }
}

impl Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.format(&Rfc3339).map_err(|_| std::fmt::Error)?)
    }
}

impl Add<Duration> for Time {
    type Output = Time;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Sub<Duration> for Time {
    type Output = Time;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl AddAssign<Duration> for Time {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

impl SubAssign<Duration> for Time {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 -= rhs;
    }
}
