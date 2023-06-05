use pyo3::PyErr;

impl From<crate::Error> for PyErr {
    fn from(err: crate::Error) -> Self {
        use crate::Error::*;

        match err {
            Tensor(err) => err.into(),
            #[cfg(feature = "runtime")]
            Runtime(err) => err.into(),
        }
    }
}
