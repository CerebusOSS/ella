use pyo3::{exceptions::*, PyErr};

impl From<crate::Error> for PyErr {
    fn from(err: crate::Error) -> Self {
        use crate::Error::*;

        match err {
            Tensor(err) => err.into(),
            DataFusion(err) => err.into(),
            Io(err) => PyIOError::new_err(err.to_string()),
            UnexpectedDirectory(_) => PyIsADirectoryError::new_err(err.to_string()),
            InvalidFilename(_) => PyOSError::new_err(err.to_string()),
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}
