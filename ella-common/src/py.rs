use pyo3::{exceptions::*, PyErr};

impl From<crate::Error> for PyErr {
    fn from(err: crate::Error) -> Self {
        use crate::error::EngineError::*;
        use crate::Error::*;

        match err {
            DataType(_) | Cast { .. } => PyTypeError::new_err(err.to_string()),
            Shape(_) => PyValueError::new_err(err.to_string()),
            ColumnLookup(_) => PyLookupError::new_err(err.to_string()),
            UnknownExtension(_) | MissingMetadata(_) => PyIOError::new_err(err.to_string()),
            DataFusion(err) => err.into(),
            Io(err) => PyIOError::new_err(err.to_string()),
            Engine(UnexpectedDirectory(_)) => PyIsADirectoryError::new_err(err.to_string()),
            Engine(InvalidFilename(_)) => PyOSError::new_err(err.to_string()),
            _ => PyRuntimeError::new_err(err.to_string()),
        }
    }
}
