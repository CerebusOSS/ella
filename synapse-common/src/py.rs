use pyo3::{exceptions::*, PyErr};

impl From<crate::Error> for PyErr {
    fn from(err: crate::Error) -> Self {
        use crate::error::EngineError::*;
        use crate::Error::*;

        match err {
            DataType(_) | Cast { .. } => PyTypeError::new_err(format!("{:?}", err)),
            Shape(_) => PyValueError::new_err(format!("{:?}", err)),
            ColumnLookup(_) => PyLookupError::new_err(format!("{:?}", err)),
            UnknownExtension(_) | MissingMetadata(_) => PyIOError::new_err(format!("{:?}", err)),
            DataFusion(err) => err.into(),
            Io(err) => PyIOError::new_err(format!("{:?}", err)),
            Engine(UnexpectedDirectory(_)) => PyIsADirectoryError::new_err(format!("{:?}", err)),
            Engine(InvalidFilename(_)) => PyOSError::new_err(format!("{:?}", err)),
            _ => PyRuntimeError::new_err(format!("{:?}", err)),
        }
    }
}
