mod column;

pub use self::column::PyColumn;

use arrow::pyarrow::{PyArrowType, ToPyArrow};
use ella::tensor::{DataFrame, Frame};
use pyo3::{
    exceptions::{PyIndexError, PyKeyError, PyTypeError},
    prelude::*,
    types::PyDict,
};

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    inner: DataFrame,
}

/// A collection of rows from multiple columns.
#[pymethods]
impl PyDataFrame {
    fn __getitem__(&self, py: Python, key: PyObject) -> PyResult<PyColumn> {
        if let Ok(key) = key.extract::<&str>(py) {
            self.col(key)
        } else if let Ok(idx) = key.extract::<usize>(py) {
            self.icol(idx)
        } else {
            Err(PyTypeError::new_err(
                "DataFrame can only be indexed by string or integer",
            ))
        }
    }

    /// Convert the dataframe to an Arrow table.
    fn to_arrow(&self, py: Python) -> PyResult<PyObject> {
        let mut py_arrays = vec![];
        for col in self.columns() {
            py_arrays.push(col.to_arrow(py)?);
        }

        let py_schema = self.inner.arrow_schema().to_pyarrow(py)?;

        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", py_schema)?;
        Ok(py
            .import("pyarrow")?
            .getattr("Table")?
            .call_method("from_arrays", (py_arrays,), Some(kwargs))?
            .to_object(py))
    }

    /// Get the dataframe's equivalent arrow schema.
    fn arrow_schema(&self, py: Python) -> PyObject {
        PyArrowType(self.inner.arrow_schema()).into_py(py)
    }

    /// Get a column by index.
    fn icol(&self, i: usize) -> PyResult<PyColumn> {
        if i < self.inner.ncols() {
            Ok(self.inner.column(i).clone().into_inner().into())
        } else {
            Err(PyIndexError::new_err(format!(
                "column index {i} out of bounds"
            )))
        }
    }

    /// Get a column by name.
    fn col(&self, name: &str) -> PyResult<PyColumn> {
        for col in self.inner.columns() {
            if col.name() == name {
                return Ok(col.clone().into_inner().into());
            }
        }
        Err(PyKeyError::new_err(format!("column \"{name}\" not found")))
    }

    /// Get a list of all columns in the dataframe.
    fn columns(&self) -> Vec<PyColumn> {
        self.inner
            .columns()
            .map(|col| col.clone().into_inner().into())
            .collect()
    }
}
