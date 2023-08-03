use arrow::pyarrow::ToPyArrow;
use ella::tensor::ColumnRef;
use pyo3::{prelude::*, types::PyTuple};

use crate::data_types::wrap_dtype;

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "Column")]
pub struct PyColumn {
    inner: ColumnRef,
}

#[pymethods]
impl PyColumn {
    #[getter]
    pub fn shape(&self) -> Vec<usize> {
        self.inner.shape().to_vec()
    }

    #[getter]
    pub fn row_shape(&self) -> Option<Vec<usize>> {
        self.inner.row_shape().map(|row| row.to_vec())
    }

    #[getter]
    pub fn nullable(&self) -> bool {
        self.inner.nullable()
    }

    #[getter]
    pub fn dtype(&self, py: Python) -> PyObject {
        wrap_dtype(py, self.inner.tensor_type())
    }

    pub fn to_arrow(&self, py: Python) -> PyResult<PyObject> {
        let values = self.inner.to_arrow().to_data().to_pyarrow(py)?;

        // If array is a tensor we have to manually recreate the extension type on the Python side
        // Trying to directly convert the type results in a segfault.
        // Possibly caused by this issue: https://github.com/apache/arrow/issues/20385
        if let Some(row_shape) = self.row_shape() {
            let py_type = self.inner.tensor_type().to_arrow().to_pyarrow(py)?;
            let py_shape = row_shape.to_object(py);
            let args = PyTuple::new(py, &[py_type, py_shape]);
            let ext_type = py
                .import("pyarrow")?
                .getattr("fixed_shape_tensor")?
                .call1(args)?;
            Ok(ext_type
                .call_method1("wrap_array", (values,))?
                .to_object(py))
        } else {
            Ok(values)
        }
    }
}
