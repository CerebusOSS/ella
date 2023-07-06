use futures::TryStreamExt;
use pyo3::prelude::*;
use synapse::engine::lazy::{Lazy, LazyStream};

use crate::{dataframe::PyDataFrame, wait_for_future};

#[derive(Debug, Clone)]
#[pyclass(name = "Lazy")]
pub struct PyLazy {
    inner: Lazy,
}

#[pymethods]
impl PyLazy {
    fn __iter__(slf: PyRef<'_, Self>, py: Python) -> crate::Result<LazyIter> {
        let inner = wait_for_future(py, slf.clone().inner.stream())?;
        Ok(LazyIter { inner })
    }

    fn execute(&self, py: Python) -> crate::Result<PyDataFrame> {
        Ok(wait_for_future(py, self.inner.clone().execute())?.into())
    }
}

#[pyclass]
struct LazyIter {
    inner: LazyStream,
}

#[pymethods]
impl LazyIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> crate::Result<Option<PyDataFrame>> {
        Ok(wait_for_future(py, slf.inner.try_next())?.map(PyDataFrame::from))
    }
}
