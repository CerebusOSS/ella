use std::future::IntoFuture;

use ella::engine::lazy::{Lazy, LazyStream};
use futures::TryStreamExt;
use pyo3::prelude::*;

use crate::{dataframe::PyDataFrame, utils::wait_for_future};

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
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

    #[pyo3(signature = (table, if_not_exists = true))]
    fn create_view(&self, py: Python, table: &str, if_not_exists: bool) -> PyResult<Self> {
        let plan = if if_not_exists {
            wait_for_future(
                py,
                self.inner
                    .clone()
                    .create_view(table)
                    .if_not_exists()
                    .into_future(),
            )?
        } else {
            wait_for_future(py, self.inner.clone().create_view(table).into_future())?
        };
        Ok(plan.into())
    }
}

#[pyclass]
pub(crate) struct LazyIter {
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
