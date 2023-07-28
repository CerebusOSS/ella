use std::{future::IntoFuture, sync::Arc};

use ella::{Ella, EllaConfig};
use pyo3::{prelude::*, types::PyMapping};

use crate::{
    lazy::PyLazy,
    table::TableAccessor,
    utils::{deserialize_py, serialize_py, wait_for_future},
};

#[derive(Debug, Clone, derive_more::Into, derive_more::From)]
#[pyclass(name = "Ella")]
pub struct PyElla {
    inner: Arc<Ella>,
}

#[pyfunction]
pub fn connect(py: Python, addr: &str) -> crate::Result<PyElla> {
    let inner = Arc::new(wait_for_future(py, ella::connect(addr))?);
    Ok(PyElla { inner })
}

#[pyfunction]
#[pyo3(signature = (root, serve="127.0.0.1:50052".to_string(), config=None, create=false))]
pub fn open(
    py: Python,
    root: String,
    serve: Option<String>,
    config: Option<&PyMapping>,
    create: bool,
) -> PyResult<PyElla> {
    let config = if let Some(config) = config {
        deserialize_py(py, &config.to_object(py))?
    } else {
        EllaConfig::default()
    };
    let inner = if create {
        let mut f = ella::open(root).or_create(config);
        if let Some(serve) = serve {
            f = f.and_serve(serve)?;
        }
        wait_for_future(py, f.into_future())
    } else {
        let mut f = ella::open(root);

        if let Some(serve) = serve {
            f = f.and_serve(serve)?;
        }
        wait_for_future(py, f.into_future())
    }?;

    Ok(PyElla {
        inner: Arc::new(inner),
    })
}

#[pymethods]
impl PyElla {
    #[getter]
    fn tables(&self) -> TableAccessor {
        self.inner.clone().into()
    }

    fn query(&self, py: Python, sql: String) -> PyResult<PyLazy> {
        let plan = wait_for_future(py, self.inner.query(sql))?;
        Ok(plan.into())
    }

    fn shutdown(&self, py: Python) -> crate::Result<()> {
        wait_for_future(py, (*self.inner).clone().shutdown())
    }

    #[getter]
    fn config(&self, py: Python) -> PyResult<PyObject> {
        serialize_py(py, &self.inner.config())
    }

    fn __enter__<'py>(this: PyRef<'py, Self>, _py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        Ok(this)
    }

    fn __exit__(
        &self,
        py: Python,
        _exc_type: &PyAny,
        _exc_value: &PyAny,
        _traceback: &PyAny,
    ) -> ella::Result<()> {
        self.shutdown(py)
    }
}
