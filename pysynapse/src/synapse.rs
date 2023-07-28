use std::{future::IntoFuture, sync::Arc};

use pyo3::{prelude::*, types::PyMapping};
use synapse::{Synapse, SynapseConfig};

use crate::{
    lazy::PyLazy,
    table::TableAccessor,
    utils::{deserialize_py, serialize_py, wait_for_future},
};

#[derive(Debug, Clone, derive_more::Into, derive_more::From)]
#[pyclass(name = "Synapse")]
pub struct PySynapse {
    inner: Arc<Synapse>,
}

#[pyfunction]
pub fn connect(py: Python, addr: &str) -> crate::Result<PySynapse> {
    let inner = Arc::new(wait_for_future(py, synapse::connect(addr))?);
    Ok(PySynapse { inner })
}

#[pyfunction]
#[pyo3(signature = (root, serve="127.0.0.1:50052".to_string(), config=None, create=false))]
pub fn open(
    py: Python,
    root: String,
    serve: Option<String>,
    config: Option<&PyMapping>,
    create: bool,
) -> PyResult<PySynapse> {
    let config = if let Some(config) = config {
        deserialize_py(py, &config.to_object(py))?
    } else {
        SynapseConfig::default()
    };
    let inner = if create {
        let mut f = synapse::open(root).or_create(config);
        if let Some(serve) = serve {
            f = f.and_serve(serve)?;
        }
        wait_for_future(py, f.into_future())
    } else {
        let mut f = synapse::open(root);

        if let Some(serve) = serve {
            f = f.and_serve(serve)?;
        }
        wait_for_future(py, f.into_future())
    }?;

    Ok(PySynapse {
        inner: Arc::new(inner),
    })
}

#[pymethods]
impl PySynapse {
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
    ) -> synapse::Result<()> {
        self.shutdown(py)
    }
}
