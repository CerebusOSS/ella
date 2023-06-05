use pyo3::prelude::*;

use synapse::runtime::{Runtime, RuntimeConfig};

use crate::{data_types::PySchema, wait_for_future, PyTopic};

#[derive(Clone)]
#[pyclass(name = "RuntimeConfig")]
pub struct PyRuntimeConfig {
    pub(crate) cfg: RuntimeConfig,
}

#[derive(derive_more::Into, derive_more::From)]
#[pyclass(name = "Runtime")]
pub struct PyRuntime {
    pub(crate) rt: Runtime,
}

#[pymethods]
impl PyRuntime {
    #[pyo3(signature = (root, config=None))]
    #[new]
    fn new(py: Python, root: String, config: Option<PyRuntimeConfig>) -> crate::Result<Self> {
        let config = if let Some(c) = config {
            c.cfg
        } else {
            RuntimeConfig::default()
        };
        let rt = wait_for_future(py, Runtime::start_with_config(root, config))?;
        Ok(Self { rt })
    }

    fn shutdown(&self, py: Python) -> crate::Result<()> {
        Ok(wait_for_future(py, self.rt.shutdown())?)
    }

    #[pyo3(signature = (name, schema=None))]
    fn topic(&self, py: Python, name: String, schema: Option<PySchema>) -> crate::Result<PyTopic> {
        let topic = if let Some(schema) = schema {
            wait_for_future(py, self.rt.topic(name).get_or_create(schema.into()))?
        } else {
            self.rt
                .topic(&name)
                .get()
                .ok_or_else(|| crate::Error::TopicNotFound(name))?
        };
        Ok(topic.into())
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
    ) -> crate::Result<()> {
        self.shutdown(py)
    }
}

#[pyfunction]
#[pyo3(signature = (root, config=None))]
pub(crate) fn runtime(
    py: Python,
    root: String,
    config: Option<PyRuntimeConfig>,
) -> crate::Result<PyRuntime> {
    PyRuntime::new(py, root, config)
}
