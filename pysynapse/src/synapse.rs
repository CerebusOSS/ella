use pyo3::{exceptions::PyKeyError, prelude::*};
use synapse::Synapse;

use crate::{data_types::PySchema, wait_for_future, PyEngineConfig, PyTopic};

#[derive(Debug, Clone, derive_more::Into, derive_more::From)]
#[pyclass(name = "Synapse")]
pub struct PySynapse {
    inner: Synapse,
}

#[pyfunction]
pub fn connect(py: Python, addr: &str) -> crate::Result<PySynapse> {
    let inner = wait_for_future(py, synapse::connect(addr))?;
    Ok(PySynapse { inner })
}

#[pyfunction]
#[pyo3(signature = (root, serve="127.0.0.1:50052".to_string(), config=None))]
pub fn start(
    py: Python,
    root: String,
    serve: Option<String>,
    config: Option<PyEngineConfig>,
) -> crate::Result<PySynapse> {
    let mut builder = synapse::start(root);
    if let Some(serve) = serve {
        builder.serve(serve)?;
    }
    if let Some(config) = config {
        builder.config(config.cfg);
    }
    let inner = wait_for_future(py, builder.build())?;

    Ok(PySynapse { inner })
}

#[pymethods]
impl PySynapse {
    fn shutdown(&self, py: Python) -> crate::Result<()> {
        wait_for_future(py, self.inner.shutdown())
    }

    #[pyo3(signature = (topic, schema=None))]
    fn topic(&self, py: Python, topic: String, schema: Option<PySchema>) -> PyResult<PyTopic> {
        if let Some(schema) = schema {
            let topic = wait_for_future(py, self.inner.topic(topic).get_or_create(schema.into()))?;
            Ok(topic.into())
        } else {
            match wait_for_future(py, self.inner.topic(&topic).get())? {
                Some(topic) => Ok(topic.into()),
                None => Err(PyKeyError::new_err(format!("topic {topic} not found")).into()),
            }
        }
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
