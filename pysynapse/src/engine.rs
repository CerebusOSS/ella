use pyo3::prelude::*;

use synapse::{
    common::error::PySynapseError,
    engine::{Engine, EngineConfig},
};

use crate::{data_types::PySchema, wait_for_future, PyTopic};

#[derive(Clone)]
#[pyclass(name = "EngineConfig")]
pub struct PyEngineConfig {
    pub(crate) cfg: EngineConfig,
}

#[derive(Clone, derive_more::Into, derive_more::From)]
#[pyclass(name = "Engine")]
pub struct PyEngine {
    pub(crate) engine: Engine,
}

#[pymethods]
impl PyEngine {
    #[pyo3(signature = (root, config=None))]
    #[new]
    fn new(py: Python, root: String, config: Option<PyEngineConfig>) -> synapse::Result<Self> {
        let config = if let Some(c) = config {
            c.cfg
        } else {
            EngineConfig::default()
        };
        let engine = wait_for_future(py, Engine::start_with_config(root, config))?;
        Ok(Self { engine })
    }

    fn shutdown(&self, py: Python) -> synapse::Result<()> {
        Ok(wait_for_future(py, self.engine.shutdown())?)
    }

    #[pyo3(signature = (name, schema=None))]
    fn topic(
        &self,
        py: Python,
        name: String,
        schema: Option<PySchema>,
    ) -> synapse::Result<PyTopic> {
        let topic = if let Some(schema) = schema {
            wait_for_future(py, self.engine.topic(name).get_or_create(schema.into()))?
        } else {
            self.engine
                .topic(&name)
                .get()
                .ok_or_else(|| PySynapseError::TopicNotFound(name))?
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
    ) -> synapse::Result<()> {
        self.shutdown(py)
    }
}

#[pyfunction]
#[pyo3(signature = (root, config=None))]
pub(crate) fn runtime(
    py: Python,
    root: String,
    config: Option<PyEngineConfig>,
) -> synapse::Result<PyEngine> {
    PyEngine::new(py, root, config)
}
