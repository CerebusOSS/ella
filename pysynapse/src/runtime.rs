use pyo3::prelude::*;

use synapse::runtime::{Runtime, RuntimeConfig};

use crate::wait_for_future;

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
    fn new(py: Python, root: String, config: Option<PyRuntimeConfig>) -> synapse::Result<Self> {
        let config = if let Some(c) = config {
            c.cfg
        } else {
            RuntimeConfig::default()
        };
        let rt = wait_for_future(py, Runtime::start_with_config(root, config))?;
        Ok(Self { rt })
    }

    fn shutdown(&self, py: Python) -> synapse::Result<()> {
        Ok(wait_for_future(py, self.rt.shutdown())?)
    }

    // fn topic(&self, py: Python, name: String, schema: Option<) -> anyhow::Result<PyTopic> {

    // }
}
