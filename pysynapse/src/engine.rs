use pyo3::prelude::*;

use synapse::engine::EngineConfig;

#[derive(Debug, Clone)]
#[pyclass(name = "EngineConfig")]
pub struct PyEngineConfig {
    pub(crate) cfg: EngineConfig,
}
