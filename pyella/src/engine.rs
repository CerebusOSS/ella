use pyo3::prelude::*;

use ella::engine::EngineConfig;

#[derive(Debug, Clone)]
#[pyclass(name = "EngineConfig")]
pub struct PyEngineConfig {
    pub(crate) cfg: EngineConfig,
}
