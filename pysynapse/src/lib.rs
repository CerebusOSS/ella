use futures::Future;
use pyo3::prelude::*;

use synapse::{Runtime, RuntimeConfig};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[pymodule]
#[pyo3(name = "_internal")]
fn pysynapse(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add(
        "runtime",
        TokioRuntime(tokio::runtime::Runtime::new().unwrap()),
    )?;
    m.add_class::<PyRuntime>()?;
    Ok(())
}

#[derive(Debug, Clone)]
#[pyclass(name = "RuntimeConfig")]
pub(crate) struct PyRuntimeConfig {
    pub(crate) cfg: RuntimeConfig,
}

#[pyclass(name = "Runtime")]
pub(crate) struct PyRuntime {
    pub(crate) rt: Runtime,
}

#[pymethods]
impl PyRuntime {
    #[pyo3(signature = (root, config=None))]
    #[new]
    fn new(py: Python, root: String, config: Option<PyRuntimeConfig>) -> anyhow::Result<Self> {
        let config = if let Some(c) = config {
            c.cfg
        } else {
            RuntimeConfig::default()
        };
        let rt = wait_for_future(py, Runtime::start_with_config(root, config))?;
        Ok(Self { rt })
    }

    fn shutdown(&self, py: Python) -> anyhow::Result<()> {
        Ok(wait_for_future(py, self.rt.shutdown())?)
    }
}

#[pyclass]
pub struct TokioRuntime(tokio::runtime::Runtime);

pub(crate) fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    py.import("synapse._internal")
        .unwrap()
        .getattr("runtime")
        .unwrap()
        .extract()
        .unwrap()
}

pub(crate) fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let runtime = &get_tokio_runtime(py).0;
    py.allow_threads(|| runtime.block_on(f))
}
