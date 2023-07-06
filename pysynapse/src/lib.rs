mod data_types;
mod dataframe;
mod engine;
mod lazy;
mod synapse;
mod topic;

use futures::Future;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;

pub use self::synapse::{connect, start, PySynapse};
pub use ::synapse::{Error, Result};
pub use engine::PyEngineConfig;
pub use topic::{PyPublisher, PyTopic};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[pymodule]
#[pyo3(name = "_internal")]
fn pysynapse(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyEngineConfig>()?;
    m.add_class::<PyTopic>()?;
    m.add_class::<PyPublisher>()?;
    m.add_class::<PySynapse>()?;

    m.add_function(wrap_pyfunction!(start, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;

    data_types::add_module(py, m)?;
    Ok(())
}

pub(crate) fn tokio_runtime() -> &'static tokio::runtime::Runtime {
    static INSTANCE: OnceCell<tokio::runtime::Runtime> = OnceCell::new();

    INSTANCE.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

pub(crate) fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    py.allow_threads(|| tokio_runtime().block_on(f))
}
