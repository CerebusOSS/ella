mod runtime;
mod schema;
mod topic;

use futures::Future;
use pyo3::prelude::*;

pub use runtime::{PyRuntime, PyRuntimeConfig};
pub use topic::PyTopic;

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
