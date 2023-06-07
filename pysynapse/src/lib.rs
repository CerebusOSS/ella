mod data_types;
mod engine;
mod topic;

use futures::Future;
use once_cell::sync::OnceCell;
use pyo3::{
    exceptions::{PyLookupError, PyValueError},
    prelude::*,
};

pub use engine::{PyEngine, PyEngineConfig};
pub use synapse;
pub use topic::{PyPublisher, PyTopic};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[pymodule]
#[pyo3(name = "_internal")]
fn pysynapse(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyEngine>()?;
    m.add_class::<PyEngineConfig>()?;
    m.add_class::<PyTopic>()?;
    m.add_class::<PyPublisher>()?;

    m.add_function(wrap_pyfunction!(engine::runtime, m)?)?;

    data_types::add_module(py, m)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("synapse engine error")]
    Engine(#[from] synapse::engine::Error),
    #[error("synapse tensor error")]
    Tensor(#[from] synapse::tensor::Error),
    #[error("no topic with id '{0}' (to create the topic pass a schema)")]
    TopicNotFound(String),
    #[error("expected one of 'ascending' or 'descending' for index, got '{0}'")]
    InvalidIndexMode(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for PyErr {
    fn from(err: Error) -> Self {
        use Error::*;
        match err {
            Engine(err) => err.into(),
            Tensor(err) => err.into(),
            TopicNotFound(err) => PyLookupError::new_err(err),
            InvalidIndexMode(err) => PyValueError::new_err(err),
        }
    }
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
