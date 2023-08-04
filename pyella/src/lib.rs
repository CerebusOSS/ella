mod data_types;
mod dataframe;
mod ella;
mod lazy;
mod table;
mod utils;

use dataframe::{PyColumn, PyDataFrame};
use lazy::{LazyIter, PyLazy};
use pyo3::{prelude::*, types::PyString};
use table::{publisher::PyPublisher, PyColumnInfo, PyTable, PyTopicInfo, TableAccessor};
use tracing_subscriber::{
    filter::LevelFilter, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter, Layer,
};

pub use self::ella::{connect, open, PyElla};
pub use ::ella::{Error, Result};
pub use data_types::generate_py_types;

pub(crate) use data_types::unwrap_dtype;
pub(crate) use table::{column, topic};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[pymodule]
#[pyo3(name = "_internal")]
fn pyella(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    init_logging(LevelFilter::WARN);
    m.add("runtime", utils::TokioRuntime::new())?;

    m.add_class::<PyElla>()?;
    m.add_class::<TableAccessor>()?;
    m.add_class::<PyTable>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyPublisher>()?;
    m.add_class::<PyLazy>()?;
    m.add_class::<LazyIter>()?;
    m.add_class::<PyColumnInfo>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyTopicInfo>()?;

    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(column, m)?)?;
    m.add_function(wrap_pyfunction!(topic, m)?)?;
    m.add_function(wrap_pyfunction!(now, m)?)?;

    data_types::add_datatypes(py, m)?;
    Ok(())
}

/// Get the current system time.
///
/// This is the recommended way to generate ella timestamps as it has higher
/// precision than the built-in `datetime` module.
#[pyfunction]
pub(crate) fn now(py: Python) -> PyResult<&PyAny> {
    let dt = py.import("numpy")?.getattr("datetime64")?;

    let now = ::ella::now().timestamp();
    dt.call1((now.into_py(py), PyString::new(py, "ns")))
}

fn init_logging(level: LevelFilter) {
    let res = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                EnvFilter::builder()
                    .with_default_directive(level.into())
                    .with_env_var("ELLE_LOG")
                    .from_env()
                    .unwrap(),
            ),
        )
        .try_init();
    if let Err(error) = res {
        eprintln!("failed to initialize ella logging: {error}");
    }
}
