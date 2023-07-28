mod data_types;
mod dataframe;
mod ella;
mod lazy;
mod table;
mod utils;

use pyo3::prelude::*;

pub use self::ella::{connect, open, PyElla};
pub use ::ella::{Error, Result};

pub(crate) use data_types::unwrap_dtype;
pub(crate) use table::{column, topic};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[pymodule]
#[pyo3(name = "_internal")]
fn pyella(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add("runtime", utils::TokioRuntime::new())?;

    m.add_class::<PyElla>()?;

    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(column, m)?)?;
    m.add_function(wrap_pyfunction!(topic, m)?)?;

    data_types::add_module(py, m)?;
    Ok(())
}
