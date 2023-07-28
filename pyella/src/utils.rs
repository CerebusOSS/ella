use futures::Future;
use pyo3::{exceptions::PyRuntimeError, prelude::*};

pub(crate) fn tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    let lib = py.import("ella._internal").unwrap();
    lib.getattr("runtime").unwrap().extract().unwrap()
}

pub(crate) fn wait_for_future<F: Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    let rt = &tokio_runtime(py).0;
    py.allow_threads(|| rt.block_on(f))
}

#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

impl TokioRuntime {
    pub(crate) fn new() -> Self {
        Self(tokio::runtime::Runtime::new().unwrap())
    }
}

pub(crate) fn deserialize_py<T: serde::de::DeserializeOwned>(
    py: Python,
    obj: &PyObject,
) -> PyResult<T> {
    serde_json::from_str(&py_to_str(py, obj)?)
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

pub(crate) fn serialize_py<T: serde::Serialize>(py: Python, obj: &T) -> PyResult<PyObject> {
    let raw = serde_json::to_string(obj).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    py_from_str(py, &raw)
}

fn py_to_str(py: Python, obj: &PyObject) -> PyResult<String> {
    let json = py.import("json")?;
    json.getattr("dumps")?.call1((obj,))?.extract()
}

fn py_from_str(py: Python, s: &str) -> PyResult<PyObject> {
    let json = py.import("json")?;
    Ok(json.getattr("loads")?.call1((s,))?.to_object(py))
}
