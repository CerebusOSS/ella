use ella::tensor::DataFrame;
use pyo3::prelude::*;

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    inner: DataFrame,
}
