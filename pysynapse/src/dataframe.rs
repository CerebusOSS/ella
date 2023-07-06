use pyo3::prelude::*;
use synapse::tensor::DataFrame;

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    inner: DataFrame,
}
