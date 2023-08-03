mod publisher;

use std::{fmt::Debug, future::IntoFuture, sync::Arc};

use ella::{
    engine::table::{
        info::{TableInfo, TopicInfo, ViewInfo},
        Column,
    },
    shape::Dyn,
    Ella, Table, TensorType,
};
use pyo3::{exceptions::PyKeyError, prelude::*, types::PyTuple};

use self::publisher::PyPublisher;
use crate::{data_types::wrap_dtype, utils::wait_for_future};

#[derive(Debug, derive_more::From, derive_more::Into)]
#[pyclass]
pub(crate) struct TableAccessor {
    inner: Arc<Ella>,
}

#[pymethods]
impl TableAccessor {
    fn get(&self, py: Python, table: &str) -> PyResult<Option<PyTable>> {
        Ok(wait_for_future(py, self.inner.table(table).into_future())?.map(Into::into))
    }

    fn get_or_create(&self, py: Python, table: &str, info: PyTableInfo) -> PyResult<PyTable> {
        let table = wait_for_future(py, self.inner.table(table).or_create(info).into_future())?;
        Ok(table.into())
    }

    fn create(&self, py: Python, table: &str, info: PyTableInfo) -> PyResult<PyTable> {
        let table = wait_for_future(py, self.inner.table(table).replace(info).into_future())?;
        Ok(table.into())
    }

    fn drop(&self, py: Python, table: &str) -> PyResult<()> {
        wait_for_future(py, self.inner.table(table).drop().into_future())?;
        Ok(())
    }

    fn __getitem__(&self, py: Python, key: &PyAny) -> PyResult<PyTable> {
        self.get(py, key.extract()?)?
            .ok_or_else(|| PyKeyError::new_err(format!("table {key} not found")))
    }
}

#[derive(Debug, derive_more::From, derive_more::Into)]
#[pyclass(name = "Table")]
pub struct PyTable {
    inner: Table,
}

#[pymethods]
impl PyTable {
    #[pyo3(signature = (capacity = 1))]
    fn publish(&self, py: Python, capacity: usize) -> PyResult<PyPublisher> {
        PyPublisher::new(py, self.inner.publish()?, capacity)
    }

    #[getter]
    fn id(&self) -> String {
        self.inner.id().to_string()
    }

    #[getter]
    fn info(&self, py: Python) -> PyObject {
        PyTableInfo::from(self.inner.info()).into_py(py)
    }
}

#[pyfunction]
#[pyo3(signature = (*columns, temporary=false, index=Vec::new()))]
pub(crate) fn topic(
    columns: &PyTuple,
    temporary: bool,
    index: Vec<(String, bool)>,
) -> PyResult<PyTopicInfo> {
    let mut builder = TopicInfo::builder();
    if temporary {
        builder = builder.temporary();
    }
    for col in columns {
        let col: PyColumn = col.extract()?;
        builder = builder.column(col);
    }
    for (col, ascending) in index {
        builder = builder.index(col, ascending);
    }
    Ok(builder.build().into())
}

#[derive(Debug, Clone, derive_more::From, FromPyObject)]
pub enum PyTableInfo {
    Topic(PyTopicInfo),
    View(PyViewInfo),
}

impl IntoPy<PyObject> for PyTableInfo {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            PyTableInfo::Topic(topic) => topic.into_py(py),
            PyTableInfo::View(view) => view.into_py(py),
        }
    }
}

impl From<PyTableInfo> for TableInfo {
    fn from(value: PyTableInfo) -> Self {
        match value {
            PyTableInfo::Topic(topic) => TableInfo::Topic(topic.into()),
            PyTableInfo::View(view) => TableInfo::View(view.into()),
        }
    }
}

impl From<TableInfo> for PyTableInfo {
    fn from(value: TableInfo) -> Self {
        match value {
            TableInfo::Topic(topic) => Self::Topic(topic.into()),
            TableInfo::View(view) => Self::View(view.into()),
        }
    }
}

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "TopicInfo")]
pub struct PyTopicInfo {
    inner: TopicInfo,
}

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "ViewInfo")]
pub struct PyViewInfo {
    inner: ViewInfo,
}

#[pyfunction]
#[pyo3(signature = (name, data_type, required=false, row_shape=None))]
pub(crate) fn column(
    name: String,
    #[pyo3(from_py_with = "crate::unwrap_dtype")] data_type: TensorType,
    required: bool,
    row_shape: Option<Vec<usize>>,
) -> PyColumn {
    Column {
        name,
        data_type,
        required,
        row_shape: row_shape.map(Dyn::from),
    }
    .into()
}

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "Column")]
pub struct PyColumn {
    inner: Column,
}

#[pymethods]
impl PyColumn {
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[getter]
    fn dtype(&self, py: Python) -> PyObject {
        wrap_dtype(py, self.inner.data_type.clone())
    }

    #[getter]
    fn required(&self) -> bool {
        self.inner.required
    }

    #[getter]
    fn row_shape(&self) -> Option<Vec<usize>> {
        self.inner.row_shape.clone().map(|shape| shape.to_vec())
    }
}
