pub(crate) mod publisher;

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

/// Provides access to table in the datastore.
#[derive(Debug, derive_more::From, derive_more::Into)]
#[pyclass(module = "ella.table")]
pub(crate) struct TableAccessor {
    inner: Arc<Ella>,
}

#[pymethods]
impl TableAccessor {
    /// Get a table if it exists.
    fn get(&self, py: Python, table: &str) -> PyResult<Option<PyTable>> {
        Ok(wait_for_future(py, self.inner.table(table).into_future())?.map(Into::into))
    }

    /// Get a table, creating it if it doesn't exist.
    fn get_or_create(&self, py: Python, table: &str, info: PyTableInfo) -> PyResult<PyTable> {
        let table = wait_for_future(py, self.inner.table(table).or_create(info).into_future())?;
        Ok(table.into())
    }

    /// Create a table, replacing it if it already exists.
    fn create(&self, py: Python, table: &str, info: PyTableInfo) -> PyResult<PyTable> {
        let table = wait_for_future(py, self.inner.table(table).replace(info).into_future())?;
        Ok(table.into())
    }

    /// Remove a table from the datastore.
    fn drop(&self, py: Python, table: &str) -> PyResult<()> {
        wait_for_future(py, self.inner.table(table).drop().into_future())?;
        Ok(())
    }

    fn __getitem__(&self, py: Python, key: &PyAny) -> PyResult<PyTable> {
        self.get(py, key.extract()?)?
            .ok_or_else(|| PyKeyError::new_err(format!("table {key} not found")))
    }
}

/// A topic or view in the datastore.
#[derive(Debug, derive_more::From, derive_more::Into)]
#[pyclass(name = "Table", module = "ella.table")]
pub struct PyTable {
    inner: Table,
}

#[pymethods]
impl PyTable {
    /// Create a new publisher that writes to this table.
    #[pyo3(signature = (capacity = 1))]
    fn publish(&self, py: Python, capacity: usize) -> PyResult<PyPublisher> {
        PyPublisher::new(py, self.inner.publish()?, capacity)
    }

    /// The fully-qualified table ID
    #[getter]
    fn id(&self) -> String {
        self.inner.id().to_string()
    }

    #[getter]
    fn info(&self, py: Python) -> PyObject {
        PyTableInfo::from(self.inner.info()).into_py(py)
    }
}

/// Create a new topic definition.
///
/// Args:
///     columns: one or more column definitions
///     temporary: if `True` then values written to the topic are not saved to disk.
///     index: topic indices with the format `(column, ascending)`
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
        let col: PyColumnInfo = col.extract()?;
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

/// The definition for a datastore topic.
#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "TopicInfo", module = "ella.table")]
pub struct PyTopicInfo {
    inner: TopicInfo,
}

#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "ViewInfo", module = "ella.table")]
pub struct PyViewInfo {
    inner: ViewInfo,
}

/// Create a new column definition.
///
/// Args:
///     name: column name
///     required: if `True` then column values can't be null
///     row_shape: the shape of each row if the column is a tensor
#[pyfunction]
#[pyo3(signature = (name, data_type, required=false, row_shape=None))]
pub(crate) fn column(
    name: String,
    #[pyo3(from_py_with = "crate::unwrap_dtype")] data_type: TensorType,
    required: bool,
    row_shape: Option<Vec<usize>>,
) -> PyColumnInfo {
    Column {
        name,
        data_type,
        required,
        row_shape: row_shape.map(Dyn::from),
    }
    .into()
}

/// The definition for a column in a table.
///
/// To create a new column definition see the [`column()`] function.
#[derive(Debug, Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "ColumnInfo", module = "ella.table")]
pub struct PyColumnInfo {
    inner: Column,
}

#[pymethods]
impl PyColumnInfo {
    /// Column name
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Column data type
    #[getter]
    fn dtype(&self, py: Python) -> PyObject {
        wrap_dtype(py, self.inner.data_type.clone())
    }

    /// Whether the column is required.
    ///
    /// If `True` then rows in the column can't be null.
    #[getter]
    fn required(&self) -> bool {
        self.inner.required
    }

    /// Get the shape of each row if the column is a tensor, or `None` if it's a scalar.
    #[getter]
    fn row_shape(&self) -> Option<Vec<usize>> {
        self.inner.row_shape.clone().map(|shape| shape.to_vec())
    }
}
