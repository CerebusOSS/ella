use std::{fmt::Debug, ops::Deref};

use arrow::{
    array::{make_array, ArrayData, ArrayRef},
    datatypes::{DataType, FieldRef, SchemaRef},
    pyarrow::{ArrowException, PyArrowType, ToPyArrow},
    record_batch::RecordBatch,
};
use ella::{table::Publisher, tensor::DataFrame};
use futures::SinkExt;
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{PyDict, PyList, PyTuple},
};

use crate::{dataframe::PyDataFrame, utils::wait_for_future};

/// Writes rows to table.
#[derive(Debug)]
#[pyclass(name = "Publisher")]
pub struct PyPublisher {
    len: usize,
    capacity: usize,
    columns: Py<PyDict>,
    schema: SchemaRef,
    inner: Publisher,
    time: Option<String>,
}

#[pymethods]
impl PyPublisher {
    /// Write a row of values to the table.
    #[pyo3(signature = (*args, **kwargs))]
    fn write(&mut self, py: Python, args: &PyTuple, kwargs: Option<&PyDict>) -> PyResult<()> {
        self.append_row(py, args, kwargs)?;
        self.len += 1;
        self.maybe_write(py, false)?;
        Ok(())
    }

    /// Write any pending rows to the table.
    fn flush(&mut self, py: Python) -> PyResult<()> {
        self.maybe_write(py, true)?;
        wait_for_future(py, self.inner.flush())?;
        Ok(())
    }

    /// Write a batch of rows to the table.
    #[pyo3(signature = (*args, **kwargs))]
    fn write_batch(&mut self, py: Python, args: &PyTuple, kwargs: Option<&PyDict>) -> PyResult<()> {
        // Flush any accumulated rows to retain row order
        self.maybe_write(py, true)?;

        // Check if function was passed a record batch or dataframe
        if args.len() == 1 && kwargs.is_none() {
            if let Ok(batch) = args[0].extract::<PyArrowType<RecordBatch>>() {
                wait_for_future(py, self.inner.send(batch.0))?;
                return Ok(());
            }
            if let Ok(df) = args[0].extract::<PyDataFrame>() {
                wait_for_future(py, self.inner.send(DataFrame::from(df).into()))?;
                return Ok(());
            }
        }

        let mut arrays = vec![];
        for (items, field) in self.map_args(py, args, kwargs)? {
            let array = items_to_array(py, items, field)?;
            arrays.push(array);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|err| ArrowException::new_err(err.to_string()))?;
        wait_for_future(py, self.inner.send(batch))?;
        Ok(())
    }

    /// Close the publisher.
    ///
    /// Any pending values are flushed before closing.
    fn close(&mut self, py: Python) -> PyResult<()> {
        self.maybe_write(py, true)?;
        wait_for_future(py, self.inner.close())?;
        Ok(())
    }

    fn __enter__<'py>(this: PyRef<'py, Self>, _py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        Ok(this)
    }

    fn __exit__(
        &mut self,
        py: Python,
        _exc_type: &PyAny,
        _exc_value: &PyAny,
        _traceback: &PyAny,
    ) -> PyResult<()> {
        self.close(py)
    }
}

impl Drop for PyPublisher {
    fn drop(&mut self) {
        Python::with_gil(|py| {
            if let Err(err) = self.close(py) {
                err.restore(py);
            }
        })
    }
}

impl PyPublisher {
    pub fn new(py: Python, inner: Publisher, capacity: usize) -> PyResult<Self> {
        let schema = inner.arrow_schema().clone();
        let columns = PyDict::new(py);
        for field in schema.fields() {
            columns.set_item(field.name(), PyList::empty(py).to_object(py))?;
        }
        let time = if schema.fields.len() > 0
            && matches!(schema.fields[0].data_type(), DataType::Timestamp(_, _))
        {
            Some(schema.fields[0].name().clone())
        } else {
            None
        };

        Ok(Self {
            len: 0,
            capacity,
            schema,
            inner,
            columns: columns.into(),
            time,
        })
    }

    fn maybe_write(&mut self, py: Python, force: bool) -> PyResult<()> {
        if self.len >= self.capacity || self.len > 0 && force {
            self.write_rows(py)?;
            let columns = self.columns.as_ref(py);
            for (_, col) in columns.iter() {
                col.call_method0("clear")?;
            }
            self.len = 0;
        }
        Ok(())
    }

    fn write_rows(&mut self, py: Python) -> PyResult<()> {
        let mut arrays = vec![];
        let columns = self.columns.as_ref(py);

        for field in self.schema.fields() {
            let items = columns.get_item(field.name()).unwrap();
            arrays.push(items_to_array(py, items, field)?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|err| ArrowException::new_err(err.to_string()))?;
        wait_for_future(py, self.inner.send(batch))?;
        Ok(())
    }

    fn append_row(&self, py: Python, args: &PyTuple, kwargs: Option<&PyDict>) -> PyResult<()> {
        let columns = self.columns.as_ref(py);
        for (value, field) in self.map_args(py, args, kwargs)? {
            let col = PyAny::get_item(columns.deref(), field.name())?.downcast::<PyList>()?;
            col.append(value)?;
        }
        Ok(())
    }

    /// Map positional or keyword arguments to schema columns.
    fn map_args<'a>(
        &'a self,
        py: Python<'a>,
        args: &'a PyTuple,
        kwargs: Option<&'a PyDict>,
    ) -> PyResult<Vec<(&'a PyAny, &'a FieldRef)>> {
        if !args.is_empty() {
            if kwargs.is_some() {
                return Err(PyValueError::new_err(
                    "publisher accepts position arguments or keyword arguments but not both",
                ));
            }

            if args.len() == self.schema.fields.len() {
                // All columns present in arguments
                Ok(args.iter().zip(self.schema.fields()).collect())
            } else if args.len() + 1 == self.schema.fields.len() && self.time.is_some() {
                // Need to prepend time column
                Ok(std::iter::once(crate::now(py)?)
                    .chain(args)
                    .zip(self.schema.fields())
                    .collect())
            } else {
                // Missing one or more columns
                Err(PyValueError::new_err(format!(
                    "publisher expects {} arguments but received {}",
                    self.schema.fields.len(),
                    args.len(),
                )))
            }
        } else if let Some(kwargs) = kwargs {
            // Insert time column if needed
            if let Some(time) = &self.time {
                if !(kwargs.contains(time)?) {
                    kwargs.set_item(time, crate::now(py)?)?;
                }
            }
            if kwargs.len() != self.schema.fields.len() {
                return Err(PyValueError::new_err(format!(
                    "publisher expects {} arguments but received {}",
                    self.schema.fields.len(),
                    kwargs.len(),
                )));
            }

            let mut fields = vec![];
            for (k, v) in kwargs.iter() {
                let name = k
                    .extract::<&str>()
                    .expect("keyword argument key should always be string");
                let idx = self
                    .schema
                    .index_of(name)
                    .map_err(|_| PyKeyError::new_err(format!("column not found: \"{name}\"")))?;
                fields.push((v, &self.schema.fields[idx]));
            }
            Ok(fields)
        } else {
            return Err(PyValueError::new_err(format!(
                "publisher expects {} arguments but received 0",
                self.schema.fields.len(),
            )));
        }
    }
}

/// Convert one or more Python items to an Arrow array.
fn items_to_array(py: Python, items: &PyAny, field: &FieldRef) -> PyResult<ArrayRef> {
    let pyarrow = py.import("pyarrow")?;
    let numpy = py.import("numpy")?;

    let data = if let Ok(data) = items.extract::<PyArrowType<ArrayData>>() {
        data.0
    } else if matches!(field.data_type(), DataType::FixedSizeList(_, _)) {
        let items = numpy.getattr("ma")?.getattr("asarray")?.call1((items,))?;
        let array = pyarrow
            .getattr("FixedShapeTensorArray")?
            .call_method1("from_numpy_ndarray", (items,))?
            .extract::<PyArrowType<ArrayData>>()?;
        array.0
    } else {
        let pytype = field.data_type().to_pyarrow(py)?;
        let array = pyarrow
            .getattr("array")?
            .call1((items,))?
            .call_method1("cast", (pytype,))?
            .extract::<PyArrowType<ArrayData>>()?;
        array.0
    };
    Ok(make_array(data))
}
