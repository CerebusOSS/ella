use arrow::{pyarrow::FromPyArrow, record_batch::RecordBatch};
use futures::{FutureExt, SinkExt};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use synapse::topic::{Publisher, Topic};

use crate::wait_for_future;

#[derive(derive_more::From, derive_more::Into)]
#[pyclass(name = "Topic")]
pub struct PyTopic {
    topic: Topic,
}

#[pymethods]
impl PyTopic {
    fn publish(&self) -> PyPublisher {
        PyPublisher {
            inner: self.topic.publish(),
        }
    }
}

#[derive(derive_more::From, derive_more::Into)]
#[pyclass(name = "Publisher")]
pub struct PyPublisher {
    inner: Publisher,
}

#[pymethods]
impl PyPublisher {
    fn try_write(&mut self, batch: &PyAny) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow(batch)?;
        match self.inner.send(batch).now_or_never() {
            Some(Ok(_)) => Ok(()),
            Some(Err(err)) => Err(err.into()),
            None => Err(PyRuntimeError::new_err("failed to write to table")),
        }
    }

    fn write(&mut self, py: Python, batch: &PyAny) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow(batch)?;
        wait_for_future(py, self.inner.send(batch))?;
        Ok(())
    }
}
