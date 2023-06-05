use pyo3::prelude::*;
use std::sync::Arc;
use synapse::runtime::{topic::Publisher, Topic};

use crate::wait_for_future;

#[derive(Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "Topic")]
pub struct PyTopic {
    topic: Arc<Topic>,
}

#[pymethods]
impl PyTopic {
    fn publish(&self) -> PyPublisher {
        PyPublisher {
            inner: Arc::new(self.topic.publish()),
        }
    }

    fn close(&self, py: Python) -> crate::Result<()> {
        wait_for_future(py, self.topic.close())?;
        Ok(())
    }
}

#[derive(Clone, derive_more::From, derive_more::Into)]
#[pyclass(name = "Publisher")]
pub struct PyPublisher {
    inner: Arc<Publisher>,
}

#[pymethods]
impl PyPublisher {}
