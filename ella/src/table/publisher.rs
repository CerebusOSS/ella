use std::{fmt::Debug, pin::Pin};

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use ella_common::row::{RowFormat, RowSink};
use futures::{Sink, SinkExt};

pub struct Publisher {
    inner: Pin<Box<dyn Sink<RecordBatch, Error = crate::Error> + Send + 'static>>,
    arrow_schema: SchemaRef,
}

impl Publisher {
    pub(super) fn new<S>(inner: S, arrow_schema: SchemaRef) -> Self
    where
        S: Sink<RecordBatch, Error = crate::Error> + Send + 'static,
    {
        Self {
            inner: Box::pin(inner),
            arrow_schema,
        }
    }

    pub fn rows<R: RowFormat>(self, buffer: usize) -> crate::Result<RowSink<R>> {
        let schema = self.arrow_schema.clone();
        RowSink::try_new(self, schema, buffer)
    }
}

impl Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("arrow_schema", &self.arrow_schema)
            .finish_non_exhaustive()
    }
}

impl Sink<RecordBatch> for Publisher {
    type Error = crate::Error;

    #[inline]
    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready_unpin(cx)
    }

    #[inline]
    fn start_send(mut self: Pin<&mut Self>, item: RecordBatch) -> Result<(), Self::Error> {
        self.inner.start_send_unpin(item)
    }

    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_flush_unpin(cx)
    }

    #[inline]
    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_close_unpin(cx)
    }
}
