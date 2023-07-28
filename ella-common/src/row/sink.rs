use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use super::{RowBatchBuilder, RowFormat};
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::{Sink, SinkExt};

pub struct RowSink<R: RowFormat> {
    inner: Pin<Box<dyn Sink<RecordBatch, Error = crate::Error> + Send + 'static>>,
    builder: R::Builder,
    capacity: usize,
    schema: SchemaRef,
}

impl<R: RowFormat> Debug for RowSink<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowSink")
            .field("builder", &self.builder)
            .field("capacity", &self.capacity)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl<R> RowSink<R>
where
    R: RowFormat,
{
    pub fn try_new<S>(inner: S, schema: SchemaRef, capacity: usize) -> crate::Result<Self>
    where
        S: Sink<RecordBatch, Error = crate::Error> + Send + 'static,
    {
        let inner = Box::pin(inner);
        let builder = R::builder(&schema.fields)?;

        Ok(Self {
            inner,
            builder,
            capacity,
            schema,
        })
    }

    pub fn len(&self) -> usize {
        self.builder.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn maybe_flush(&mut self, force: bool, cx: &mut Context<'_>) -> Poll<crate::Result<()>>
    where
        Self: Unpin,
    {
        if self.builder.len() >= self.capacity || !self.builder.is_empty() && force {
            futures::ready!(self.inner.poll_ready_unpin(cx))?;
            let batch = self.builder.build(self.schema.clone())?;
            Poll::Ready(self.inner.start_send_unpin(batch))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<R> Sink<R> for RowSink<R>
where
    R: RowFormat,
    Self: Unpin,
{
    type Error = crate::Error;

    #[inline]
    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.maybe_flush(false, cx)
    }

    #[inline]
    fn start_send(mut self: std::pin::Pin<&mut Self>, item: R) -> Result<(), Self::Error> {
        self.builder.push(item);
        Ok(())
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        futures::ready!(self.maybe_flush(true, cx))?;
        self.inner.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        futures::ready!(self.maybe_flush(true, cx))?;
        self.inner.poll_close_unpin(cx)
    }
}
