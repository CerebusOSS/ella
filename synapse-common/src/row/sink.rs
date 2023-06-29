use std::task::{Context, Poll};

use super::{RowBatchBuilder, RowFormat};
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::Sink;

pin_project_lite::pin_project! {
    #[derive(Debug, Clone)]
    #[project = RowSinkProj]
    pub struct RowSink<R: RowFormat, S> {
        #[pin]
        inner: S,
        builder: R::Builder,
        capacity: usize,
        schema: SchemaRef,
    }
}

impl<R, S> RowSink<R, S>
where
    R: RowFormat,
    S: Sink<RecordBatch, Error = crate::Error>,
{
    pub fn try_new(inner: S, schema: SchemaRef, capacity: usize) -> crate::Result<Self> {
        let builder = R::builder(&schema.fields)?;

        Ok(Self {
            inner,
            builder,
            capacity,
            schema,
        })
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    pub fn len(&self) -> usize {
        self.builder.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn maybe_flush(
        this: &mut RowSinkProj<'_, R, S>,
        force: bool,
        cx: &mut Context<'_>,
    ) -> Poll<crate::Result<()>> {
        if this.builder.len() >= *this.capacity || !this.builder.is_empty() && force {
            futures::ready!(this.inner.as_mut().poll_ready(cx))?;
            let batch = this.builder.build(this.schema.clone())?;
            Poll::Ready(this.inner.as_mut().start_send(batch))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<R, S> Sink<R> for RowSink<R, S>
where
    R: RowFormat,
    S: Sink<RecordBatch, Error = crate::Error>,
{
    type Error = crate::Error;

    #[inline]
    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Self::maybe_flush(&mut self.project(), false, cx)
    }

    #[inline]
    fn start_send(self: std::pin::Pin<&mut Self>, item: R) -> Result<(), Self::Error> {
        self.project().builder.push(item);
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        futures::ready!(Self::maybe_flush(&mut this, true, cx))?;
        this.inner.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        futures::ready!(Self::maybe_flush(&mut this, true, cx))?;
        this.inner.poll_close(cx)
    }
}
