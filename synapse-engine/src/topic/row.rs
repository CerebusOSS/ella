use std::sync::Arc;

use synapse_common::row::{RowBatchBuilder, RowFormat};

use crate::Schema;

use super::Publisher;

#[derive(Debug, Clone)]
pub struct RowSink<R: RowFormat> {
    inner: Publisher,
    builder: R::Builder,
    capacity: usize,
}

impl<R: RowFormat> RowSink<R> {
    pub(crate) fn try_new(inner: Publisher, capacity: usize) -> crate::Result<Self> {
        let builder = R::builder(&inner.schema().arrow_schema().fields)?;

        Ok(Self {
            inner,
            builder,
            capacity,
        })
    }

    pub fn schema(&self) -> &Arc<Schema> {
        self.inner.schema()
    }

    pub fn into_inner(self) -> Publisher {
        self.inner
    }

    pub fn len(&self) -> usize {
        self.builder.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub async fn write(&mut self, row: R) -> crate::Result<()> {
        self.builder.push(row);
        self.maybe_flush(false).await
    }

    pub async fn flush(&mut self) -> crate::Result<()> {
        let batch = self.builder.build(self.schema().arrow_schema().clone())?;
        self.inner.write(batch).await
    }

    async fn maybe_flush(&mut self, closing: bool) -> crate::Result<()> {
        if self.builder.len() >= self.capacity || !self.builder.is_empty() && closing {
            self.flush().await?;
        }
        Ok(())
    }
}
