use std::{fmt::Debug, task::Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};

use super::{format::RowViewIter, RowFormat};

pub struct RowStream<R: RowFormat> {
    inner: BoxStream<'static, crate::Result<RecordBatch>>,
    iter: Option<RowViewIter<R, R::View>>,
}

impl<R: RowFormat> Debug for RowStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowStream")
            .field("iter", &self.iter)
            .finish_non_exhaustive()
    }
}

impl<R: RowFormat> RowStream<R> {
    pub fn new<S, E>(inner: S) -> Self
    where
        S: Stream<Item = Result<RecordBatch, E>> + Send + 'static,
        E: Into<crate::Error> + 'static,
    {
        Self {
            inner: inner.map_err(Into::into).boxed(),
            iter: None,
        }
    }
}

impl<R: RowFormat> Stream for RowStream<R>
where
    Self: Unpin,
{
    type Item = crate::Result<R>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(iter) = &mut self.iter {
                match iter.next() {
                    Some(item) => return Poll::Ready(Some(Ok(item))),
                    None => {
                        self.iter = None;
                    }
                }
            }
            match futures::ready!(self.inner.as_mut().poll_next(cx)) {
                Some(Ok(batch)) => {
                    match R::view(batch.num_rows(), &batch.schema().fields, batch.columns()) {
                        Ok(view) => {
                            self.iter = Some(view.into_iter());
                        }
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(None),
            }
        }
    }
}
