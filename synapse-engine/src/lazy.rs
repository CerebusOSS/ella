use std::{fmt::Debug, pin::Pin, sync::Arc, task::Poll};

use datafusion::{
    arrow::{compute::concat_batches, record_batch::RecordBatch},
    execution::context::SessionState,
    logical_expr::LogicalPlan,
    physical_plan::execute_stream,
};
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use synapse_common::row::{RowFormat, RowStream};
use synapse_tensor::DataFrame;

#[async_trait::async_trait]
pub trait LazyBackend: Debug + Send + Sync {
    async fn stream(
        &self,
        plan: &LogicalPlan,
    ) -> crate::Result<BoxStream<'static, crate::Result<RecordBatch>>>;

    async fn execute(&self, plan: &LogicalPlan) -> crate::Result<DataFrame> {
        let schema = (**plan.schema()).clone().into();
        let batches = self.stream(plan).await?.try_collect::<Vec<_>>().await?;

        concat_batches(&schema, &batches)?.try_into()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LocalBackend(pub(crate) SessionState);

#[async_trait::async_trait]
impl LazyBackend for LocalBackend {
    async fn stream(
        &self,
        plan: &LogicalPlan,
    ) -> crate::Result<BoxStream<'static, crate::Result<RecordBatch>>> {
        let plan = self.0.create_physical_plan(plan).await?;
        Ok(execute_stream(plan, self.0.task_ctx())?
            .map_err(crate::Error::from)
            .boxed())
    }
}

#[derive(Debug, Clone)]
pub struct Lazy {
    plan: LogicalPlan,
    backend: Arc<dyn LazyBackend + 'static>,
}

impl Lazy {
    pub fn new(plan: LogicalPlan, backend: Arc<dyn LazyBackend + 'static>) -> Self {
        Self { plan, backend }
    }

    pub async fn execute(self) -> crate::Result<DataFrame> {
        self.backend.execute(&self.plan).await
    }

    pub async fn stream(self) -> crate::Result<LazyStream> {
        Ok(LazyStream(self.backend.stream(&self.plan).await?))
    }

    pub async fn rows<R: RowFormat>(self) -> crate::Result<RowStream<R>> {
        Ok(self.stream().await?.rows())
    }
}

pub struct LazyStream(BoxStream<'static, crate::Result<RecordBatch>>);

impl LazyStream {
    pub fn rows<R: RowFormat>(self) -> RowStream<R> {
        RowStream::new(self.0)
    }
}

impl Stream for LazyStream {
    type Item = crate::Result<DataFrame>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Poll::Ready(match futures::ready!(self.0.poll_next_unpin(cx)) {
            Some(Ok(batch)) => Some(DataFrame::try_from(batch)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        })
    }
}
