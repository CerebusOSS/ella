use std::{fmt::Debug, marker::PhantomData, pin::Pin, sync::Arc, task::Poll};

use datafusion::{
    arrow::{compute::concat_batches, record_batch::RecordBatch},
    execution::context::SessionState,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    physical_plan::execute_stream,
    prelude::Expr,
};
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use synapse_common::{
    row::{RowFormat, RowStream},
    TensorValue,
};
use synapse_tensor::{Const, DataFrame, Dyn, Shape, Tensor};

#[async_trait::async_trait]
pub trait LazyBackend: Debug + Send + Sync {
    async fn stream(
        &self,
        plan: &LogicalPlan,
    ) -> crate::Result<BoxStream<'static, crate::Result<RecordBatch>>>;

    async fn execute(&self, plan: &LogicalPlan) -> crate::Result<DataFrame> {
        let batches = self.stream(plan).await?.try_collect::<Vec<_>>().await?;
        let schema = if let Some(batch) = batches.first() {
            batch.schema()
        } else {
            (**plan.schema()).clone().into()
        };

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
        println!("{:?}", plan.schema());

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

    pub fn col<T, S>(&self, col: &str) -> crate::Result<Column<T, S>>
    where
        T: TensorValue,
        S: Shape,
    {
        let col = datafusion::common::Column::new_unqualified(col)
            .normalize_with_schemas_and_ambiguity_check(&[&[&self.plan.schema()]], &[])?;
        let expr = Expr::Column(col);

        Ok(Column {
            src: self.clone(),
            expr,
            _type: PhantomData,
        })
    }

    pub fn col1<T: TensorValue>(&self, col: &str) -> crate::Result<Column<T, Const<1>>> {
        self.col(col)
    }

    pub fn col2<T: TensorValue>(&self, col: &str) -> crate::Result<Column<T, Const<2>>> {
        self.col(col)
    }

    pub fn col3<T: TensorValue>(&self, col: &str) -> crate::Result<Column<T, Const<3>>> {
        self.col(col)
    }

    pub fn col_dyn<T: TensorValue>(&self, col: &str) -> crate::Result<Column<T, Dyn>> {
        self.col(col)
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

pub struct Column<T, S> {
    src: Lazy,
    expr: Expr,
    _type: PhantomData<(T, S)>,
}

impl<T, S> From<Column<T, S>> for Lazy {
    fn from(value: Column<T, S>) -> Self {
        // We check validity at compile time so adding the column expression to the existing
        // plan should never fail
        let plan = LogicalPlanBuilder::from(value.src.plan)
            .project(Some(value.expr))
            .expect("column expressions should always be valid")
            .build()
            .expect("logical plan should always be valid");

        Self {
            plan,
            backend: value.src.backend,
        }
    }
}

impl<T, S> Column<T, S>
where
    T: TensorValue,
    S: Shape,
{
    pub async fn execute(self) -> crate::Result<Tensor<T, S>> {
        let frame = Lazy::from(self).execute().await?;
        frame.icol(0)
    }

    pub async fn stream(self) -> crate::Result<BoxStream<'static, crate::Result<Tensor<T, S>>>> {
        Ok(Lazy::from(self)
            .stream()
            .await?
            .and_then(|batch| futures::future::ready(batch.icol(0)))
            .boxed())
    }
}
