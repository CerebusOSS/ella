mod backend;
mod view;

use crate::{registry::TableRef, Plan};

pub use self::view::LazyToView;
pub use backend::LazyBackend;
pub(crate) use backend::LocalBackend;

use std::{fmt::Debug, marker::PhantomData, pin::Pin, sync::Arc, task::Poll};

use arrow_schema::SchemaRef;
use datafusion::{
    logical_expr::LogicalPlanBuilder, physical_plan::SendableRecordBatchStream, prelude::Expr,
};
use ella_common::{
    row::{RowFormat, RowStream},
    TensorValue,
};
use ella_tensor::{Const, DataFrame, Dyn, Shape, Tensor};
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};

#[derive(Debug, Clone)]
pub struct Lazy {
    plan: Plan,
    backend: Arc<dyn LazyBackend + 'static>,
}

impl Lazy {
    pub fn new(plan: Plan, backend: Arc<dyn LazyBackend + 'static>) -> Self {
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

    pub fn limit(mut self, limit: usize) -> crate::Result<Self> {
        self.plan = self.plan.try_map(|plan| {
            LogicalPlanBuilder::from(plan)
                .limit(0, Some(limit))?
                .build()
        })?;
        Ok(self)
    }

    pub fn skip(mut self, skip: usize) -> crate::Result<Self> {
        self.plan = self
            .plan
            .try_map(|plan| LogicalPlanBuilder::from(plan).limit(skip, None)?.build())?;
        Ok(self)
    }

    pub fn col<T, S>(&self, col: &str) -> crate::Result<Column<T, S>>
    where
        T: TensorValue,
        S: Shape,
    {
        let col = datafusion::common::Column::new_unqualified(col)
            .normalize_with_schemas_and_ambiguity_check(&[&[self.plan.df_schema()]], &[])?;
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

    pub fn create_view<'a>(self, table: impl Into<TableRef<'a>>) -> LazyToView {
        let table: TableRef<'static> = table.into().into_owned();
        LazyToView::new(self, table)
    }

    pub fn plan(&self) -> &Plan {
        &self.plan
    }
}

pub struct LazyStream(SendableRecordBatchStream);

impl LazyStream {
    pub fn into_inner(self) -> SendableRecordBatchStream {
        self.0
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        self.0.schema()
    }

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
            Some(Err(err)) => Some(Err(err.into())),
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
        let plan = value.src.plan.map(|plan| {
            LogicalPlanBuilder::from(plan)
                .project(Some(value.expr))
                .expect("column expressions should always be valid")
                .build()
                .expect("logical plan should always be valid")
        });

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
