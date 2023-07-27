use std::future::IntoFuture;

use futures::{future::BoxFuture, FutureExt};

use crate::{registry::TableRef, table::info::ViewBuilder, TableConfig};

use super::Lazy;

#[derive(Debug)]
pub struct LazyToView {
    inner: Lazy,
    table: TableRef<'static>,
    builder: ViewBuilder,
    if_not_exists: bool,
    or_replace: bool,
}

impl LazyToView {
    pub(crate) fn new(inner: Lazy, table: TableRef<'static>) -> Self {
        Self {
            builder: ViewBuilder::new(inner.plan.clone()),
            inner,
            table,
            if_not_exists: false,
            or_replace: false,
        }
    }

    pub fn materialize(mut self) -> Self {
        self.builder = self.builder.materialize();
        self
    }

    pub fn config(mut self, config: TableConfig) -> Self {
        self.builder = self.builder.config(config);
        self
    }

    pub fn index(mut self, col: impl Into<String>, ascending: bool) -> crate::Result<Self> {
        self.builder = self.builder.index(col, ascending)?;
        Ok(self)
    }

    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
        self
    }

    pub fn or_replace(mut self) -> Self {
        self.or_replace = true;
        self
    }
}

impl IntoFuture for LazyToView {
    type Output = crate::Result<Lazy>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let plan = self
                .inner
                .backend
                .create_view(
                    self.table,
                    self.builder.build(),
                    self.if_not_exists,
                    self.or_replace,
                )
                .await?;
            Ok(Lazy::new(plan, self.inner.backend))
        }
        .boxed()
    }
}
