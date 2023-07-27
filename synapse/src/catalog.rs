use std::future::IntoFuture;

use futures::{future::BoxFuture, FutureExt};
use synapse_engine::registry::Id;

use crate::Synapse;

#[must_use]
#[derive(Debug)]
pub struct GetCatalog<'a> {
    inner: &'a Synapse,
    catalog: Id<'a>,
}

impl<'a> GetCatalog<'a> {
    pub(crate) fn new(inner: &'a Synapse, catalog: Id<'a>) -> Self {
        Self { inner, catalog }
    }

    pub fn or_create(self) -> GetOrCreateCatalog<'a> {
        GetOrCreateCatalog {
            inner: self.inner,
            catalog: self.catalog,
        }
    }

    pub fn and_use(self) -> UseCatalog<'a> {
        UseCatalog {
            inner: self.inner,
            catalog: self.catalog,
            create: false,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub struct GetOrCreateCatalog<'a> {
    inner: &'a Synapse,
    catalog: Id<'a>,
}

impl<'a> GetOrCreateCatalog<'a> {
    pub fn and_use(self) -> UseCatalog<'a> {
        UseCatalog {
            inner: self.inner,
            catalog: self.catalog,
            create: true,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub struct UseCatalog<'a> {
    inner: &'a Synapse,
    catalog: Id<'a>,
    create: bool,
}

impl<'a> IntoFuture for UseCatalog<'a> {
    type Output = crate::Result<Synapse>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let mut this = self.inner.clone();
            if self.create {
                this.create_catalog(self.catalog.clone(), true).await?;
            }
            this.use_catalog(self.catalog).await
        }
        .boxed()
    }
}
