use std::future::IntoFuture;

use futures::{future::BoxFuture, FutureExt};
use synapse_engine::registry::SchemaRef;

use crate::Synapse;

#[must_use]
#[derive(Debug)]
pub struct GetSchema<'a> {
    inner: &'a Synapse,
    schema: SchemaRef<'a>,
}

impl<'a> GetSchema<'a> {
    pub(crate) fn new(inner: &'a Synapse, schema: SchemaRef<'a>) -> Self {
        Self { inner, schema }
    }

    pub fn or_create(self) -> GetOrCreateSchema<'a> {
        GetOrCreateSchema {
            inner: self.inner,
            schema: self.schema,
        }
    }

    pub fn and_use(self) -> UseSchema<'a> {
        UseSchema {
            inner: self.inner,
            schema: self.schema,
            create: false,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub struct GetOrCreateSchema<'a> {
    inner: &'a Synapse,
    schema: SchemaRef<'a>,
}

impl<'a> GetOrCreateSchema<'a> {
    pub fn and_use(self) -> UseSchema<'a> {
        UseSchema {
            inner: self.inner,
            schema: self.schema,
            create: true,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub struct UseSchema<'a> {
    inner: &'a Synapse,
    schema: SchemaRef<'a>,
    create: bool,
}

impl<'a> IntoFuture for UseSchema<'a> {
    type Output = crate::Result<Synapse>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let mut this = self.inner.clone();
            if self.create {
                this.create_schema(self.schema.clone(), true).await?;
            }
            this.use_schema(self.schema.schema).await
        }
        .boxed()
    }
}
