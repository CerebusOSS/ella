use std::future::IntoFuture;

use ella_engine::registry::SchemaRef;
use futures::{future::BoxFuture, FutureExt};

use crate::Ella;

#[must_use]
#[derive(Debug)]
pub struct GetSchema<'a> {
    inner: &'a Ella,
    schema: SchemaRef<'a>,
}

impl<'a> GetSchema<'a> {
    pub(crate) fn new(inner: &'a Ella, schema: SchemaRef<'a>) -> Self {
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

    pub fn drop(self) -> DropSchema<'a> {
        DropSchema {
            inner: self.inner,
            schema: self.schema,
            if_exists: false,
            cascade: false,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub struct GetOrCreateSchema<'a> {
    inner: &'a Ella,
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
    inner: &'a Ella,
    schema: SchemaRef<'a>,
    create: bool,
}

impl<'a> IntoFuture for UseSchema<'a> {
    type Output = crate::Result<Ella>;
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

#[must_use]
#[derive(Debug)]
pub struct DropSchema<'a> {
    inner: &'a Ella,
    schema: SchemaRef<'a>,
    if_exists: bool,
    cascade: bool,
}

impl<'a> DropSchema<'a> {
    pub fn if_exists(mut self) -> Self {
        self.if_exists = true;
        self
    }

    pub fn cascade(mut self) -> Self {
        self.cascade = true;
        self
    }
}

impl<'a> IntoFuture for DropSchema<'a> {
    type Output = crate::Result<&'a Ella>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let if_exists = if self.if_exists { "IF EXISTS " } else { "" };
            let cascade = if self.cascade { " CASCADE" } else { "" };
            self.inner
                .execute(format!("DROP SCHEMA {if_exists}{}{cascade}", self.schema))
                .await?;
            Ok(self.inner)
        }
        .boxed()
    }
}
