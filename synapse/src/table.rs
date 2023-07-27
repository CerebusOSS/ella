mod publisher;

pub use publisher::Publisher;

use std::{future::IntoFuture, sync::Arc};

use futures::{future::BoxFuture, FutureExt};
use synapse_engine::{
    registry::TableRef,
    table::{info::TableInfo, SynapseTable},
};
use synapse_server::table::RemoteTable;

use crate::Synapse;

#[derive(Debug)]
pub struct Table {
    inner: TableInner,
}

#[derive(Debug)]
enum TableInner {
    Local(Arc<SynapseTable>),
    Remote(RemoteTable),
}

impl Table {
    pub(crate) fn local(table: Arc<SynapseTable>) -> Self {
        Self {
            inner: TableInner::Local(table),
        }
    }

    pub(crate) fn remote(table: RemoteTable) -> Self {
        Self {
            inner: TableInner::Remote(table),
        }
    }

    pub fn publish(&self) -> crate::Result<Publisher> {
        use TableInner::*;
        Ok(match &self.inner {
            Local(table) => match table.as_topic() {
                Some(topic) => Publisher::new(topic.publish(), topic.info().arrow_schema()),
                None => todo!(),
            },
            Remote(table) => Publisher::new(table.publish(), table.arrow_schema()?),
        })
    }
}

#[must_use]
#[derive(Debug)]
pub struct GetTable<'a> {
    inner: &'a Synapse,
    table: TableRef<'a>,
}

impl<'a> GetTable<'a> {
    pub(crate) fn new(inner: &'a Synapse, table: TableRef<'a>) -> Self {
        Self { inner, table }
    }

    pub fn or_create(self, info: impl Into<TableInfo>) -> GetOrCreateTable<'a> {
        GetOrCreateTable {
            inner: self.inner,
            table: self.table,
            info: info.into(),
        }
    }
}

impl<'a> IntoFuture for GetTable<'a> {
    type Output = crate::Result<Option<crate::Table>>;
    type IntoFuture = BoxFuture<'a, crate::Result<Option<crate::Table>>>;

    fn into_future(self) -> Self::IntoFuture {
        self.inner.get_table(self.table).boxed()
    }
}

#[must_use]
#[derive(Debug)]
pub struct GetOrCreateTable<'a> {
    inner: &'a Synapse,
    table: TableRef<'a>,
    info: TableInfo,
}

impl<'a> IntoFuture for GetOrCreateTable<'a> {
    type Output = crate::Result<crate::Table>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            Ok(match self.inner.get_table(self.table.clone()).await? {
                Some(table) => table,
                None => {
                    self.inner
                        .create_table_inner(self.table, self.info, true, false)
                        .await?
                }
            })
        }
        .boxed()
    }
}

#[must_use]
#[derive(Debug)]
pub struct CreateTable<'a> {
    inner: &'a Synapse,
    table: TableRef<'a>,
    info: TableInfo,
}

impl<'a> CreateTable<'a> {
    pub fn if_not_exists(self) -> GetOrCreateTable<'a> {
        GetOrCreateTable {
            inner: self.inner,
            table: self.table,
            info: self.info,
        }
    }

    pub fn or_replace(self) -> CreateOrReplaceTable<'a> {
        CreateOrReplaceTable(self)
    }
}

impl<'a> IntoFuture for CreateTable<'a> {
    type Output = crate::Result<crate::Table>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            self.inner
                .create_table_inner(self.table, self.info, false, false)
                .await
        }
        .boxed()
    }
}

#[must_use]
#[derive(Debug)]
pub struct CreateOrReplaceTable<'a>(CreateTable<'a>);

impl<'a> IntoFuture for CreateOrReplaceTable<'a> {
    type Output = crate::Result<crate::Table>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            self.0
                .inner
                .create_table_inner(self.0.table, self.0.info, false, true)
                .await
        }
        .boxed()
    }
}
