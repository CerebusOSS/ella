use datafusion::arrow::datatypes::SchemaRef;
use ella_engine::{codec::TableStub, registry::TableId, table::info::TableInfo};

use crate::client::{EllaClient, FlightPublisher};

#[derive(Debug)]
pub struct RemoteTable {
    id: TableId<'static>,
    client: EllaClient,
    info: TableInfo,
}

impl RemoteTable {
    pub(crate) fn new(id: TableId<'static>, info: TableInfo, client: EllaClient) -> Self {
        Self { id, client, info }
    }

    pub fn id(&self) -> &TableId<'static> {
        &self.id
    }

    pub fn info(&self) -> TableInfo {
        self.info.clone()
    }

    pub fn arrow_schema(&self) -> crate::Result<SchemaRef> {
        Ok(match &self.info {
            TableInfo::Topic(topic) => topic.arrow_schema(),
            TableInfo::View(view) => view.plan().arrow_schema(),
        })
    }

    pub fn publish(&self) -> FlightPublisher {
        FlightPublisher::new(self.client.clone(), self.id.clone())
    }

    pub fn as_stub(&self) -> crate::Result<TableStub> {
        Ok(TableStub::new(self.id.clone(), self.arrow_schema()?))
    }
}
