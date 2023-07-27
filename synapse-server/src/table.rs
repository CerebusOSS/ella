use datafusion::arrow::datatypes::SchemaRef;
use synapse_engine::{codec::TableStub, registry::TableId, table::info::TableInfo};

use crate::client::{FlightPublisher, SynapseClient};

#[derive(Debug)]
pub struct RemoteTable {
    id: TableId<'static>,
    client: SynapseClient,
    info: TableInfo,
}

impl RemoteTable {
    pub(crate) fn new(id: TableId<'static>, info: TableInfo, client: SynapseClient) -> Self {
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
