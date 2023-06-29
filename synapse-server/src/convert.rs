use std::sync::Arc;

use arrow_flight::{IpcMessage, SchemaAsIpc, SchemaResult};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use synapse_engine::{schema::IndexColumn, Schema};

use crate::gen;

impl TryFrom<gen::Schema> for Schema {
    type Error = crate::Error;

    fn try_from(value: gen::Schema) -> crate::Result<Self> {
        let arrow_schema = SchemaResult {
            schema: value.arrow_schema.into(),
        }
        .try_into()?;
        let index_cols = value
            .index_columns
            .into_iter()
            .map(|col| IndexColumn {
                name: col.name,
                column: col.column as usize,
                ascending: col.ascending,
            })
            .collect::<Vec<_>>();
        Ok(Schema::new(Arc::new(arrow_schema), index_cols))
    }
}

impl From<Schema> for gen::Schema {
    fn from(value: Schema) -> Self {
        let index_columns = value
            .index_columns()
            .iter()
            .cloned()
            .map(|col| gen::IndexColumn {
                name: col.name,
                column: col.column as u32,
                ascending: col.ascending,
            })
            .collect::<Vec<_>>();
        let IpcMessage(arrow_schema) =
            SchemaAsIpc::new(value.arrow_schema(), &IpcWriteOptions::default())
                .try_into()
                .unwrap();
        gen::Schema {
            arrow_schema: arrow_schema.into(),
            index_columns,
        }
    }
}

impl From<String> for gen::TopicId {
    fn from(name: String) -> Self {
        gen::TopicId { name }
    }
}
