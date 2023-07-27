use synapse_common::TensorType;
use synapse_engine::{
    registry::{TableId, TableRef},
    table::{
        info::{TableInfo, TopicInfo, ViewBuilder, ViewInfo},
        Column,
    },
    Plan,
};

use crate::gen::{self, table_info::Kind};

impl TryFrom<gen::TensorType> for TensorType {
    type Error = crate::Error;

    fn try_from(value: gen::TensorType) -> Result<Self, Self::Error> {
        Ok(match value {
            gen::TensorType::Unknown => todo!(),
            gen::TensorType::Bool => TensorType::Bool,
            gen::TensorType::Int8 => TensorType::Int8,
            gen::TensorType::Int16 => TensorType::Int16,
            gen::TensorType::Int32 => TensorType::Int32,
            gen::TensorType::Int64 => TensorType::Int64,
            gen::TensorType::Uint8 => TensorType::UInt8,
            gen::TensorType::Uint16 => TensorType::UInt16,
            gen::TensorType::Uint32 => TensorType::UInt32,
            gen::TensorType::Uint64 => TensorType::UInt64,
            gen::TensorType::Float32 => TensorType::Float32,
            gen::TensorType::Float64 => TensorType::Float64,
            gen::TensorType::Timestamp => TensorType::Timestamp,
            gen::TensorType::Duration => TensorType::Duration,
            gen::TensorType::String => TensorType::String,
        })
    }
}

impl From<TensorType> for gen::TensorType {
    fn from(value: TensorType) -> Self {
        match value {
            TensorType::Bool => gen::TensorType::Bool,
            TensorType::Int8 => gen::TensorType::Int8,
            TensorType::Int16 => gen::TensorType::Int16,
            TensorType::Int32 => gen::TensorType::Int32,
            TensorType::Int64 => gen::TensorType::Int64,
            TensorType::UInt8 => gen::TensorType::Uint8,
            TensorType::UInt16 => gen::TensorType::Uint16,
            TensorType::UInt32 => gen::TensorType::Uint32,
            TensorType::UInt64 => gen::TensorType::Uint64,
            TensorType::Float32 => gen::TensorType::Float32,
            TensorType::Float64 => gen::TensorType::Float64,
            TensorType::Timestamp => gen::TensorType::Timestamp,
            TensorType::Duration => gen::TensorType::Duration,
            TensorType::String => gen::TensorType::String,
        }
    }
}

impl From<gen::TableId> for TableId<'static> {
    fn from(value: gen::TableId) -> Self {
        TableId {
            catalog: value.catalog.into(),
            schema: value.schema.into(),
            table: value.table.into(),
        }
    }
}

impl From<gen::TableRef> for TableRef<'static> {
    fn from(value: gen::TableRef) -> Self {
        TableRef {
            catalog: value.catalog.map(Into::into),
            schema: value.schema.map(Into::into),
            table: value.table.into(),
        }
    }
}

impl<'a> From<TableRef<'a>> for gen::TableRef {
    fn from(value: TableRef<'a>) -> Self {
        Self {
            catalog: value.catalog.map(|c| c.to_string()),
            schema: value.schema.map(|s| s.to_string()),
            table: value.table.to_string(),
        }
    }
}

impl<'a> From<TableId<'a>> for gen::TableId {
    fn from(value: TableId<'a>) -> Self {
        Self {
            catalog: value.catalog.to_string(),
            schema: value.schema.to_string(),
            table: value.table.to_string(),
        }
    }
}

impl TryFrom<gen::Column> for Column {
    type Error = crate::Error;

    fn try_from(value: gen::Column) -> Result<Self, Self::Error> {
        let mut builder = Column::builder(&value.name, value.data_type().try_into()?);
        if !value.row_shape.is_empty() {
            builder = builder.row_shape(
                value
                    .row_shape
                    .into_iter()
                    .map(|x| x as usize)
                    .collect::<Vec<_>>(),
            );
        }
        if value.required {
            builder = builder.required();
        }

        Ok(builder.build())
    }
}

impl From<Column> for gen::Column {
    fn from(value: Column) -> Self {
        Self {
            name: value.name,
            data_type: gen::TensorType::from(value.data_type).into(),
            row_shape: value.row_shape.map_or_else(
                || Vec::new(),
                |row| row.into_iter().map(|r| r as u32).collect(),
            ),
            required: value.required,
        }
    }
}

impl TryFrom<gen::TopicInfo> for TopicInfo {
    type Error = crate::Error;

    fn try_from(value: gen::TopicInfo) -> Result<Self, Self::Error> {
        let mut builder = Self::builder().append_time(false);
        for col in value.columns {
            builder = builder.column(Column::try_from(col)?);
        }

        for index in value.index {
            builder = builder.index(index.column, index.ascending);
        }

        if value.temporary {
            builder = builder.temporary();
        }
        if let Some(config) = value.config.as_deref() {
            builder = builder.config(serde_json::from_slice(config)?);
        }

        Ok(builder.build())
    }
}

impl TryFrom<TopicInfo> for gen::TopicInfo {
    type Error = crate::Error;

    fn try_from(value: TopicInfo) -> Result<Self, Self::Error> {
        let columns = value
            .columns()
            .iter()
            .map(|c| c.clone().into())
            .collect::<Vec<_>>();
        let index = value
            .index()
            .iter()
            .map(|i| gen::TableIndex {
                column: i.column.clone(),
                ascending: i.ascending,
            })
            .collect::<Vec<_>>();
        let config = if let Some(config) = value.config() {
            Some(serde_json::to_vec(config)?)
        } else {
            None
        };

        Ok(Self {
            columns,
            temporary: value.temporary(),
            index,
            config,
        })
    }
}

impl TryFrom<gen::ViewInfo> for ViewInfo {
    type Error = crate::Error;

    fn try_from(value: gen::ViewInfo) -> Result<Self, Self::Error> {
        let plan = Plan::from_bytes(&value.plan)?;
        let mut builder = ViewBuilder::new(plan);

        if let Some(definition) = value.definition {
            builder = builder.definition(definition);
        }
        if value.materialized {
            builder = builder.materialize();
        }
        for index in value.index {
            builder = builder.index(index.column, index.ascending)?;
        }
        if let Some(config) = value.config.as_deref() {
            builder = builder.config(serde_json::from_slice(config)?);
        }
        Ok(builder.build())
    }
}

impl TryFrom<ViewInfo> for gen::ViewInfo {
    type Error = crate::Error;

    fn try_from(value: ViewInfo) -> Result<Self, Self::Error> {
        let config = if let Some(config) = value.config() {
            Some(serde_json::to_vec(config)?)
        } else {
            None
        };
        let index = value.index().map_or_else(
            || Vec::new(),
            |index| {
                index
                    .iter()
                    .map(|i| gen::TableIndex {
                        column: i.column.clone(),
                        ascending: i.ascending,
                    })
                    .collect()
            },
        );
        Ok(Self {
            plan: value.plan().to_bytes(),
            definition: value.definition().map(|def| def.to_string()),
            materialized: value.materialized(),
            index,
            config,
        })
    }
}

impl TryFrom<gen::TableInfo> for TableInfo {
    type Error = crate::Error;

    fn try_from(value: gen::TableInfo) -> Result<Self, Self::Error> {
        match value.kind {
            Some(Kind::Topic(topic)) => Ok(TopicInfo::try_from(topic)?.into()),
            Some(Kind::View(view)) => Ok(ViewInfo::try_from(view)?.into()),
            None => todo!(),
        }
    }
}

impl TryFrom<TableInfo> for gen::TableInfo {
    type Error = crate::Error;

    fn try_from(value: TableInfo) -> Result<Self, Self::Error> {
        Ok(match value {
            TableInfo::Topic(topic) => gen::TableInfo {
                kind: Some(Kind::Topic(topic.try_into()?)),
            },
            TableInfo::View(view) => gen::TableInfo {
                kind: Some(Kind::View(view.try_into()?)),
            },
        })
    }
}
