use std::{collections::HashMap, sync::Arc};

use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion::{
    parquet::format::SortingColumn,
    physical_expr::{self, PhysicalSortExpr},
};
use synapse_common::TensorType;

use crate::{
    engine::SynapseState,
    registry::{ShardId, TableId},
    table::Column,
    util::parquet::parquet_compat_schema,
    Path, Plan, TableConfig,
};

use super::{topic::ShardInfo, TableIndex};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, derive_more::From)]
pub enum TableInfo {
    Topic(TopicInfo),
    View(ViewInfo),
}

impl TableInfo {
    pub fn is_topic(&self) -> bool {
        matches!(self, Self::Topic(_))
    }

    pub fn is_view(&self) -> bool {
        matches!(self, Self::View(_))
    }
}

impl From<TopicBuilder> for TableInfo {
    fn from(value: TopicBuilder) -> Self {
        value.build().into()
    }
}

impl From<ViewBuilder> for TableInfo {
    fn from(value: ViewBuilder) -> Self {
        value.build().into()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SynapseTableInfo {
    id: TableId<'static>,
    path: Path,
    config: TableConfig,
    arrow_schema: SchemaRef,
    parquet_schema: Option<SchemaRef>,
    sorting_cols: Option<Vec<SortingColumn>>,
    shards: Vec<ShardInfo>,
}

impl SynapseTableInfo {
    pub fn id(&self) -> &TableId<'static> {
        &self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    pub fn parquet_schema(&self) -> Option<&SchemaRef> {
        self.parquet_schema.as_ref()
    }

    pub fn sorting_cols(&self) -> Option<&Vec<SortingColumn>> {
        self.sorting_cols.as_ref()
    }

    pub fn shards(&self) -> &Vec<ShardInfo> {
        &self.shards
    }

    pub fn output_ordering(&self) -> Option<Vec<PhysicalSortExpr>> {
        self.sorting_cols.as_ref().map(|cols| {
            cols.iter()
                .map(|col| PhysicalSortExpr {
                    expr: Arc::new(physical_expr::expressions::Column::new(
                        self.arrow_schema.fields[col.column_idx as usize].name(),
                        col.column_idx as usize,
                    )),
                    options: SortOptions {
                        descending: col.descending,
                        nulls_first: col.nulls_first,
                    },
                })
                .collect::<Vec<_>>()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ViewInfo {
    plan: Plan,
    definition: Option<String>,
    materialized: bool,
    index: Option<Vec<TableIndex>>,
    config: Option<TableConfig>,
}

impl ViewInfo {
    pub fn builder(plan: Plan) -> ViewBuilder {
        ViewBuilder::new(plan)
    }

    pub fn definition(&self) -> Option<&str> {
        self.definition.as_deref()
    }

    pub fn materialized(&self) -> bool {
        self.materialized
    }

    pub fn index(&self) -> Option<&Vec<TableIndex>> {
        self.index.as_ref()
    }

    pub fn config(&self) -> Option<&TableConfig> {
        self.config.as_ref()
    }

    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    pub(crate) fn table_info(
        &self,
        id: TableId<'static>,
        state: &SynapseState,
    ) -> crate::Result<SynapseTableInfo> {
        let arrow_schema = self.plan.arrow_schema();
        let parquet_schema = parquet_compat_schema(arrow_schema.clone());
        let sorting_cols = sorting_cols(self.index.as_deref(), &arrow_schema)?;

        let path = state
            .root()
            .join(id.catalog.as_ref())
            .join(id.schema.as_ref())
            .join(id.table.as_ref());

        Ok(SynapseTableInfo {
            arrow_schema,
            parquet_schema,
            sorting_cols,
            id,
            path,
            shards: Vec::new(),
            config: self
                .config
                .clone()
                .unwrap_or_else(|| state.config().table_config().clone()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewBuilder {
    plan: Plan,
    definition: Option<String>,
    materialized: bool,
    index: Option<Vec<TableIndex>>,
    config: Option<TableConfig>,
}

impl ViewBuilder {
    pub fn new(plan: Plan) -> Self {
        Self {
            plan,
            definition: None,
            materialized: false,
            index: None,
            config: None,
        }
    }

    pub fn materialize(mut self) -> Self {
        self.materialized = true;
        self
    }

    pub fn definition(mut self, definition: impl Into<String>) -> Self {
        self.definition = Some(definition.into());
        self
    }

    pub fn index(mut self, col: impl Into<String>, ascending: bool) -> crate::Result<Self> {
        if !self.materialized {
            return Err(crate::EngineError::InvalidIndex(
                "cannot create index on non-materialized view".to_string(),
            )
            .into());
        }

        self.index.get_or_insert_with(Vec::new).push(TableIndex {
            column: col.into(),
            ascending,
        });
        Ok(self)
    }

    pub fn config(mut self, config: TableConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn build(self) -> ViewInfo {
        ViewInfo {
            plan: self.plan,
            definition: self.definition,
            materialized: self.materialized,
            index: self.index,
            config: self.config,
        }
    }
}

impl From<ViewBuilder> for ViewInfo {
    fn from(value: ViewBuilder) -> Self {
        value.build()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopicInfo {
    columns: Vec<Column>,
    index: Vec<TableIndex>,
    temporary: bool,
    shards: Vec<ShardInfo>,
    config: Option<TableConfig>,
}

impl TopicInfo {
    pub fn builder() -> TopicBuilder {
        TopicBuilder::new()
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn index(&self) -> &Vec<TableIndex> {
        &self.index
    }

    pub fn temporary(&self) -> bool {
        self.temporary
    }

    pub fn config(&self) -> Option<&TableConfig> {
        self.config.as_ref()
    }

    pub fn into_builder(mut self) -> TopicBuilder {
        let time = self.columns.remove(0);
        debug_assert!(time.data_type == TensorType::Timestamp);
        let time_idx = self.index.remove(0);
        debug_assert!(time_idx.column == time.name);

        TopicBuilder {
            columns: self.columns,
            index: self.index,
            time: Some(time.name),
            temporary: self.temporary,
            config: self.config,
            append_time: true,
        }
    }

    pub(crate) fn shards_mut(&mut self) -> &mut Vec<ShardInfo> {
        &mut self.shards
    }

    pub(crate) fn shard_mut(&mut self, id: &ShardId) -> crate::Result<&mut ShardInfo> {
        match self.shards.binary_search_by_key(id, |s| s.id) {
            Ok(idx) => Ok(&mut self.shards[idx]),
            Err(_) => Err(crate::EngineError::ShardNotFound(id.to_string()).into()),
        }
    }

    pub(crate) fn insert_shard(&mut self, shard: ShardInfo) -> crate::Result<()> {
        match self.shards.binary_search_by_key(&shard.id, |s| s.id) {
            Ok(_) => return Err(crate::EngineError::ShardExists(shard.id.to_string()).into()),
            Err(idx) => self.shards.insert(idx, shard),
        }
        Ok(())
    }

    pub(crate) fn table_info(
        &self,
        id: TableId<'static>,
        state: &SynapseState,
    ) -> crate::Result<SynapseTableInfo> {
        let arrow_schema = self.arrow_schema();
        let parquet_schema = parquet_compat_schema(arrow_schema.clone());
        let sorting_cols = sorting_cols(Some(&self.index), &arrow_schema)?;

        let path = state
            .root()
            .join(id.catalog.as_ref())
            .join(id.schema.as_ref())
            .join(id.table.as_ref());

        Ok(SynapseTableInfo {
            arrow_schema,
            parquet_schema,
            sorting_cols,
            id,
            path,
            shards: self.shards.clone(),
            config: self
                .config
                .clone()
                .unwrap_or_else(|| state.config().table_config().clone()),
        })
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| c.arrow_field())
                .collect::<Vec<_>>(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicBuilder {
    columns: Vec<Column>,
    index: Vec<TableIndex>,
    time: Option<String>,
    temporary: bool,
    config: Option<TableConfig>,
    append_time: bool,
}

impl Default for TopicBuilder {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            index: Vec::new(),
            time: None,
            temporary: false,
            config: None,
            append_time: true,
        }
    }
}

impl TopicBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn temporary(mut self) -> Self {
        self.temporary = true;
        self
    }

    pub fn time(mut self, name: impl Into<String>) -> Self {
        self.time = Some(name.into());
        self
    }

    pub fn index(mut self, col: impl Into<String>, ascending: bool) -> Self {
        self.index.push(TableIndex {
            column: col.into(),
            ascending,
        });
        self
    }

    pub fn column(mut self, col: impl Into<Column>) -> Self {
        self.columns.push(col.into());
        self
    }

    pub fn config(mut self, config: TableConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn build(self) -> TopicInfo {
        let mut columns = Vec::with_capacity(self.columns.len() + 1);
        let mut index = Vec::with_capacity(self.index.len() + 1);

        let time = self.time.unwrap_or_else(|| "time".to_string());
        if self.append_time {
            columns.push(Column {
                name: time.clone(),
                data_type: TensorType::Timestamp,
                row_shape: None,
                required: true,
            });
            index.push(TableIndex {
                column: time,
                ascending: true,
            });
        }
        columns.extend(self.columns.into_iter());
        index.extend(self.index.into_iter());

        TopicInfo {
            columns,
            index,
            temporary: self.temporary,
            shards: Vec::new(),
            config: self.config,
        }
    }

    #[doc(hidden)]
    pub fn append_time(mut self, append: bool) -> Self {
        self.append_time = append;
        self
    }
}

impl From<TopicBuilder> for TopicInfo {
    fn from(value: TopicBuilder) -> Self {
        value.build()
    }
}

fn sorting_cols(
    index: Option<&[TableIndex]>,
    schema: &SchemaRef,
) -> crate::Result<Option<Vec<SortingColumn>>> {
    if let Some(index) = index {
        let mut cols = Vec::with_capacity(index.len());
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), i))
            .collect::<HashMap<String, usize>>();

        for idx in index {
            match fields.get(&idx.column) {
                Some(&i) => cols.push(SortingColumn::new(i as i32, !idx.ascending, true)),
                None => {
                    return Err(crate::EngineError::InvalidIndex(format!(
                        "cannot define table index for nonexistent column {}",
                        idx.column
                    ))
                    .into())
                }
            }
        }
        Ok(Some(cols))
    } else {
        Ok(None)
    }
}
