macro_rules! table {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            Self::Topic($pattern) => $result,
            Self::View($pattern) => $result,
        }
    };
}

pub(crate) mod config;
pub mod info;
pub mod topic;
pub mod view;

pub use config::TableConfig;
pub use topic::SynapseTopic;
pub use view::SynapseView;

use std::sync::Arc;

use arrow_schema::{Field, SchemaRef};
use datafusion::{
    datasource::TableProvider,
    error::Result as DfResult,
    execution::context::SessionState,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableType},
    parquet::format::SortingColumn,
    physical_plan::{ExecutionPlan, Statistics},
    prelude::Expr,
};
use synapse_common::TensorType;
use synapse_tensor::{tensor_schema, Dyn, IntoShape, Shape};

use crate::{
    engine::SynapseState,
    registry::{snapshot::TableState, transactions::CreateTable, TableId},
    Path,
};

use self::{info::TableInfo, topic::shard::ShardSet};

#[derive(Debug)]
pub enum SynapseTable {
    Topic(Arc<SynapseTopic>),
    View(Arc<SynapseView>),
}

impl From<Arc<SynapseView>> for SynapseTable {
    fn from(value: Arc<SynapseView>) -> Self {
        Self::View(value)
    }
}

impl From<Arc<SynapseTopic>> for SynapseTable {
    fn from(value: Arc<SynapseTopic>) -> Self {
        Self::Topic(value)
    }
}

impl SynapseTable {
    pub(crate) fn new(
        id: TableId<'static>,
        info: TableInfo,
        state: &SynapseState,
        resolve: bool,
    ) -> crate::Result<Self> {
        Ok(match info {
            TableInfo::Topic(info) => Self::Topic(Arc::new(SynapseTopic::new(id, info, state)?)),
            TableInfo::View(info) => {
                Self::View(Arc::new(SynapseView::new(id, info, state, resolve)?))
            }
        })
    }

    pub fn id(&self) -> &TableId<'static> {
        table!(self, t => t.table())
    }

    pub fn config(&self) -> &TableConfig {
        table!(self, t => t.config())
    }

    pub fn path(&self) -> &Path {
        table!(self, t => t.path())
    }

    pub fn info(&self) -> TableInfo {
        table!(self, t => t.info().clone().into())
    }

    pub fn as_topic(&self) -> Option<Arc<SynapseTopic>> {
        match self {
            Self::Topic(t) => Some(t.clone()),
            _ => None,
        }
    }

    pub fn as_view(&self) -> Option<Arc<SynapseView>> {
        match self {
            Self::View(v) => Some(v.clone()),
            _ => None,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Topic(_) => "topic",
            Self::View(_) => "view",
        }
    }

    pub(crate) async fn close(&self) -> crate::Result<()> {
        match self {
            Self::Topic(t) => t.close().await,
            _ => Ok(()),
        }
    }

    pub(crate) async fn drop_shards(&self) -> crate::Result<()> {
        match self {
            Self::Topic(t) => t.drop_shards().await,
            Self::View(_) => Ok(()),
        }
    }

    pub fn load(table: &TableState, state: &SynapseState) -> crate::Result<Self> {
        Self::new(table.id.clone(), table.info.clone(), state, false)
    }

    pub(crate) fn transaction(&self) -> CreateTable {
        match self {
            SynapseTable::Topic(t) => CreateTable::topic(self.id().clone(), t.info().clone()),
            SynapseTable::View(v) => CreateTable::view(self.id().clone(), v.info().clone()),
        }
    }

    pub(crate) fn shards(&self) -> Option<&Arc<ShardSet>> {
        match self {
            SynapseTable::Topic(t) => t.shards(),
            SynapseTable::View(v) => v.shards(),
        }
    }

    pub(crate) fn file_schema(&self) -> SchemaRef {
        table!(self, t => t.file_schema())
    }

    pub(crate) fn sort(&self) -> Option<Vec<SortingColumn>> {
        table!(self, t => t.sort())
    }

    pub(crate) fn resolve(&self, state: &SynapseState) -> crate::Result<()> {
        match self {
            SynapseTable::Topic(_) => Ok(()),
            SynapseTable::View(view) => view.resolve(state),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for SynapseTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        table!(self, t => t.schema())
    }

    fn table_type(&self) -> TableType {
        table!(self, t => t.table_type())
    }

    fn get_table_definition(&self) -> Option<&str> {
        table!(self, t => t.get_table_definition())
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        table!(self, t => t.get_logical_plan())
    }

    fn statistics(&self) -> Option<Statistics> {
        table!(self, t => t.statistics())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        table!(self, t => t.supports_filters_pushdown(filters))
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        table!(self, t => t.scan(state, projection, filters, limit).await)
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        table!(self, t => t.insert_into(state, input).await)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableIndex {
    pub column: String,
    pub ascending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: TensorType,
    pub row_shape: Option<Dyn>,
    pub required: bool,
}

impl Column {
    pub fn builder(name: impl Into<String>, data_type: TensorType) -> ColumnBuilder {
        ColumnBuilder {
            name: name.into(),
            data_type,
            row_shape: None,
            required: false,
        }
    }

    pub fn new(name: impl Into<String>, data_type: TensorType) -> Self {
        Self {
            name: name.into(),
            data_type,
            row_shape: None,
            required: false,
        }
    }

    pub(crate) fn arrow_field(&self) -> Field {
        tensor_schema(
            self.name.clone(),
            self.data_type.clone(),
            self.row_shape.clone(),
            !self.required,
        )
    }
}

impl<S: Into<String>> From<(S, TensorType)> for Column {
    fn from((name, dtype): (S, TensorType)) -> Self {
        Self::new(name, dtype)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnBuilder {
    name: String,
    data_type: TensorType,
    row_shape: Option<Dyn>,
    required: bool,
}

impl ColumnBuilder {
    pub fn new(name: impl Into<String>, data_type: TensorType) -> Self {
        Self {
            name: name.into(),
            data_type,
            row_shape: None,
            required: false,
        }
    }

    pub fn row_shape<S: IntoShape>(mut self, row_shape: S) -> Self {
        self.row_shape = Some(row_shape.into_shape().as_dyn());
        self
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn build(self) -> Column {
        Column {
            name: self.name,
            data_type: self.data_type,
            row_shape: self.row_shape,
            required: self.required,
        }
    }
}

impl From<ColumnBuilder> for Column {
    fn from(value: ColumnBuilder) -> Self {
        value.build()
    }
}
