use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::{
    datasource::TableProvider,
    error::Result as DfResult,
    execution::context::SessionState,
    logical_expr::{LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType},
    parquet::format::SortingColumn,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use once_cell::sync::OnceCell;

use crate::{engine::SynapseState, registry::TableId, Path, TableConfig};

use super::{
    info::{SynapseTableInfo, ViewInfo},
    topic::shard::ShardSet,
};

#[derive(Debug)]
pub struct SynapseView {
    info: ViewInfo,
    table_info: SynapseTableInfo,
    plan: OnceCell<LogicalPlan>,
    config: TableConfig,
}

impl SynapseView {
    pub(crate) fn new(
        id: TableId<'static>,
        info: ViewInfo,
        state: &SynapseState,
        resolve: bool,
    ) -> crate::Result<Self> {
        if info.materialized() {
            todo!()
        }
        let table_info = info.table_info(id, state)?;
        let plan = OnceCell::new();
        let config = table_info.config().clone();

        let this = Self {
            info,
            table_info,
            plan,
            config,
        };
        if resolve {
            this.resolve(state)?;
        }
        Ok(this)
    }

    pub fn table(&self) -> &TableId<'static> {
        self.table_info.id()
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    pub fn path(&self) -> &Path {
        self.table_info.path()
    }

    pub(crate) fn info(&self) -> &ViewInfo {
        &self.info
    }

    pub(crate) fn file_schema(&self) -> SchemaRef {
        self.table_info
            .parquet_schema()
            .cloned()
            .unwrap_or_else(|| self.table_info.arrow_schema().clone())
    }

    pub(crate) fn sort(&self) -> Option<Vec<SortingColumn>> {
        self.table_info.sorting_cols().cloned()
    }

    pub(crate) fn resolve(&self, state: &SynapseState) -> crate::Result<()> {
        let plan = self.info.plan().resolve(state)?;
        let _ = self.plan.set(plan);
        Ok(())
    }

    pub(crate) fn shards(&self) -> Option<&Arc<ShardSet>> {
        if self.info.materialized() {
            todo!()
        } else {
            None
        }
    }

    fn logical_plan(&self) -> &LogicalPlan {
        self.plan.get().unwrap()
    }
}

#[async_trait::async_trait]
impl TableProvider for SynapseView {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_info.arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.info.definition()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        Some(self.logical_plan())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.logical_plan();
        let plan = if let Some(projection) = projection {
            let current_projection = (0..logical_plan.schema().fields().len()).collect::<Vec<_>>();
            if projection == &current_projection {
                logical_plan.clone()
            } else {
                let fields = projection
                    .iter()
                    .map(|&i| Expr::Column(logical_plan.schema().field(i).qualified_column()));
                LogicalPlanBuilder::from(logical_plan.clone())
                    .project(fields)?
                    .build()?
            }
        } else {
            logical_plan.clone()
        };

        let mut plan = LogicalPlanBuilder::from(plan);
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(filter) = filter {
            plan = plan.filter(filter)?;
        }

        if let Some(limit) = limit {
            plan = plan.limit(0, Some(limit))?;
        }

        state.create_physical_plan(&plan.build()?).await
    }
}
