use std::{fmt::Debug, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::{DataFusionError, Result as DfResult},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::{cluster::SynapseCluster, registry::TableId, table::SynapseTable};

fn encode_table(
    node: Arc<dyn datafusion::datasource::TableProvider>,
    buf: &mut Vec<u8>,
) -> datafusion::error::Result<()> {
    if let Some(table) = node.as_any().downcast_ref::<TableStub>() {
        serde_json::to_writer(buf, table.table())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(())
    } else if let Some(table) = node.as_any().downcast_ref::<SynapseTable>() {
        serde_json::to_writer(buf, table.id())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(())
    } else {
        Err(DataFusionError::Internal(
            "failed to encode table provider".to_string(),
        ))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RemoteExtensionCodec {}

impl LogicalExtensionCodec for RemoteExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion::logical_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        todo!()
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        Err(DataFusionError::NotImplemented(format!(
            "unable to encode extension node: {:?}",
            node.node
        )))
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        schema: SchemaRef,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let table: TableId =
            serde_json::from_slice(buf).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Arc::new(TableStub { schema, table }))
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        encode_table(node, buf)
    }
}

#[derive(Clone)]
pub struct SynapseExtensionCodec {
    cluster: Arc<SynapseCluster>,
}

impl Debug for SynapseExtensionCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SynapseExtensionCodec")
            .finish_non_exhaustive()
    }
}

impl SynapseExtensionCodec {
    pub fn new(cluster: Arc<SynapseCluster>) -> Self {
        Self { cluster }
    }
}

impl LogicalExtensionCodec for SynapseExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion::logical_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        todo!()
    }

    fn try_encode(
        &self,
        _node: &datafusion::logical_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        todo!()
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _schema: SchemaRef,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::datasource::TableProvider>> {
        let table: TableId =
            serde_json::from_slice(buf).map_err(|err| DataFusionError::External(Box::new(err)))?;

        self.cluster
            .catalog(&table.catalog)
            .and_then(|catalog| catalog.schema(&table.schema))
            .and_then(|schema| schema.table(&table.table))
            .ok_or_else(|| DataFusionError::Plan(format!("table {} not found", table)))
            .map(|t| t as Arc<_>)
    }

    fn try_encode_table_provider(
        &self,
        node: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        encode_table(node, buf)
    }
}

#[derive(Debug, Clone)]
pub struct TableStub {
    table: TableId<'static>,
    schema: SchemaRef,
}

impl TableStub {
    pub fn new(table: TableId<'static>, schema: SchemaRef) -> Self {
        Self { table, schema }
    }

    pub fn table(&self) -> &TableId {
        &self.table
    }
}

#[async_trait::async_trait]
impl TableProvider for TableStub {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "stub tables can't be scanned".to_string(),
        ))
    }
}
