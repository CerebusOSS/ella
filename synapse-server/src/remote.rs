use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::{DataFusionError, Result as DfResult},
    execution::context::SessionState,
    logical_expr::TableType,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use synapse_engine::{
    catalog::{Catalog, TopicId},
    Engine, Topic,
};

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
        let id: TopicId =
            serde_json::from_slice(buf).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Arc::new(RemoteTopic { schema, id }))
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        let topic = node.as_any().downcast_ref::<RemoteTopic>().ok_or_else(|| {
            DataFusionError::Internal("failed to encode table provider".to_string())
        })?;

        serde_json::to_writer(buf, topic.id())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SynapseExtensionCodec {
    catalog: Arc<Catalog>,
}

impl SynapseExtensionCodec {
    pub fn new(engine: &Engine) -> Self {
        Self {
            catalog: engine.catalog(),
        }
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
        let topic: TopicId =
            serde_json::from_slice(buf).map_err(|err| DataFusionError::External(Box::new(err)))?;
        self.catalog
            .topic(topic.clone())
            .ok_or_else(|| DataFusionError::Plan(format!("topic {} not found", topic)))
            .map(|topic| topic as Arc<_>)
    }

    fn try_encode_table_provider(
        &self,
        node: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        let topic = node.as_any().downcast_ref::<Topic>().ok_or_else(|| {
            DataFusionError::Internal("failed to encode table provider".to_string())
        })?;

        serde_json::to_writer(buf, topic.id())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(())
    }
}

#[derive(Debug)]
struct RemoteTopic {
    id: TopicId,
    schema: SchemaRef,
}

impl RemoteTopic {
    pub fn id(&self) -> &TopicId {
        &self.id
    }
}

#[tonic::async_trait]
impl TableProvider for RemoteTopic {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "remote tables can't be scanned".to_string(),
        ))
    }
}
