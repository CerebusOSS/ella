use std::{fmt::Debug, pin::Pin, sync::Arc};

use arrow_schema::Schema;
use datafusion::{
    arrow::compute::concat_batches,
    datasource::provider_as_source,
    logical_expr::{DdlStatement, LogicalPlan, LogicalPlanBuilder},
    physical_plan::{
        execute_stream, stream::RecordBatchStreamAdapter, RecordBatchStream,
        SendableRecordBatchStream,
    },
};
use futures::TryStreamExt;
use synapse_tensor::DataFrame;

use crate::{
    engine::SynapseState,
    registry::{SchemaId, TableRef},
    table::info::{ViewBuilder, ViewInfo},
    Plan,
};

#[async_trait::async_trait]
pub trait LazyBackend: Debug + Send + Sync {
    async fn stream(&self, plan: &Plan) -> crate::Result<SendableRecordBatchStream>;

    async fn create_view(
        &self,
        table: TableRef<'static>,
        info: ViewInfo,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<Plan>;

    async fn execute(&self, plan: &Plan) -> crate::Result<DataFrame> {
        let stream = self.stream(plan).await?;
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await?;

        concat_batches(&schema, &batches)?.try_into()
    }
}

#[derive(Debug, Clone)]
pub struct LocalBackend {
    state: SynapseState,
}

impl LocalBackend {
    pub(crate) fn new(state: SynapseState) -> Self {
        Self { state }
    }
}

fn empty() -> Pin<Box<dyn RecordBatchStream + Send + 'static>> {
    Box::pin(RecordBatchStreamAdapter::new(
        Arc::new(Schema::empty()),
        futures::stream::empty(),
    ))
}

#[async_trait::async_trait]
impl LazyBackend for LocalBackend {
    async fn stream(&self, plan: &Plan) -> crate::Result<SendableRecordBatchStream> {
        let plan = plan.resolve(&self.state)?;
        match plan {
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateView(cmd) => {
                    let name = TableRef::from(cmd.name.clone());
                    let id = self.state.resolve(name.clone());
                    let plan = (*cmd.input).clone();
                    let mut info = ViewBuilder::new(Plan::from_plan(plan));
                    if let Some(definition) = cmd.definition.as_deref() {
                        info = info.definition(definition);
                    }
                    self.state
                        .create_view(id, info.build(), false, cmd.or_replace)
                        .await?;
                    Ok(empty())
                }
                DdlStatement::CreateMemoryTable(_cmd) => {
                    todo!()
                }
                DdlStatement::CreateCatalogSchema(cmd) => {
                    let id =
                        SchemaId::parse(&cmd.schema_name, self.state.default_catalog().clone());
                    self.state
                        .cluster()
                        .catalog(id.catalog.as_ref())
                        .ok_or_else(|| crate::EngineError::CatalogNotFound(id.catalog.to_string()))?
                        .create_schema(id.schema, cmd.if_not_exists)
                        .await?;
                    Ok(empty())
                }
                DdlStatement::CreateCatalog(cmd) => {
                    self.state
                        .cluster()
                        .create_catalog(&cmd.catalog_name, cmd.if_not_exists)
                        .await?;
                    Ok(empty())
                }
                DdlStatement::CreateExternalTable(_cmd) => unimplemented!(),
                DdlStatement::DropTable(cmd) => {
                    let name = TableRef::from(cmd.name.clone());
                    let id = self.state.resolve(name.clone());

                    let schema = self
                        .state
                        .cluster()
                        .catalog(&id.catalog)
                        .and_then(|catalog| catalog.schema(&id.schema));
                    match (cmd.if_exists, schema) {
                        (_, Some(schema)) => {
                            schema.drop_topic(&id.table, cmd.if_exists).await?;
                            Ok(empty())
                        }
                        (true, None) => Ok(empty()),
                        (false, None) => {
                            Err(crate::EngineError::TableNotFound(id.to_string()).into())
                        }
                    }
                }
                DdlStatement::DropView(cmd) => {
                    let name = TableRef::from(cmd.name.clone());
                    let id = self.state.resolve(name.clone());

                    let schema = self
                        .state
                        .cluster()
                        .catalog(&id.catalog)
                        .and_then(|catalog| catalog.schema(&id.schema));
                    match (cmd.if_exists, schema) {
                        (_, Some(schema)) => {
                            schema.drop_view(&id.table, cmd.if_exists).await?;
                            Ok(empty())
                        }
                        (true, None) => Ok(empty()),
                        (false, None) => {
                            Err(crate::EngineError::TableNotFound(id.to_string()).into())
                        }
                    }
                }
                DdlStatement::DropCatalogSchema(cmd) => {
                    let id =
                        SchemaId::resolve(cmd.name.clone(), self.state.default_catalog().clone());

                    let catalog = self.state.cluster().catalog(&id.catalog);
                    match (cmd.if_exists, catalog) {
                        (_, Some(catalog)) => {
                            catalog
                                .deregister(&id.schema, cmd.if_exists, cmd.cascade)
                                .await?;
                            Ok(empty())
                        }
                        (true, None) => Ok(empty()),
                        (false, None) => {
                            Err(crate::EngineError::CatalogNotFound(id.catalog.to_string()).into())
                        }
                    }
                }
            },
            LogicalPlan::Statement(_stmt) => unimplemented!(),
            LogicalPlan::DescribeTable(_desc) => todo!(),
            plan => {
                let plan = self.state.session().create_physical_plan(&plan).await?;

                Ok(execute_stream(plan, self.state.session().task_ctx())?)
            }
        }
    }

    async fn create_view(
        &self,
        table: TableRef<'static>,
        info: ViewInfo,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<Plan> {
        let id = self.state.resolve(table);
        let view = self
            .state
            .create_view(id, info, if_not_exists, or_replace)
            .await?;
        let plan = LogicalPlanBuilder::scan(view.table().clone(), provider_as_source(view), None)?
            .build()?;
        Ok(Plan::from_plan(plan))
    }
}
