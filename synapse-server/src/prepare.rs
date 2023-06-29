use std::{fmt::Debug, sync::Arc};

use dashmap::DashMap;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    execution::context::SessionState,
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    plan: LogicalPlan,
    state: SessionState,
    handle: String,
}

impl PreparedStatement {
    pub async fn new(ctx: &SessionContext, query: &str) -> crate::Result<Self> {
        let state = ctx.state();
        let plan = state.create_logical_plan(query).await?;
        let plan = state.optimize(&plan)?;

        let handle = if let LogicalPlan::Prepare(prepare) = &plan {
            prepare.name.clone()
        } else {
            return Err(crate::ServerError::InvalidPrepareQuery(query.to_string()).into());
        };

        Ok(Self {
            state,
            plan,
            handle,
        })
    }

    pub fn handle(&self) -> &str {
        &self.handle
    }

    pub fn schema(&self) -> SchemaRef {
        (**self.plan.schema()).clone().into()
    }

    pub fn parameter_schema(&self) -> crate::Result<Option<SchemaRef>> {
        let params = self.plan.get_parameter_types()?;
        let mut fields = vec![];
        for (k, v) in params {
            match v {
                Some(dtype) => {
                    fields.push(Field::new(k, dtype, true));
                }
                None => return Ok(None),
            }
        }

        Ok(Some(Arc::new(Schema::new(fields))))
    }
}

#[derive(Clone)]
pub struct PreparedStatements {
    statements: DashMap<String, PreparedStatement>,
    ctx: SessionContext,
}

impl Debug for PreparedStatements {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatements")
            .field("statements", &self.statements)
            .finish_non_exhaustive()
    }
}

impl PreparedStatements {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            statements: DashMap::new(),
            ctx,
        }
    }

    pub fn remove(&self, name: &str) {
        self.statements.remove(name);
    }

    pub fn insert(&self, name: String, statement: PreparedStatement) {
        self.statements.insert(name, statement);
    }

    pub fn get(&self, name: &str) -> Option<PreparedStatement> {
        self.statements.get(name).map(|plan| plan.clone())
    }
}
