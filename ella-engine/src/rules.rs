use std::{marker::PhantomData, sync::Arc};

use arrow_schema::DataType;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRewriter},
        DFSchema,
    },
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{
        BinaryExpr, ExprSchemable, LogicalPlan, Projection, UserDefinedLogicalNode,
        UserDefinedLogicalNodeCore,
    },
    optimizer::analyzer::AnalyzerRule,
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::Expr,
};
use ella_common::{TensorType, TensorValue};
use ella_tensor::{Column, Dyn, Tensor};

#[derive(Debug, Default)]
pub struct FixTensorOps {}

impl AnalyzerRule for FixTensorOps {
    fn analyze(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<datafusion::logical_expr::LogicalPlan> {
        plan.transform_up(&analyze_internal)
        // Ok(plan)
    }

    fn name(&self) -> &str {
        "rewrite_tensor_ops"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    Ok(match plan {
        LogicalPlan::Projection(Projection {
            mut expr,
            input,
            schema,
            ..
        }) => {
            let mut modified = false;
            for ex in &mut expr {
                match ex {
                    Expr::BinaryExpr(BinaryExpr { left, right, op }) => {
                        let (left_ty, left_shape) = ella_type(&schema, &left);
                        let (right_ty, right_shape) = ella_type(&schema, &right);
                        match (left_shape, right_shape) {
                            (Some(left), Some(right)) => todo!(),
                            (Some(left), None) => todo!(),
                            (None, Some(right)) => todo!(),
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            let out =
                LogicalPlan::Projection(Projection::try_new_with_schema(expr, input, schema)?);
            if modified {
                Transformed::Yes(out)
            } else {
                Transformed::No(out)
            }
        }
        _ => Transformed::No(plan),
    })
}

fn ella_type(schema: &DFSchema, expr: &Expr) -> (TensorType, Option<Dyn>) {
    todo!()
}

// struct TensorOpsRewriter {}

// impl TreeNodeRewriter for TensorOpsRewriter {
//     type N = ;

//     fn mutate(&mut self, node: Self::N) -> datafusion::error::Result<Self::N> {
//         todo!()
//     }
// }

#[derive(Debug, PartialEq, Eq, Hash)]
struct TensorCompatNode {
    inputs: Vec<LogicalPlan>,
    args: Vec<Expr>,
    op: TensorUnaryExpr,
}

impl UserDefinedLogicalNodeCore for TensorCompatNode {
    fn name(&self) -> &str {
        todo!()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect::<Vec<_>>()
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        todo!()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.args.clone()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            inputs: inputs.to_vec(),
            args: exprs.to_vec(),
            op: self.op.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TensorExpr {
    Unary(TensorUnaryExpr),
    Binary(TensorBinaryExpr),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TensorUnaryExpr {
    Sin,
    Cos,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TensorBinaryExpr {}

pub struct TensorPlanner {}

#[async_trait::async_trait]
impl ExtensionPlanner for TensorPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        // if let Some(node) = node.as_any().downcast_ref::<TensorUnaryNode>() {}
        todo!()
    }
}

struct TensorUnaryOp<T: TensorValue, O: TensorValue> {
    func: Arc<dyn Fn(Tensor<T, Dyn>) -> Tensor<O, Dyn> + Send + Sync + 'static>,
    _type: PhantomData<(T, O)>,
}

pub struct TensorExec {}

impl ExecutionPlan for TensorExec {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        todo!()
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        todo!()
    }

    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }
}

// impl<T, O> TensorNode for TensorUnaryOp<T, O>
// where
//     T: TensorValue,
//     O: TensorValue,
// {
//     fn inputs(&self) -> &[TensorField] {
//         &[TensorField {
//             dtype: T::TENSOR_TYPE,
//             nullable: T::NULLABLE,
//         }]
//     }

//     fn output(&self) -> TensorField {
//         TensorField {
//             dtype: O::TENSOR_TYPE,
//             nullable: O::NULLABLE,
//         }
//     }

//     fn call(
//         &self,
//         inputs: &[Arc<dyn ColumnData + 'static>],
//     ) -> crate::Result<Arc<dyn ColumnData + 'static>> {
//         todo!()
//         // let res = (self.func)(
//         //     inputs[0]
//         // );
//     }
// }

// trait TensorNode {
//     fn inputs(&self) -> &[TensorField];
//     fn output(&self) -> TensorField;
//     fn call(
//         &self,
//         inputs: &[Arc<dyn ColumnData + 'static>],
//     ) -> crate::Result<Arc<dyn ColumnData + 'static>>;
// }

// #[derive(Debug, Clone)]
// struct TensorField {
//     dtype: TensorType,
//     nullable: bool,
// }

// fn build_tensor_exec_unary(
//     node: &TensorUnaryNode,
// ) -> Arc<dyn ExecutionPlan> {
//     match node.op {
//         TensorUnaryExpr::Sin => todo!(),
//         TensorUnaryExpr::Cos => todo!(),
//     }
// }

// fn tensor_unary_op<F>(
//     col: &dyn ColumnData,
//     f: F,
// ) -> crate::Result<Arc<dyn ColumnData + 'static>>
//     where F: Fn()
// {

// }
