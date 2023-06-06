use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    arrow::{
        array::{ArrayData, ArrayRef, FixedSizeListArray},
        compute,
        record_batch::RecordBatch,
    },
    error::Result as DfResult,
    physical_plan::{
        expressions::{CastExpr, Column},
        projection::ProjectionExec,
        ExecutionPlan, PhysicalExpr,
    },
};

pub fn parquet_compat_schema(schema: Arc<Schema>) -> Option<Arc<Schema>> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let dt = datatype_to_parquet_compat(f.data_type());
            Arc::new((**f).clone().with_data_type(dt))
        })
        .collect::<Vec<_>>();
    let out = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    if out != schema {
        Some(out)
    } else {
        None
    }
}

fn datatype_to_parquet_compat(dt: &DataType) -> DataType {
    use DataType::*;
    match dt {
        Duration(_) => Int64,
        &FixedSizeList(ref inner, size) => {
            let inner = Arc::new(
                (**inner)
                    .clone()
                    .with_data_type(datatype_to_parquet_compat(inner.data_type())),
            );
            FixedSizeList(inner, size)
        }
        List(inner) => {
            let inner = Arc::new(
                (**inner)
                    .clone()
                    .with_data_type(datatype_to_parquet_compat(inner.data_type())),
            );
            List(inner)
        }
        LargeList(inner) => {
            let inner = Arc::new(
                (**inner)
                    .clone()
                    .with_data_type(datatype_to_parquet_compat(inner.data_type())),
            );
            LargeList(inner)
        }
        _ => dt.clone(),
    }
}

pub fn cast_batch(batch: &RecordBatch, schema: Arc<Schema>) -> crate::Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(col, f)| cast_array_inner(col, f))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn cast_array_inner(col: &ArrayRef, f: &Arc<Field>) -> crate::Result<ArrayRef> {
    if col.data_type() != f.data_type() {
        // cast kernel doesn't support fixed-size lists for some reason, so we have to do this weird hack.
        if let DataType::FixedSizeList(inner, _size) = f.data_type() {
            let values = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("expected fixed-size list array")
                .values();
            let values = compute::cast(values, inner.data_type())?;
            let data = unsafe {
                ArrayData::builder(f.data_type().clone())
                    .len(col.len())
                    .nulls(col.nulls().cloned())
                    .add_child_data(values.to_data())
                    .build_unchecked()
            };
            Ok(Arc::new(FixedSizeListArray::from(data)))
        } else {
            Ok(compute::cast(col, f.data_type())?)
        }
    } else {
        Ok(col.clone())
    }
}

pub fn cast_batch_plan(
    plan: Arc<dyn ExecutionPlan>,
    src_schema: &Arc<Schema>,
    dst_schema: &Arc<Schema>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let columns = src_schema
        .fields()
        .iter()
        .zip(dst_schema.fields().iter())
        .enumerate()
        .map(|(i, (src, dst))| {
            let col = Arc::new(Column::new(dst.name(), i));
            if src == dst {
                (col as Arc<dyn PhysicalExpr>, dst.name().clone())
            } else {
                (
                    Arc::new(CastExpr::new(col, dst.data_type().clone(), None)) as Arc<_>,
                    dst.name().clone(),
                )
            }
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(columns, plan)?))
}
