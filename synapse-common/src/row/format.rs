use std::{fmt::Debug, sync::Arc};

use datafusion::arrow::{
    array::ArrayRef,
    datatypes::{Field, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};

pub trait RowFormat: Debug + Clone + 'static {
    type Builder: RowBatchBuilder<Self>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder>;
}

pub trait RowBatchBuilder<R>: Debug + Clone + 'static {
    const COLUMNS: usize;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn push(&mut self, row: R);
    fn build_columns(&mut self) -> crate::Result<Vec<ArrayRef>>;

    fn build(&mut self, schema: SchemaRef) -> crate::Result<RecordBatch> {
        let opts = RecordBatchOptions::new().with_row_count(Some(self.len()));
        let columns = self.build_columns()?;
        Ok(RecordBatch::try_new_with_options(schema, columns, &opts)?)
    }
}
