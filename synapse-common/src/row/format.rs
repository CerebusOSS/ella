use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use datafusion::arrow::{
    array::ArrayRef,
    datatypes::{Field, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};

pub trait RowFormat: Debug + Clone + 'static {
    const COLUMNS: usize;

    type Builder: RowBatchBuilder<Self>;
    type View: RowFormatView<Self>;

    fn builder(fields: &[Arc<Field>]) -> crate::Result<Self::Builder>;
    fn view(rows: usize, fields: &[Arc<Field>], arrays: &[ArrayRef]) -> crate::Result<Self::View>;
}

pub trait RowBatchBuilder<R>: Debug + Clone + 'static {
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

pub trait RowFormatView<R>:
    Debug + IntoIterator<Item = R, IntoIter = RowViewIter<R, Self>> + Clone + 'static
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn row(&self, i: usize) -> R;
    unsafe fn row_unchecked(&self, i: usize) -> R;

    fn iter(&self) -> RowViewIter<R, Self> {
        self.clone().into_iter()
    }
}

#[derive(Debug, Clone)]
pub struct RowViewIter<R, V> {
    view: V,
    _row: PhantomData<R>,
    i: usize,
}

impl<R, V> RowViewIter<R, V> {
    pub fn new(view: V) -> Self {
        Self {
            view,
            _row: PhantomData,
            i: 0,
        }
    }
}

impl<R, V> Iterator for RowViewIter<R, V>
where
    V: RowFormatView<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.view.len() {
            let item = unsafe { self.view.row_unchecked(self.i) };
            self.i += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.view.len(), Some(self.view.len()))
    }
}

impl<R, V: RowFormatView<R>> ExactSizeIterator for RowViewIter<R, V> {}
