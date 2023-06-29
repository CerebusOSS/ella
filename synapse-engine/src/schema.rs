use std::sync::Arc;

pub use arrow_schema::Schema as ArrowSchema;
use arrow_schema::{Field, SortOptions};
use datafusion::{
    parquet::format::SortingColumn, physical_expr::PhysicalSortExpr,
    physical_plan::expressions::Column,
};
use synapse_tensor::{tensor_schema, Dyn, IntoShape, Shape, TensorType};

use crate::util::parquet::parquet_compat_schema;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexColumn {
    pub name: String,
    pub column: usize,
    pub ascending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    inner: Arc<ArrowSchema>,
    index_columns: Vec<IndexColumn>,
    parquet: Option<Arc<ArrowSchema>>,
}

impl Schema {
    pub fn new(arrow_schema: Arc<ArrowSchema>, index_columns: Vec<IndexColumn>) -> Self {
        let parquet = parquet_compat_schema(arrow_schema.clone());
        Self {
            inner: arrow_schema,
            index_columns,
            parquet,
        }
    }

    #[must_use]
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.inner
    }

    pub fn parquet_schema(&self) -> Option<&Arc<ArrowSchema>> {
        self.parquet.as_ref()
    }

    pub fn index_columns(&self) -> &[IndexColumn] {
        &self.index_columns
    }

    pub(crate) fn output_ordering(&self) -> Option<Vec<PhysicalSortExpr>> {
        if self.index_columns.is_empty() {
            None
        } else {
            let cols = self
                .index_columns
                .iter()
                .map(|col| PhysicalSortExpr {
                    expr: Arc::new(Column::new(&col.name, col.column)),
                    options: SortOptions {
                        descending: !col.ascending,
                        nulls_first: true,
                    },
                })
                .collect::<Vec<_>>();
            Some(cols)
        }
    }

    pub(crate) fn sorting_columns(&self) -> Option<Vec<SortingColumn>> {
        if self.index_columns.is_empty() {
            None
        } else {
            let cols = self
                .index_columns
                .iter()
                .map(|col| SortingColumn::new(col.column as i32, !col.ascending, false))
                .collect::<Vec<_>>();
            Some(cols)
        }
    }
}

#[derive(Debug)]
pub struct SchemaBuilder {
    fields: Vec<Field>,
    index_columns: Vec<IndexColumn>,
}

impl SchemaBuilder {
    fn new() -> Self {
        Self {
            fields: Vec::new(),
            index_columns: Vec::new(),
        }
    }

    #[must_use]
    pub fn field<S>(&mut self, name: S) -> FieldBuilder<'_, ()>
    where
        S: Into<String>,
    {
        FieldBuilder::new(self, name.into())
    }

    pub fn build(&mut self) -> Schema {
        let inner = Arc::new(ArrowSchema::new(self.fields.clone()));
        let parquet = parquet_compat_schema(inner.clone());
        Schema {
            inner,
            parquet,
            index_columns: self.index_columns.clone(),
        }
    }
}

#[derive(Debug)]
pub struct FieldBuilder<'a, T> {
    schema: &'a mut SchemaBuilder,
    name: String,
    data_type: T,
    row_shape: Option<Dyn>,
    required: bool,
    index: Option<IndexColumn>,
}

impl<'a> FieldBuilder<'a, ()> {
    fn new(schema: &'a mut SchemaBuilder, name: String) -> Self {
        Self {
            schema,
            name,
            data_type: (),
            row_shape: None,
            required: false,
            index: None,
        }
    }

    #[must_use]
    pub fn data_type(self, dtype: TensorType) -> FieldBuilder<'a, TensorType> {
        FieldBuilder {
            schema: self.schema,
            name: self.name,
            data_type: dtype,
            row_shape: self.row_shape,
            required: self.required,
            index: self.index,
        }
    }
}

impl<'a, T> FieldBuilder<'a, T> {
    #[must_use]
    pub fn row_shape<S>(mut self, shape: S) -> Self
    where
        S: IntoShape,
    {
        self.row_shape = Some(shape.into_shape().as_dyn());
        self
    }

    #[must_use]
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    pub fn index(mut self, ascending: bool) -> Self {
        self.index = Some(IndexColumn {
            name: self.name.clone(),
            column: self.schema.fields.len(),
            ascending,
        });
        self
    }
}

impl<'a> FieldBuilder<'a, TensorType> {
    pub fn finish(self) -> &'a mut SchemaBuilder {
        let field = tensor_schema(
            self.name.clone(),
            self.data_type,
            self.row_shape,
            !self.required,
        );
        self.schema.fields.push(field);
        if let Some(index) = self.index {
            self.schema.index_columns.push(index);
        }
        self.schema
    }
}
