use std::sync::Arc;

use arrow_schema::Field;
pub use arrow_schema::Schema as ArrowSchema;
use synapse_tensor::{tensor_schema, Dyn, IntoShape, Shape, TensorType};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    inner: Arc<ArrowSchema>,
}

impl Schema {
    #[must_use]
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    fn new(inner: ArrowSchema) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.inner
    }
}

#[derive(Debug)]
pub struct SchemaBuilder {
    fields: Vec<Field>,
}

impl SchemaBuilder {
    fn new() -> Self {
        Self { fields: Vec::new() }
    }

    #[must_use]
    pub fn field<S>(&mut self, name: S) -> FieldBuilder<'_, ()>
    where
        S: Into<String>,
    {
        FieldBuilder::new(self, name.into())
    }

    pub fn build(&mut self) -> Schema {
        let inner = ArrowSchema::new(self.fields.clone());
        Schema::new(inner)
    }
}

#[derive(Debug)]
pub struct FieldBuilder<'a, T> {
    schema: &'a mut SchemaBuilder,
    name: String,
    data_type: T,
    row_shape: Option<Dyn>,
    nullable: bool,
}

impl<'a> FieldBuilder<'a, ()> {
    fn new(schema: &'a mut SchemaBuilder, name: String) -> Self {
        Self {
            schema,
            name,
            data_type: (),
            row_shape: None,
            nullable: true,
        }
    }

    #[must_use]
    pub fn data_type(self, dtype: TensorType) -> FieldBuilder<'a, TensorType> {
        FieldBuilder {
            schema: self.schema,
            name: self.name,
            data_type: dtype,
            row_shape: self.row_shape,
            nullable: self.nullable,
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
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

impl<'a> FieldBuilder<'a, TensorType> {
    pub fn finish(self) -> &'a mut SchemaBuilder {
        let field = tensor_schema(self.name, self.data_type, self.row_shape, self.nullable);
        self.schema.fields.push(field);
        self.schema
    }
}
