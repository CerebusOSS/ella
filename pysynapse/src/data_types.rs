use pyo3::prelude::*;
use synapse::{
    runtime::Schema,
    tensor::{Dyn, TensorType},
};

pub(crate) fn add_module(py: Python<'_>, parent: &PyModule) -> PyResult<()> {
    let m = PyModule::new(py, "data_types")?;
    assign_mod_types(m)?;

    m.add_class::<PyField>()?;
    m.add_function(wrap_pyfunction!(field, m)?)?;

    m.add_class::<PySchema>()?;
    m.add_function(wrap_pyfunction!(schema, m)?)?;

    parent.add_submodule(m)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("synapse._internal.data_types", m)?;
    Ok(())
}

macro_rules! impl_type_wrappers {
    ($($t:tt),+) => {
        paste::paste! {
            $(
            #[pyclass]
            pub struct [<$t Type>];

            #[pymethods]
            impl [<$t Type>] {
                #[classattr]
                fn type_id() -> u8 {
                    TensorType::$t as u8
                }
            }
            )+

            fn wrap_dtype(py: Python, t: TensorType) -> Py<PyAny> {
                use TensorType::*;
                match t {
                    $(
                        $t => [<$t Type>].into_py(py),
                    )+
                }
            }

            fn assign_mod_types(m: &PyModule) -> PyResult<()> {
                $(
                    m.add_class::<[<$t Type>]>()?;
                    m.add(
                        stringify!([<$t:snake>]),
                        [<$t Type>]{},
                    )?;
                )+
                Ok(())
            }
        }

    };
}

impl_type_wrappers!(
    Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Timestamp,
    Duration
);

fn unwrap_dtype(t: &PyAny) -> PyResult<TensorType> {
    let id: u8 = t.getattr("type_id")?.extract()?;
    Ok(TensorType::from_repr(id).unwrap())
}

#[derive(Clone)]
#[pyclass(name = "Field")]
pub struct PyField {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    row_shape: Option<Vec<usize>>,
    #[pyo3(get)]
    required: bool,
    #[pyo3(get)]
    index: Option<String>,
    dtype: TensorType,
}

#[pymethods]
impl PyField {
    #[pyo3(signature = (name, dtype, row_shape=None, required=false, index=None))]
    #[new]
    fn new(
        name: String,
        #[pyo3(from_py_with = "unwrap_dtype")] dtype: TensorType,
        row_shape: Option<Vec<usize>>,
        required: bool,
        index: Option<String>,
    ) -> Self {
        Self {
            name,
            row_shape,
            required,
            index,
            dtype,
        }
    }

    #[getter]
    fn data_type(&self, py: Python) -> Py<PyAny> {
        wrap_dtype(py, self.dtype.clone())
    }
}

#[pyfunction]
#[pyo3(signature = (name, dtype, row_shape=None, required=false, index=None))]
fn field(
    name: String,
    #[pyo3(from_py_with = "unwrap_dtype")] dtype: TensorType,
    row_shape: Option<Vec<usize>>,
    required: bool,
    index: Option<String>,
) -> PyField {
    PyField::new(name, dtype, row_shape, required, index)
}

#[derive(Clone)]
#[pyclass(name = "Schema")]
pub struct PySchema {
    #[pyo3(get)]
    fields: Vec<PyField>,
}

#[pymethods]
impl PySchema {
    #[new]
    fn new(fields: Vec<PyField>) -> Self {
        Self { fields }
    }
}

impl PySchema {
    pub fn to_schema(&self) -> crate::Result<Schema> {
        let mut builder = Schema::builder();
        for f in &self.fields {
            let mut field = builder
                .field(&f.name)
                .data_type(f.dtype.clone())
                .required(f.required);

            if let Some(shape) = f.row_shape.as_deref() {
                field = field.row_shape(Dyn::from(shape));
            }
            if let Some(index) = &f.index {
                let ascending = if index == "ascending" {
                    true
                } else if index == "descending" {
                    false
                } else {
                    return Err(crate::Error::InvalidIndexMode(index.clone()));
                };
                field = field.index(ascending);
            }
            field.finish();
        }
        Ok(builder.build())
    }
}

#[pyfunction]
fn schema(fields: Vec<PyField>) -> PySchema {
    PySchema::new(fields)
}
