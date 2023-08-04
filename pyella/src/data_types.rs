use std::{fmt::Display, fs::File, io::Write, path::Path};

use ella::TensorType;
use pyo3::prelude::*;

pub(crate) fn add_datatypes(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyDataType>()?;
    assign_mod_types(py, m)?;

    Ok(())
}

#[derive(Debug, Clone, Copy)]
#[pyclass(subclass, name = "DataType")]
pub struct PyDataType;

macro_rules! impl_type_wrappers {
    ($($t:tt),+) => {
        paste::paste! {
            $(
            #[pyclass(extends = PyDataType)]
            #[derive(Debug, Clone)]
            pub struct [<$t Type>];

            impl Display for [<$t Type>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, stringify!([<$t:lower>]))
                }
            }

            #[pymethods]
            impl [<$t Type>] {
                #[new]
                fn new() -> PyClassInitializer<Self> {
                    PyClassInitializer::from(PyDataType)
                        .add_subclass(Self)
                }

                #[classattr]
                fn type_id() -> u8 {
                    TensorType::$t as u8
                }

                fn __repr__(&self) -> String {
                    format!("{:?}", self)
                }

                fn __str__(&self) -> String {
                    self.to_string()
                }
            }
            )+

            pub(crate) fn wrap_dtype(py: Python, t: TensorType) -> Py<PyAny> {
                use TensorType::*;
                match t {
                    $(
                        $t => py.get_type::<[<$t Type>]>().call((), None).unwrap().to_object(py),
                    )+
                }
            }

            fn assign_mod_types(_py: Python, m: &PyModule) -> PyResult<()> {
                $(
                    m.add_class::<[<$t Type>]>()?;
                )+
                Ok(())
            }

            /// Generate minimal Python type definitions for ella datatypes.
            pub fn generate_py_types(root: &Path) -> std::io::Result<()> {
                let mut names = vec![$((stringify!([<$t:lower>]), stringify!([<$t Type>]))),+];
                for (ty, _) in &mut names {
                    if ty == &"bool" {
                        *ty = "bool_";
                    }
                }
                write_py_type_info(root, &names)?;
                write_py_type_wrapper(root, &names)?;
                Ok(())
            }
        }

    };
}

impl_type_wrappers!(
    Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Timestamp,
    Duration, String
);

pub(crate) fn unwrap_dtype(t: &PyAny) -> PyResult<TensorType> {
    let id: u8 = t.getattr("type_id")?.extract()?;
    Ok(TensorType::from_repr(id).unwrap())
}

/// Generate Python type stub file.
///
/// Structure:
/// ```text
/// __all__ = [
///     <variables>,
///     <classes>,
/// ]
/// <class definitions>
/// <variable definitions>
/// ```
fn write_py_type_info(root: &Path, names: &[(&str, &str)]) -> std::io::Result<()> {
    let mut f = File::create(root.join("types.pyi"))?;
    f.write_all(b"__all__ = [\n")?;
    for (ty, _) in names {
        f.write_fmt(format_args!("\t\"{ty}\",\n"))?;
    }
    f.write_all(b"\t\"DataType\",\n")?;
    for (_, cls) in names {
        f.write_fmt(format_args!("\t\"{cls}\",\n"))?;
    }
    f.write_all(b"]\n\n")?;
    f.write_all(b"class DataType: ...\n")?;
    for (_, cls) in names {
        f.write_fmt(format_args!("class {cls}(DataType): ...\n"))?;
    }

    for (ty, cls) in names {
        f.write_fmt(format_args!("{ty}: {cls}\n"))?;
    }
    Ok(())
}

/// Generate Python wrapper file.
///
/// Structure:
/// ```text
/// __all__ = [
///     <variables>,
///     <classes>,
/// ]
/// <import classes>
/// <define variables>
/// ```
fn write_py_type_wrapper(root: &Path, names: &[(&str, &str)]) -> std::io::Result<()> {
    let mut f = File::create(root.join("types.py"))?;

    f.write_all(b"__all__ = [\n")?;
    for (ty, _) in names {
        f.write_fmt(format_args!("\t\"{ty}\",\n"))?;
    }
    f.write_all(b"\t\"DataType\",\n")?;
    for (_, cls) in names {
        f.write_fmt(format_args!("\t\"{cls}\",\n"))?;
    }
    f.write_all(b"]\n\n")?;
    f.write_all(b"from ella._internal import (\n\tDataType,\n")?;
    for (_, cls) in names {
        f.write_fmt(format_args!("\t{cls},\n"))?;
    }
    f.write_all(b")\n\n")?;

    for (ty, cls) in names {
        f.write_fmt(format_args!("{ty}: {cls} = {cls}()\n"))?;
    }
    Ok(())
}
