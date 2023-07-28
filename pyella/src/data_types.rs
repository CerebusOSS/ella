use std::fmt::Display;

use ella::TensorType;
use pyo3::prelude::*;

pub(crate) fn add_module(py: Python<'_>, parent: &PyModule) -> PyResult<()> {
    let dt = PyModule::new(py, "data_types")?;
    let td = PyModule::new(py, "type_defs")?;
    assign_mod_types(dt, td)?;

    let mods = py.import("sys")?.getattr("modules")?;
    parent.add_submodule(dt)?;
    parent.add_submodule(td)?;

    mods.set_item("ella._internal.data_types", dt)?;
    mods.set_item("ella._internal.type_defs", td)?;
    Ok(())
}

macro_rules! impl_type_wrappers {
    ($($t:tt),+) => {
        paste::paste! {
            $(
            #[pyclass]
            #[derive(Debug, Clone)]
            pub struct [<$t Type>];

            impl Display for [<$t Type>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, stringify!([<$t:lower>]))
                }
            }

            #[pymethods]
            impl [<$t Type>] {
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
                        $t => [<$t Type>].into_py(py),
                    )+
                }
            }

            fn assign_mod_types(ty_mod: &PyModule, def_mod: &PyModule) -> PyResult<()> {
                $(
                    ty_mod.add_class::<[<$t Type>]>()?;
                    def_mod.add(
                        stringify!([<$t:lower>]),
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
    Duration, String
);

pub(crate) fn unwrap_dtype(t: &PyAny) -> PyResult<TensorType> {
    let id: u8 = t.getattr("type_id")?.extract()?;
    Ok(TensorType::from_repr(id).unwrap())
}
