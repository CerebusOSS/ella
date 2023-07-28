mod catalog;
mod ella;
mod schema;
pub mod table;

pub mod shape {
    pub use ella_common::shape::{
        Axis, Const, Dyn, Flat, IndexValue, Indexer, IntoShape, NdimAdd, NdimMax, RemoveAxis, Shape,
    };
}

pub mod tensor {
    pub use ella_tensor::{
        frame, tensor, DataFrame, Tensor, Tensor1, Tensor2, Tensor3, Tensor4, TensorD,
    };
}

pub use ella::Ella;
use ella::{CreateElla, OpenElla};
pub use ella_common as common;
pub use ella_common::{now, row::Row, time, Error, Result, TensorType, Time};
pub use ella_derive::RowFormat;
pub use ella_engine as engine;
pub use ella_server as server;
pub use engine::config::{self, EllaConfig};
pub use table::Table;

#[doc(hidden)]
pub mod derive {
    pub use datafusion::arrow::{array::ArrayRef, datatypes::Field};
}

#[macro_export]
macro_rules! row {
    ($($value:expr),+ $(,)*) => {
        $crate::Row($crate::now(), ($($value),+))
    };
}

pub async fn connect(addr: impl AsRef<str>) -> crate::Result<Ella> {
    Ella::connect(addr).await
}

pub fn open(root: impl Into<String>) -> OpenElla {
    Ella::open(root)
}

pub fn create(root: impl Into<String>, config: impl Into<EllaConfig>) -> CreateElla {
    Ella::create(root, config)
}
