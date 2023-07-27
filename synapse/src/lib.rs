mod synapse;
pub mod table;

pub mod shape {
    pub use synapse_common::shape::{
        Axis, Const, Dyn, Flat, IndexValue, Indexer, IntoShape, NdimAdd, NdimMax, RemoveAxis, Shape,
    };
}

pub mod tensor {
    pub use synapse_tensor::{frame, tensor, Tensor, Tensor1, Tensor2, Tensor3, Tensor4, TensorD};
}

pub use engine::config::{self, SynapseConfig};
pub use synapse::Synapse;
use synapse::{CreateSynapse, OpenSynapse};
pub use synapse_common as common;
pub use synapse_common::{now, row::Row, time, Error, Result, TensorType, Time};
pub use synapse_derive::RowFormat;
pub use synapse_engine as engine;
pub use synapse_server as server;
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

pub async fn connect(addr: impl AsRef<str>) -> crate::Result<Synapse> {
    Synapse::connect(addr).await
}

pub fn open(root: impl Into<String>) -> OpenSynapse {
    Synapse::open(root)
}

pub fn create(root: impl Into<String>, config: impl Into<SynapseConfig>) -> CreateSynapse {
    Synapse::create(root, config)
}
