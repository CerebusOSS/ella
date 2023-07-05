mod synapse;
pub mod topic;

pub use synapse::{EngineBuilder, Synapse};
pub use synapse_common as common;
pub use synapse_common::{now, row::Row, time, Error, Result, TensorType, Time};
pub use synapse_engine as engine;
pub use synapse_engine::Schema;
pub use synapse_server as server;
pub use synapse_tensor as tensor;
pub use synapse_tensor::{frame, tensor};

#[macro_export]
macro_rules! row {
    ($($value:expr),+ $(,)*) => {
        $crate::Row($crate::now(), ($($value),+))
    };
}

pub async fn connect(addr: &str) -> crate::Result<Synapse> {
    Synapse::connect(addr).await
}

pub fn start(root: impl Into<String>) -> EngineBuilder {
    Synapse::start(root)
}
