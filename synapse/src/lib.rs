#[cfg(feature = "pyo3")]
mod py;

#[cfg(feature = "engine")]
pub use synapse_engine as engine;
pub use synapse_tensor as tensor;
pub use synapse_time as time;

pub use synapse_tensor::{frame, tensor};
pub use synapse_time::now;

#[cfg(feature = "engine")]
pub use engine::{Engine, EngineConfig};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("synapse tensor error")]
    Tensor(#[from] tensor::Error),
    #[cfg(feature = "engine")]
    #[error("synapse engine error")]
    Engine(#[from] engine::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
