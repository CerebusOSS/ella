#[cfg(feature = "runtime")]
pub use synapse_runtime as runtime;
pub use synapse_tensor as tensor;
pub use synapse_time as time;

pub use synapse_tensor::{frame, tensor};
pub use synapse_time::now;

#[cfg(feature = "runtime")]
pub use runtime::{Runtime, RuntimeConfig};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("synapse tensor error")]
    Tensor(#[from] tensor::Error),
    #[cfg(feature = "runtime")]
    #[error("synapse runtime error")]
    Runtime(#[from] runtime::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
