pub mod client;
mod convert;
pub mod server;
pub mod table;

pub use tonic;

pub use synapse_common::{
    error::{ClientError, ServerError},
    Error, Result,
};

pub(crate) mod gen {
    tonic::include_proto!("synapse.engine");
}
