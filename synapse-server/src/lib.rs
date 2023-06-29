pub mod client;
mod convert;
mod prepare;
pub mod server;
mod ticket;

pub use tonic;

pub use synapse_common::{
    error::{ClientError, ServerError},
    Error, Result,
};

pub(crate) mod gen {
    tonic::include_proto!("synapse.engine");
}
