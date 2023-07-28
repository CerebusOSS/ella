pub mod client;
mod convert;
pub mod server;
pub mod table;

pub use tonic;

pub use ella_common::{
    error::{ClientError, ServerError},
    Error, Result,
};

#[allow(non_snake_case)]
pub(crate) mod gen {
    tonic::include_proto!("ella.engine");
}
