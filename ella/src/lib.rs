//! # Getting Started
//!
//! Add `ella` to your `Cargo.toml` file:
//!
//! ```toml
//! ella = "0.1.3"
//! ```
//!
//! You may also want to install the [ella CLI tools](https://crates.io/crates/ella-cli/).
//!
//! ## Windows
//!
//! Building `ella` on Windows requires the Protobuf compiler to be installed.
//! [You can download a pre-built binary from the Protobuf repo.](https://github.com/protocolbuffers/protobuf/releases)
//!
//! # Usage
//!
//! You can access ella by either starting a new instance or connecting to an existing instance.
//!
//! Start a new instance by opening or creating a datastore:
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! let el = ella::open("file:///path/to/db")
//!     .or_create(ella::Config::default())
//!     .and_serve("localhost:50052")?
//!     .await?;
//! # ella::Result::Ok(())
//! # }).unwrap();
//!
//! ```
//!
//! Connect to an existing instance using `ella::connect`:
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! let el = ella::connect("http://localhost:50052").await?;
//! # ella::Result::Ok(())
//! # }).unwrap();
//! ```

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

pub use crate::ella::Ella;
use crate::ella::{CreateElla, OpenElla};
pub use ella_common as common;
pub use ella_common::{now, row::Row, time, Error, Result, TensorType, Time};
pub use ella_derive::RowFormat;
pub use ella_engine as engine;
pub use ella_server as server;
pub use engine::{
    config::{EllaConfig as Config, EllaConfigBuilder as ConfigBuilder},
    Path,
};
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

/// Connect to an ella API server at `addr`.
pub async fn connect(addr: impl AsRef<str>) -> crate::Result<Ella> {
    Ella::connect(addr).await
}

/// Open the datastore at `root`, if one exists.
///
/// Returns an error if `root` is inaccessible or doesn't contain a valid datastore.
///
/// `open` returns a future which provides methods to customize opening behavior.
pub fn open(root: impl Into<String>) -> OpenElla {
    Ella::open(root)
}

/// Create a new datastore at `root`.
///
/// Returns an error if `root` is inaccessible or a datastore already exists.
///
/// `create` returns a future which provides methods to customize creation behavior.
pub fn create(root: impl Into<String>, config: impl Into<Config>) -> CreateElla {
    Ella::create(root, config)
}
