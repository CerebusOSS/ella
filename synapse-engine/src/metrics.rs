mod load_monitor;
#[cfg(feature = "metrics")]
mod server;

pub use load_monitor::{InstrumentedBuffer, MonitorLoadExt, ReportLoad};
#[cfg(feature = "metrics")]
pub use server::MetricsServer;

use once_cell::sync::Lazy;
use std::sync::Mutex;

#[cfg(feature = "metrics")]
pub(crate) static METRICS: Lazy<Mutex<prometheus_client::registry::Registry>> =
    Lazy::new(|| Mutex::new(prometheus_client::registry::Registry::default()));
