use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Weak,
    },
    thread::{self, JoinHandle},
};

use dashmap::{mapref::entry::Entry, DashMap};
use flume::r#async::{RecvStream, SendSink};
use futures::{Sink, SinkExt, Stream, StreamExt};
use once_cell::sync::Lazy;
use synapse_time::Duration;

pub trait ReportLoad {
    fn items(&self) -> usize;
    fn capacity(&self) -> Option<usize>;
}

impl<T> ReportLoad for flume::Sender<T> {
    fn items(&self) -> usize {
        self.len()
    }

    fn capacity(&self) -> Option<usize> {
        self.capacity()
    }
}

impl<'a, T> ReportLoad for SendSink<'a, T> {
    fn items(&self) -> usize {
        self.len()
    }

    fn capacity(&self) -> Option<usize> {
        self.capacity()
    }
}

impl<'a, T> ReportLoad for RecvStream<'a, T> {
    fn items(&self) -> usize {
        self.len()
    }

    fn capacity(&self) -> Option<usize> {
        self.capacity()
    }
}

#[derive(Debug, Clone)]
pub struct Instrument<T> {
    inner: T,
    handle: LoadHandle,
}

#[allow(dead_code)]
impl<T> Instrument<T>
where
    T: ReportLoad,
{
    pub fn new(inner: T, name: &str) -> Self {
        let handle = LoadMonitor::register(name, inner.capacity());
        Self { inner, handle }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    fn report_load(&self) {
        self.handle.report(self.inner.items());
    }
}

#[allow(dead_code)]
impl<T> Instrument<flume::Sender<T>> {
    pub fn send(&self, msg: T) -> Result<(), flume::SendError<T>> {
        self.report_load();
        self.inner.send(msg)
    }

    pub fn try_send(&self, msg: T) -> Result<(), flume::TrySendError<T>> {
        self.report_load();
        self.inner.try_send(msg)
    }

    pub fn send_async(&self, msg: T) -> flume::r#async::SendFut<'_, T> {
        self.report_load();
        self.inner.send_async(msg)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }

    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T> Stream for Instrument<T>
where
    T: Stream + Unpin + ReportLoad,
{
    type Item = T::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.report_load();
        self.inner.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T, I> Sink<I> for Instrument<T>
where
    T: Sink<I> + Unpin + ReportLoad,
{
    type Error = T::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.report_load();
        self.inner.poll_ready_unpin(cx)
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.inner.start_send_unpin(item)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.report_load();
        self.inner.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_close_unpin(cx)
    }
}

#[derive(Debug, Clone)]
pub struct LoadMonitor {
    _handle: Arc<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl Drop for LoadMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

impl LoadMonitor {
    pub fn start(rate: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let run_stop = stop.clone();
        let _handle = Arc::new(thread::spawn(move || Self::run(rate, run_stop)));
        Self { _handle, stop }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn register(name: &str, capacity: Option<usize>) -> LoadHandle {
        let load = Arc::new(AtomicUsize::new(0));
        let value = LoadRef {
            capacity,
            items: Arc::downgrade(&load),
        };

        match Self::load_map().entry(name.to_string()) {
            Entry::Occupied(entry) => {
                if entry.get().items.upgrade().is_some() {
                    tracing::warn!(name, "registering source with load monitor but a source with that name is still active");
                }
                entry.replace_entry(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
        LoadHandle(load)
    }

    fn run(rate: Duration, stop: Arc<AtomicBool>) {
        let rate = rate.unsigned_abs();
        while !stop.load(Ordering::Relaxed) {
            thread::sleep(rate);

            for item in Self::load_map().iter() {
                if let Some(m) = item.value().measure() {
                    tracing::trace!(
                        target: "load_monitor",
                        name=item.key(),
                        items=m.items,
                        capacity=m.capacity,
                        load=m.load,
                    );
                }
            }
        }
    }

    fn load_map() -> &'static DashMap<String, LoadRef> {
        static MAP: Lazy<DashMap<String, LoadRef>> = Lazy::new(DashMap::new);
        &MAP
    }
}

#[derive(Debug, Clone)]
pub struct LoadHandle(Arc<AtomicUsize>);

impl LoadHandle {
    pub fn report(&self, items: usize) {
        self.0.store(items, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct LoadRef {
    capacity: Option<usize>,
    items: Weak<AtomicUsize>,
}

impl LoadRef {
    fn measure(&self) -> Option<LoadMeasurement> {
        self.items.upgrade().map(|value| {
            let items = value.load(Ordering::Relaxed);
            let capacity = self.capacity;
            let load = capacity.map(|cap| items as f64 / cap as f64);
            LoadMeasurement {
                items,
                capacity,
                load,
            }
        })
    }
}

#[derive(Debug, Clone)]
struct LoadMeasurement {
    pub items: usize,
    pub capacity: Option<usize>,
    pub load: Option<f64>,
}
