use crate::catalog::TopicId;
use flume::r#async::{RecvStream, SendSink};
use futures::{Sink, SinkExt, Stream, StreamExt};
use once_cell::sync::Lazy;
#[cfg(feature = "metrics")]
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{family::Family, gauge::Gauge},
};

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

pub trait MonitorLoadExt: Sized {
    fn monitor_load(self, topic: &TopicId, buffer: &str) -> InstrumentedBuffer<Self>;
}

impl<T> MonitorLoadExt for T
where
    T: ReportLoad,
{
    fn monitor_load(self, topic: &TopicId, buffer: &str) -> InstrumentedBuffer<Self> {
        InstrumentedBuffer::new(self, topic, buffer)
    }
}

#[derive(Debug, Clone)]
pub struct InstrumentedBuffer<T> {
    inner: T,
    labels: LoadLabels,
}

#[allow(dead_code)]
impl<T> InstrumentedBuffer<T>
where
    T: ReportLoad,
{
    pub fn new(inner: T, topic: &TopicId, buffer: &str) -> Self {
        let labels = LoadLabels {
            topic: topic.to_string(),
            buffer: buffer.to_string(),
        };
        #[cfg(feature = "metrics")]
        if let Some(cap) = inner.capacity() {
            LOAD_CAPACITY.get_or_create(&labels).set(cap as i64);
        }

        Self { inner, labels }
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
        #[cfg(feature = "metrics")]
        LOAD_ITEMS
            .get_or_create(&self.labels)
            .set(self.inner.items() as i64);
    }
}

#[allow(dead_code)]
impl<T> InstrumentedBuffer<flume::Sender<T>> {
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

impl<T> Stream for InstrumentedBuffer<T>
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

impl<T, I> Sink<I> for InstrumentedBuffer<T>
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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "metrics", derive(EncodeLabelSet))]
struct LoadLabels {
    pub topic: String,
    pub buffer: String,
}

#[cfg(feature = "metrics")]
static LOAD_ITEMS: Lazy<Family<LoadLabels, Gauge>> = Lazy::new(|| {
    let m = Family::default();
    crate::metrics::METRICS.lock().unwrap().register(
        "buffer_load_items",
        "number of items in the buffer",
        m.clone(),
    );
    m
});

#[cfg(feature = "metrics")]
static LOAD_CAPACITY: Lazy<Family<LoadLabels, Gauge>> = Lazy::new(|| {
    let m = Family::default();
    crate::metrics::METRICS.lock().unwrap().register(
        "buffer_load_capacity",
        "maximum number of items in the buffer",
        m.clone(),
    );
    m
});
