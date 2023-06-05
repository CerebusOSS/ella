use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::{Arc, RwLock},
};

use datafusion::arrow::record_batch::RecordBatch;
use futures::{future::BoxFuture, stream::FuturesOrdered, Future, FutureExt, StreamExt};
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    Notify,
};

#[derive(Debug)]
pub struct ValueTracker {
    items: VecDeque<RecordBatch>,
    pending: usize,
}

impl ValueTracker {
    fn new() -> Self {
        let items = VecDeque::new();
        let pending = 0;
        Self { items, pending }
    }

    fn new_count(&self) -> usize {
        self.items.len() - self.pending
    }

    fn push(&mut self, item: RecordBatch) {
        self.items.push_front(item);
    }

    fn process(&mut self, count: usize) -> Vec<RecordBatch> {
        debug_assert!(count <= self.items.len() - self.pending);
        let start = self.pending;
        self.pending += count;
        self.items
            .iter()
            .rev()
            .skip(start)
            .take(count)
            .cloned()
            .collect::<Vec<_>>()
    }

    fn finish(&mut self, count: usize) {
        debug_assert!(count <= self.pending);
        self.items.truncate(self.items.len() - count);
        self.pending -= count;
    }

    fn values(&self) -> Vec<RecordBatch> {
        Vec::from(self.items.clone())
    }
}

#[derive(Debug)]
pub struct WorkQueueIn<T> {
    values: Arc<RwLock<ValueTracker>>,
    send: mpsc::Sender<(usize, BoxFuture<'static, T>)>,
}

impl<T> WorkQueueIn<T>
where
    T: Send + 'static,
{
    pub fn push(&self, item: RecordBatch) {
        self.values.write().unwrap().push(item);
    }

    pub fn values(&self) -> Vec<RecordBatch> {
        self.values.read().unwrap().values()
    }

    pub fn process<F, Fut>(&self, f: F) -> crate::Result<()>
    where
        F: FnOnce(Vec<RecordBatch>) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let (count, values) = {
            let mut v = self.values.write().unwrap();
            let count = v.new_count();
            (count, v.process(count))
        };

        match self.send.try_send((count, Box::pin(f(values)))) {
            Ok(_) => Ok(()),
            Err(TrySendError::Closed(_)) => Err(crate::Error::TableClosed),
            Err(TrySendError::Full(_)) => Err(crate::Error::TableQueueFull),
        }
    }
}

#[derive(Debug)]
pub struct WorkQueueOut<T> {
    values: Arc<RwLock<ValueTracker>>,
    recv: mpsc::Receiver<(usize, T)>,
    stop: Arc<Notify>,
}

impl<T> WorkQueueOut<T> {
    pub async fn ready(&mut self) -> Option<T> {
        if let Some((count, item)) = self.recv.recv().await {
            self.values.write().unwrap().finish(count);
            Some(item)
        } else {
            None
        }
    }

    pub fn close(&mut self) {
        self.stop.notify_one();
    }
}

pub fn work_queue<T>(capacity: usize) -> (WorkQueueIn<T>, WorkQueueOut<T>)
where
    T: Send + 'static,
{
    let stop = Arc::new(Notify::new());
    let (send, worker_recv) = mpsc::channel(capacity);
    let (worker_send, recv) = mpsc::channel(capacity);
    tokio::spawn(process_queue(worker_recv, worker_send, stop.clone()));

    let values = Arc::new(RwLock::new(ValueTracker::new()));
    let queue_in = WorkQueueIn {
        values: values.clone(),
        send,
    };
    let queue_out = WorkQueueOut { values, recv, stop };
    (queue_in, queue_out)
}

async fn process_queue<T>(
    mut recv: mpsc::Receiver<(usize, BoxFuture<'static, T>)>,
    send: mpsc::Sender<(usize, T)>,
    stop: Arc<Notify>,
) {
    let done = stop.notified().fuse();
    futures::pin_mut!(done);

    let mut pending = FuturesOrdered::new();
    let mut counts = VecDeque::new();
    loop {
        tokio::select! {
            item = recv.recv() => match item {
                Some((count, job)) => {
                    counts.push_back(count);
                    pending.push_back(job);
                },
                None => break,
            },
            res = pending.next(), if !pending.is_empty() => {
                let count = counts.pop_front().unwrap();
                if send.try_send((count, res.unwrap())).is_err() {
                    tracing::error!("failed to send work queue result");
                }
            },
            _ = &mut done => {
                recv.close()
            },
        }
    }
    while let Some(res) = pending.next().await {
        let count = counts.pop_front().unwrap();
        // The receiver may have closed here so errors should be ignored
        let _ = send.try_send((count, res));
    }
}
