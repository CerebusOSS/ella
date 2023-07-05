use std::{fmt::Debug, pin::Pin, sync::Arc};

use crate::{synapse::SynapseInner, Synapse};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{Sink, SinkExt};
use synapse_common::row::{RowFormat, RowSink};
use synapse_engine::{catalog::TopicId, Schema};
use synapse_server::client::{FlightPublisher, SynapseClient};

#[derive(Debug)]
pub struct TopicRef<'a> {
    topic: TopicId,
    inner: &'a Synapse,
}

impl<'a> TopicRef<'a> {
    pub(crate) fn new(inner: &'a Synapse, topic: TopicId) -> Self {
        Self { inner, topic }
    }

    pub async fn get(&self) -> crate::Result<Option<Topic>> {
        use TopicInner::*;
        match &self.inner.0 {
            SynapseInner::Local { engine, .. } => Ok(engine
                .topic(self.topic.clone())
                .get()
                .map(|topic| Topic(Local(topic)))),
            SynapseInner::Remote(client) => {
                let mut client = client.clone();
                Ok(client.schema(self.topic.as_ref()).await?.map(|schema| {
                    Topic(Remote {
                        client,
                        schema: Arc::new(schema),
                        topic: self.topic.clone(),
                    })
                }))
            }
        }
    }

    pub async fn get_or_create(&self, schema: Schema) -> crate::Result<Topic> {
        match self.get().await? {
            Some(topic) => Ok(topic),
            None => self.create(schema).await,
        }
    }

    pub async fn create(&self, schema: Schema) -> crate::Result<Topic> {
        use TopicInner::*;
        match &self.inner.0 {
            SynapseInner::Local { engine, .. } => {
                let topic = engine.topic(self.topic.clone()).create(schema).await?;
                Ok(Topic(Local(topic)))
            }
            SynapseInner::Remote(client) => {
                let mut client = client.clone();
                client
                    .create_topic(self.topic.as_ref(), schema.clone())
                    .await?;
                Ok(Topic(Remote {
                    client,
                    topic: self.topic.clone(),
                    schema: Arc::new(schema),
                }))
            }
        }
    }
}

#[derive(Debug)]
pub struct Topic(TopicInner);

#[derive(Debug)]
enum TopicInner {
    Local(Arc<crate::engine::Topic>),
    Remote {
        client: SynapseClient,
        topic: TopicId,
        schema: Arc<Schema>,
    },
}

impl Topic {
    pub fn schema(&self) -> &Arc<Schema> {
        use TopicInner::*;
        match &self.0 {
            Local(topic) => topic.schema(),
            Remote { schema, .. } => schema,
        }
    }

    pub fn publish(&self) -> Publisher {
        use TopicInner::*;
        match &self.0 {
            Local(topic) => Publisher::new(topic.publish(), topic.schema().clone()),
            Remote {
                client,
                topic,
                schema,
            } => Publisher::new(
                FlightPublisher::new_with_schema(client.clone(), topic.as_ref(), schema.clone()),
                schema.clone(),
            ),
        }
    }
}

pub struct Publisher {
    inner: Pin<Box<dyn Sink<RecordBatch, Error = crate::Error> + Send + 'static>>,
    schema: Arc<Schema>,
}

impl Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl Publisher {
    fn new<S>(inner: S, schema: Arc<Schema>) -> Self
    where
        S: Sink<RecordBatch, Error = crate::Error> + Send + 'static,
    {
        Self {
            inner: Box::pin(inner),
            schema,
        }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn rows<R: RowFormat>(self, buffer: usize) -> crate::Result<RowSink<R>> {
        let schema = self.schema.arrow_schema().clone();
        RowSink::try_new(self, schema, buffer)
    }
}

impl Sink<RecordBatch> for Publisher {
    type Error = crate::Error;

    #[inline]
    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready_unpin(cx)
    }

    #[inline]
    fn start_send(mut self: Pin<&mut Self>, item: RecordBatch) -> Result<(), Self::Error> {
        self.inner.start_send_unpin(item)
    }

    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_flush_unpin(cx)
    }

    #[inline]
    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_close_unpin(cx)
    }
}
