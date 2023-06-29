use std::{fmt::Debug, sync::Arc};

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    sql::{CommandStatementUpdate, ProstMessageExt},
    FlightData, FlightDescriptor,
};
use datafusion::arrow::record_batch::RecordBatch;
use flume::r#async::SendSink;
use futures::{Sink, SinkExt, StreamExt};
use prost::Message;
use synapse_common::row::{RowFormat, RowSink};
use synapse_engine::Schema;
use tokio::task::JoinHandle;

use super::SynapseClient;

pub struct FlightPublisher {
    send: SendSink<'static, RecordBatch>,
    handle: JoinHandle<crate::Result<()>>,
    schema: Arc<Schema>,
}

impl Debug for FlightPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightPublisher")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl FlightPublisher {
    pub async fn try_new(mut client: SynapseClient, topic: &str) -> crate::Result<Self> {
        let schema = Arc::new(client.schema(topic).await?);

        let (send, recv) = flume::bounded(1);
        let send = send.into_sink();
        let descriptor = FlightDescriptor::new_cmd(
            CommandStatementUpdate {
                query: format!("copy this to {}", topic),
                transaction_id: None,
            }
            .as_any()
            .encode_to_vec(),
        );
        let header = futures::stream::once(async { FlightData::new().with_descriptor(descriptor) });
        let stream = FlightDataEncoderBuilder::new()
            .build(recv.into_stream().map(Ok))
            .map(|res| res.unwrap());

        let handle = tokio::spawn(async move {
            let mut resp = client.flight.do_put(header.chain(stream)).await?;
            resp.message().await.map_err(crate::ClientError::from)?;
            Ok(())
        });
        Ok(Self {
            send,
            handle,
            schema,
        })
    }

    pub fn rows<R: RowFormat>(self, buffer: usize) -> crate::Result<RowSink<R, Self>> {
        let schema = self.schema.arrow_schema().clone();
        RowSink::try_new(self, schema, buffer)
    }
}

impl Sink<RecordBatch> for FlightPublisher {
    type Error = crate::Error;

    #[inline]
    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.send
            .poll_ready_unpin(cx)
            .map_err(|_| crate::ClientError::TopicClosed.into())
    }

    #[inline]
    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: RecordBatch,
    ) -> Result<(), Self::Error> {
        self.send
            .start_send_unpin(item)
            .map_err(|_| crate::ClientError::TopicClosed.into())
    }

    #[inline]
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.send
            .poll_flush_unpin(cx)
            .map_err(|_| crate::ClientError::TopicClosed.into())
    }

    #[inline]
    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.send
            .poll_close_unpin(cx)
            .map_err(|_| crate::ClientError::TopicClosed.into())
    }
}
