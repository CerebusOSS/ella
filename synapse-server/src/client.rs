mod publisher;

use std::{collections::VecDeque, task::Poll};

use arrow_flight::{
    decode::FlightRecordBatchStream, error::FlightError, sql::client::FlightSqlServiceClient,
    FlightInfo,
};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use synapse_engine::Schema;
use tonic::transport::Channel;

use crate::gen::{self, engine_service_client::EngineServiceClient};

use self::publisher::FlightPublisher;

#[derive(Debug, Clone)]
pub struct SynapseClient {
    flight: FlightSqlServiceClient<Channel>,
    engine: EngineServiceClient<Channel>,
}

impl SynapseClient {
    pub fn new(channel: Channel) -> Self {
        let flight = FlightSqlServiceClient::new(channel.clone());
        let engine = EngineServiceClient::new(channel);
        Self { flight, engine }
    }

    pub async fn schema(&mut self, topic: &str) -> crate::Result<Schema> {
        let resp = self
            .engine
            .get_topic(gen::TopicId::from(topic.to_string()))
            .await
            .map_err(crate::ClientError::from)?
            .into_inner();
        Schema::try_from(resp.schema.unwrap_or_default())
    }

    // pub async fn topics(&mut self) -> crate::Result<()> {
    //     self.engine.list_topics(Empty).await.map_err(crate::ClientError::from)?;
    // }

    // pub async fn topics(&mut self) -> crate::Result<()> {
    //     let resp = self.flight.get_tables(CommandGetTables::default()).await?;
    //     let mut stream = self.get_stream(resp).await?;
    //     while let Some(batch) = stream.try_next().await? {
    //         println!("{:?}", batch);
    //     }
    //     Ok(())
    // }

    pub async fn publish(&self, topic: &str) -> crate::Result<FlightPublisher> {
        FlightPublisher::try_new(self.clone(), topic).await
    }

    pub async fn query<S: Into<String>>(&mut self, query: S) -> crate::Result<FlightStream> {
        let info = self.flight.execute(query.into(), None).await?;
        self.get_stream(info).await
    }

    async fn get_stream(&mut self, info: FlightInfo) -> crate::Result<FlightStream> {
        let mut stream = FlightStream::new();
        for ep in &info.endpoint {
            if !ep.location.is_empty() {
                unimplemented!()
            }
            if let Some(ticket) = ep.ticket.clone() {
                let resp = self.flight.do_get(ticket).await?.map_err(FlightError::from);
                stream.add_partition(FlightRecordBatchStream::new_from_flight_data(resp));
            }
        }
        Ok(stream)
    }
}

#[derive(Debug)]
pub struct FlightStream {
    partitions: VecDeque<FlightRecordBatchStream>,
}

impl FlightStream {
    pub fn new() -> Self {
        Self {
            partitions: VecDeque::new(),
        }
    }
    pub fn add_partition(&mut self, part: FlightRecordBatchStream) {
        self.partitions.push_back(part);
    }
}

impl Stream for FlightStream {
    type Item = crate::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(part) = self.partitions.front_mut() {
                match futures::ready!(part.poll_next_unpin(cx)) {
                    Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                    None => {
                        self.partitions.pop_front();
                    }
                }
            } else {
                return Poll::Ready(None);
            }
        }
    }
}
