mod publisher;

use std::{collections::VecDeque, fmt::Debug, sync::Arc, task::Poll};

use arrow_flight::{
    decode::FlightRecordBatchStream,
    error::FlightError,
    sql::{client::FlightSqlServiceClient, Any, Command, ProstMessageExt, TicketStatementQuery},
    Ticket,
};
use datafusion::{
    arrow::record_batch::RecordBatch, logical_expr::LogicalPlan, prelude::SessionContext,
};
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
};
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use prost::Message;
use synapse_engine::{
    lazy::{Lazy, LazyBackend},
    Schema,
};
use tonic::transport::Channel;

use crate::{
    gen::{self, engine_service_client::EngineServiceClient},
    remote::RemoteExtensionCodec,
};

use self::publisher::FlightPublisher;

#[derive(Clone)]
pub struct SynapseClient {
    flight: FlightSqlServiceClient<Channel>,
    engine: EngineServiceClient<Channel>,
    ctx: Arc<SessionContext>,
    codec: RemoteExtensionCodec,
}

impl Debug for SynapseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SynapseClient")
            .field("flight", &self.flight)
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

impl SynapseClient {
    pub fn new(channel: Channel) -> Self {
        let flight = FlightSqlServiceClient::new(channel.clone());
        let engine = EngineServiceClient::new(channel);
        let ctx = Arc::new(SessionContext::new());
        let codec = RemoteExtensionCodec::default();
        Self {
            flight,
            engine,
            ctx,
            codec,
        }
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

    pub async fn query<S: Into<String>>(&mut self, query: S) -> crate::Result<Lazy> {
        let info = self.flight.execute(query.into(), None).await?;
        let ticket = match info.endpoint.len() {
            0 => Err(crate::ClientError::MissingEndpoint),
            1 => info.endpoint[0]
                .ticket
                .as_ref()
                .ok_or_else(|| crate::ClientError::MissingTicket),
            _ => unimplemented!(),
        }?;
        let msg = Any::decode(&*ticket.ticket)?;
        let raw_plan = match Command::try_from(msg)? {
            Command::TicketStatementQuery(ticket) => ticket.statement_handle,
            cmd => {
                return Err(FlightError::DecodeError(format!(
                    "unexpected response command: {:?}",
                    cmd
                ))
                .into())
            }
        };
        let plan = logical_plan_from_bytes_with_extension_codec(&raw_plan, &self.ctx, &self.codec)?;
        Ok(Lazy::new(plan, Box::new(RemoteBackend(self.clone()))))
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

#[derive(Debug)]
struct RemoteBackend(SynapseClient);

#[tonic::async_trait]
impl LazyBackend for RemoteBackend {
    async fn stream(
        &mut self,
        plan: &LogicalPlan,
    ) -> crate::Result<BoxStream<'static, crate::Result<RecordBatch>>> {
        let statement_handle = logical_plan_to_bytes_with_extension_codec(plan, &self.0.codec)?;
        let ticket = Ticket {
            ticket: TicketStatementQuery { statement_handle }
                .as_any()
                .encode_to_vec()
                .into(),
        };
        let stream = self
            .0
            .flight
            .do_get(ticket)
            .await?
            .map_err(FlightError::from);
        Ok(FlightRecordBatchStream::new_from_flight_data(stream)
            .map_err(crate::Error::from)
            .boxed())
    }
}
