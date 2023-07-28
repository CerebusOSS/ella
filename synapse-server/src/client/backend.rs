use std::{fmt::Debug, sync::Arc, task::Poll};

use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    sql::{ProstMessageExt, TicketStatementQuery},
    FlightData, Ticket,
};
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::provider_as_source,
    error::DataFusionError,
    logical_expr::LogicalPlanBuilder,
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use synapse_engine::{lazy::LazyBackend, registry::TableRef, table::info::ViewInfo, Plan};

use super::SynapseClient;

#[derive(Debug, Clone)]
pub(crate) struct RemoteBackend(SynapseClient);

impl From<SynapseClient> for RemoteBackend {
    fn from(value: SynapseClient) -> Self {
        Self(value)
    }
}

#[tonic::async_trait]
impl LazyBackend for RemoteBackend {
    async fn stream(&self, plan: &Plan) -> crate::Result<SendableRecordBatchStream> {
        let statement_handle = plan.to_bytes().into();
        let ticket = Ticket {
            ticket: TicketStatementQuery { statement_handle }
                .as_any()
                .encode_to_vec()
                .into(),
        };
        let stream = self
            .0
            .flight
            .clone()
            .do_get(ticket)
            .await?
            .map_err(FlightError::from);
        Ok(Box::pin(RemoteStream::new(stream).await?))
    }

    async fn create_view(
        &self,
        table: TableRef<'static>,
        info: ViewInfo,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<Plan> {
        let table = self
            .0
            .clone()
            .create_table(table, info.into(), if_not_exists, or_replace)
            .await?;
        let plan = LogicalPlanBuilder::scan(
            table.id().clone(),
            provider_as_source(Arc::new(table.as_stub()?)),
            None,
        )?
        .build()?;
        Ok(Plan::from_stub(plan))
    }
}

struct RemoteStream {
    inner: FlightDataDecoder,
    schema: SchemaRef,
}

impl RemoteStream {
    async fn new<S>(inner: S) -> Result<Self, FlightError>
    where
        S: Stream<Item = Result<FlightData, FlightError>> + Send + 'static,
    {
        let mut inner = FlightDataDecoder::new(inner);
        while let Some(item) = inner.try_next().await? {
            match item.payload {
                DecodedPayload::Schema(schema) => return Ok(Self { inner, schema }),
                DecodedPayload::RecordBatch(_) => {
                    return Err(FlightError::protocol("received record batch before schema"))
                }
                DecodedPayload::None => {}
            }
        }
        Err(FlightError::protocol(
            "stream closed without sending schema",
        ))
    }
}

impl Stream for RemoteStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match futures::ready!(self.inner.poll_next_unpin(cx)) {
                Some(Ok(item)) => {
                    if let DecodedPayload::RecordBatch(batch) = item.payload {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
                Some(Err(error)) => {
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))))
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for RemoteStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
