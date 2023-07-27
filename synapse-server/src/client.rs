mod publisher;

use std::{collections::VecDeque, fmt::Debug, sync::Arc, task::Poll};

use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder, FlightRecordBatchStream},
    error::FlightError,
    sql::{client::FlightSqlServiceClient, Any, Command, ProstMessageExt, TicketStatementQuery},
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
use synapse_engine::{
    lazy::{Lazy, LazyBackend},
    registry::{Id, TableRef},
    table::info::{TableInfo, ViewInfo},
    Plan, SynapseConfig,
};
use tonic::{
    codegen::InterceptedService,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::Channel,
};

use crate::{
    gen::{self, engine_service_client::EngineServiceClient},
    table::RemoteTable,
};

pub use self::publisher::FlightPublisher;

#[derive(Debug, Clone)]
pub struct SynapseClient {
    flight: FlightSqlServiceClient<Channel>,
    engine: EngineServiceClient<InterceptedService<Channel, BearerAuth>>,
}

impl SynapseClient {
    pub async fn connect(channel: Channel) -> crate::Result<Self> {
        let mut flight = FlightSqlServiceClient::new(channel.clone());
        let token = flight.handshake("", "").await?;
        let token =
            String::from_utf8(token.into()).map_err(|_| crate::ClientError::InvalidToken)?;
        flight.set_token(token.clone());

        let auth = BearerAuth::try_new(&token)?;
        let engine = EngineServiceClient::with_interceptor(channel, auth);
        Ok(Self { flight, engine })
    }

    pub async fn create_table(
        &self,
        table: TableRef<'_>,
        info: TableInfo,
        if_not_exists: bool,
        or_replace: bool,
    ) -> crate::Result<RemoteTable> {
        let mut this = self.clone();
        let req = gen::CreateTableReq {
            table: Some(table.into()),
            info: Some(info.try_into()?),
            if_not_exists,
            or_replace,
        };
        let resp = this
            .engine
            .create_table(req)
            .await
            .map_err(|err| crate::ClientError::Server(err))?
            .into_inner();

        Ok(RemoteTable::new(
            resp.table.expect("expected table ID in response").into(),
            resp.info
                .expect("expected table info in response")
                .try_into()?,
            this,
        ))
    }

    pub async fn get_table(&self, table: TableRef<'_>) -> crate::Result<Option<RemoteTable>> {
        let mut this = self.clone();
        let resp = this
            .engine
            .get_table(gen::TableRef::from(table))
            .await
            .map_err(|err| crate::ClientError::Server(err))?
            .into_inner();
        Ok(match (&resp.table, &resp.info) {
            (Some(table), Some(info)) => Some(RemoteTable::new(
                table.clone().into(),
                info.clone().try_into()?,
                this,
            )),
            (None, None) => None,
            (_, _) => panic!(
                "expected empty or fully-populated response, got: {:?}",
                resp
            ),
        })
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
        let plan = Plan::from_bytes(&raw_plan)?;
        Ok(Lazy::new(plan, Arc::new(RemoteBackend(self.clone()))))
    }

    pub async fn set_config(&mut self, config: &SynapseConfig, global: bool) -> crate::Result<()> {
        let scope = if global {
            gen::ConfigScope::Cluster
        } else {
            gen::ConfigScope::Connection
        };
        self.engine
            .set_config(gen::Config {
                scope: scope.into(),
                config: serde_json::to_vec(config)?,
            })
            .await
            .map_err(crate::ClientError::Server)?;
        Ok(())
    }

    pub async fn get_config(&mut self, global: bool) -> crate::Result<SynapseConfig> {
        let scope = if global {
            gen::ConfigScope::Cluster
        } else {
            gen::ConfigScope::Connection
        };
        let resp = self
            .engine
            .get_config(gen::GetConfigReq {
                scope: scope.into(),
            })
            .await
            .map_err(crate::ClientError::Server)?;
        Ok(serde_json::from_slice(&resp.into_inner().config)?)
    }

    pub async fn use_catalog<'a>(&mut self, catalog: impl Into<Id<'a>>) -> crate::Result<()> {
        let catalog: Id<'static> = catalog.into().into_owned();

        let config = self
            .get_config(false)
            .await?
            .into_builder()
            .default_catalog(catalog)
            .build();
        self.set_config(&config, false).await?;
        Ok(())
    }

    pub async fn use_schema<'a>(&mut self, schema: impl Into<Id<'a>>) -> crate::Result<()> {
        let schema: Id<'static> = schema.into().into_owned();

        let config = self
            .get_config(false)
            .await?
            .into_builder()
            .default_schema(schema)
            .build();
        self.set_config(&config, false).await?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct FlightStream {
    partitions: VecDeque<FlightRecordBatchStream>,
}

impl FlightStream {
    pub fn new() -> Self {
        Self::default()
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

#[derive(Debug, Clone)]
struct RemoteBackend(SynapseClient);

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
                Some(Ok(item)) => match item.payload {
                    DecodedPayload::RecordBatch(batch) => return Poll::Ready(Some(Ok(batch))),
                    _ => {}
                },
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

#[derive(Debug, Clone)]
struct BearerAuth {
    payload: MetadataValue<Ascii>,
}

impl BearerAuth {
    fn try_new(token: &str) -> crate::Result<Self> {
        let payload = format!("Bearer {token}")
            .parse()
            .map_err(|_| crate::ClientError::InvalidToken)?;
        Ok(Self { payload })
    }
}

impl Interceptor for BearerAuth {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.payload.clone());
        Ok(request)
    }
}
