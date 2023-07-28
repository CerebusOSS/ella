mod backend;
mod publisher;

use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use arrow_flight::{
    error::FlightError,
    sql::{client::FlightSqlServiceClient, Any, Command},
};
use ella_engine::{
    lazy::Lazy,
    registry::{Id, SchemaRef, TableRef},
    table::info::TableInfo,
    EllaConfig, Plan,
};
use prost::Message;
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

use self::backend::RemoteBackend;
pub use self::publisher::FlightPublisher;

#[derive(Debug, Clone)]
pub struct EllaClient {
    flight: FlightSqlServiceClient<Channel>,
    engine: EngineServiceClient<InterceptedService<Channel, BearerAuth>>,
    config: Arc<Mutex<EllaConfig>>,
}

impl EllaClient {
    pub async fn connect(channel: Channel) -> crate::Result<Self> {
        let mut flight = FlightSqlServiceClient::new(channel.clone());
        let token = flight.handshake("", "").await?;
        let token =
            String::from_utf8(token.into()).map_err(|_| crate::ClientError::InvalidToken)?;
        flight.set_token(token.clone());

        let auth = BearerAuth::try_new(&token)?;
        let mut engine = EngineServiceClient::with_interceptor(channel, auth);

        let resp = engine
            .get_config(gen::GetConfigReq {
                scope: gen::ConfigScope::Connection.into(),
            })
            .await
            .map_err(crate::ClientError::Server)?;
        let config = serde_json::from_slice(&resp.into_inner().config)?;
        let config = Arc::new(Mutex::new(config));
        Ok(Self {
            flight,
            engine,
            config,
        })
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
            .map_err(crate::ClientError::Server)?
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
            .map_err(crate::ClientError::Server)?
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

    pub async fn query<S: Into<String>>(&self, query: S) -> crate::Result<Lazy> {
        let mut this = self.clone();

        let info = this.flight.execute(query.into(), None).await?;
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
        Ok(Lazy::new(plan, Arc::new(RemoteBackend::from(this))))
    }

    pub fn config(&self) -> EllaConfig {
        self.config.lock().unwrap().clone()
    }

    pub async fn set_config(&mut self, config: EllaConfig, persist: bool) -> crate::Result<()> {
        let scope = if persist {
            gen::ConfigScope::Cluster
        } else {
            gen::ConfigScope::Connection
        };
        let raw_config = serde_json::to_vec(&config)?;
        *self.config.lock().unwrap() = config;

        self.engine
            .set_config(gen::Config {
                scope: scope.into(),
                config: raw_config,
            })
            .await
            .map_err(crate::ClientError::Server)?;
        Ok(())
    }

    pub async fn use_catalog<'a>(&mut self, catalog: impl Into<Id<'a>>) -> crate::Result<()> {
        let catalog: Id<'static> = catalog.into().into_owned();
        let config = self
            .config()
            .into_builder()
            .default_catalog(catalog)
            .build();
        self.set_config(config, false).await?;

        Ok(())
    }

    pub async fn use_schema<'a>(&mut self, schema: impl Into<Id<'a>>) -> crate::Result<()> {
        let schema: Id<'static> = schema.into().into_owned();
        let config = self.config().into_builder().default_schema(schema).build();
        self.set_config(config, false).await?;

        Ok(())
    }

    pub async fn create_catalog<'a>(
        &mut self,
        catalog: impl Into<Id<'a>>,
        if_not_exists: bool,
    ) -> crate::Result<()> {
        let catalog: Id<'a> = catalog.into();
        self.engine
            .create_catalog(gen::CreateCatalogReq {
                catalog: catalog.to_string(),
                if_not_exists,
            })
            .await
            .map_err(crate::ClientError::Server)?;
        Ok(())
    }

    pub async fn create_schema<'a>(
        &mut self,
        schema: impl Into<SchemaRef<'a>>,
        if_not_exists: bool,
    ) -> crate::Result<()> {
        let schema: SchemaRef<'a> = schema.into();
        self.engine
            .create_schema(gen::CreateSchemaReq {
                catalog: schema.catalog.map(|c| c.to_string()),
                schema: schema.schema.to_string(),
                if_not_exists,
            })
            .await
            .map_err(crate::ClientError::Server)?;
        Ok(())
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
