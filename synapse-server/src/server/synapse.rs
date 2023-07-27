use crate::gen::{self, engine_service_server::EngineService};
use synapse_engine::{registry::TableRef, SynapseConfig};
use tonic::{Request, Response};

use super::auth::connection;

#[derive(Debug, Clone, Default)]
pub(crate) struct SynapseEngineService;

#[tonic::async_trait]
impl EngineService for SynapseEngineService {
    async fn get_table(
        &self,
        request: Request<gen::TableRef>,
    ) -> tonic::Result<Response<gen::ResolvedTable>> {
        let state = connection(&request)?.read();
        let table = state.resolve(request.into_inner().into());

        Ok(Response::new(match state.table(table) {
            Some(table) => gen::ResolvedTable {
                table: Some(table.id().clone().into()),
                info: Some(table.info().try_into()?),
            },
            None => gen::ResolvedTable::default(),
        }))
    }

    async fn create_table(
        &self,
        request: Request<gen::CreateTableReq>,
    ) -> tonic::Result<Response<gen::ResolvedTable>> {
        let state = connection(&request)?.read();
        let req = request.into_inner();
        let table: TableRef<'static> = req
            .table
            .ok_or_else(|| tonic::Status::invalid_argument("missing table field in request"))?
            .into();
        let table = state.resolve(table);

        let info = req
            .info
            .ok_or_else(|| tonic::Status::invalid_argument("missing table field in request"))?
            .try_into()?;
        let table = state
            .create_table(table, info, req.if_not_exists, req.or_replace)
            .await?;

        Ok(Response::new(gen::ResolvedTable {
            table: Some(table.id().clone().into()),
            info: Some(table.info().try_into()?),
        }))
    }

    async fn set_config(
        &self,
        request: Request<gen::Config>,
    ) -> tonic::Result<Response<gen::Config>> {
        let conn = connection(&request)?;
        let req = request.into_inner();
        let config: SynapseConfig = serde_json::from_slice(&req.config)
            .map_err(|err| tonic::Status::invalid_argument(format!("invalid config: {}", err)))?;

        match gen::ConfigScope::from_i32(req.scope) {
            Some(gen::ConfigScope::Cluster) => todo!(),
            Some(gen::ConfigScope::Connection) => {
                conn.set_config(config);
                let config =
                    serde_json::to_vec(conn.read().config()).map_err(crate::Error::from)?;
                Ok(Response::new(gen::Config {
                    scope: req.scope,
                    config,
                }))
            }
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "invalid config scope {}",
                    req.scope
                )))
            }
        }
    }

    async fn get_config(
        &self,
        request: Request<gen::GetConfigReq>,
    ) -> tonic::Result<Response<gen::Config>> {
        let conn = connection(&request)?;
        let req = request.into_inner();

        match gen::ConfigScope::from_i32(req.scope) {
            Some(gen::ConfigScope::Cluster) => todo!(),
            Some(gen::ConfigScope::Connection) => {
                let config =
                    serde_json::to_vec(conn.read().config()).map_err(crate::Error::from)?;
                Ok(Response::new(gen::Config {
                    scope: req.scope,
                    config,
                }))
            }
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "invalid config scope {}",
                    req.scope
                )))
            }
        }
    }
}
