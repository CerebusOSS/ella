use crate::gen::{self, engine_service_server::EngineService};
use futures::stream::BoxStream;
use synapse_engine::{Engine, Schema};
use tonic::{Request, Response};

#[derive(Debug, Clone)]
pub struct SynapseEngineService {
    engine: Engine,
}

impl SynapseEngineService {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl EngineService for SynapseEngineService {
    type ListTopicsStream = BoxStream<'static, tonic::Result<gen::Topic>>;

    async fn list_topics(
        &self,
        _: Request<gen::Empty>,
    ) -> tonic::Result<Response<Self::ListTopicsStream>> {
        todo!()
    }

    async fn create_topic(
        &self,
        request: Request<gen::Topic>,
    ) -> tonic::Result<Response<gen::Empty>> {
        let req = request.into_inner();
        let name = req.name;
        let schema = Schema::try_from(
            req.schema
                .ok_or_else(|| tonic::Status::invalid_argument("missing topic schema"))?,
        )?;
        self.engine.topic(name).create(schema).await?;

        Ok(Response::new(gen::Empty::default()))
    }

    async fn get_topic(
        &self,
        request: Request<gen::TopicId>,
    ) -> tonic::Result<Response<gen::Topic>> {
        let name = request.into_inner().name;
        let schema = self
            .engine
            .topic(&name)
            .get()
            .map(|topic| (**topic.schema()).clone().into());
        Ok(Response::new(gen::Topic { name, schema }))
    }
}
