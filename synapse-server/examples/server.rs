use opentelemetry::{
    sdk::{trace, Resource},
    KeyValue,
};
use synapse_engine::EngineConfig;
use synapse_server::server::SynapseServer;
use tonic::transport::Server;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let tracer = opentelemetry_otlp::new_pipeline()
    //     .tracing()
    //     .with_exporter(opentelemetry_otlp::new_exporter().tonic())
    //     .with_trace_config(
    //         trace::config()
    //             ..with_resource(Resource::new(vec![KeyValue::new(
    //                 "service.name",
    //                 "synapse",
    //             )])),
    //     )
    //     .install_batch(opentelemetry::runtime::Tokio)?;

    tracing_subscriber::registry()
        // .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::EnvFilter::new("DEBUG")),
        )
        .init();

    let config = EngineConfig::new().with_serve_metrics("0.0.0.0:8888");

    let engine = synapse_engine::Engine::start_with_config("file:///tmp/synapse", config).await?;
    let mut server = SynapseServer::start(
        Server::builder(),
        engine.clone(),
        "0.0.0.0:50051".parse().unwrap(),
    );

    let _ = tokio::signal::ctrl_c().await;
    server.stop().await?;
    engine.shutdown().await?;
    // opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
