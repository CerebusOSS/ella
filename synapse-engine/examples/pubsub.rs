use engine::{EngineConfig, Schema};
use opentelemetry::{
    sdk::{trace, Resource},
    KeyValue,
};
use synapse_common::Duration;
use synapse_engine as engine;
use synapse_tensor as tensor;
use tensor::{Tensor, TensorType};
use tokio_stream::StreamExt;
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            trace::config().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                "synapse",
            )])),
        )
        .install_batch(opentelemetry::runtime::Tokio)?;

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::EnvFilter::new("INFO")),
        )
        .init();

    let config = EngineConfig::new().with_serve_metrics("0.0.0.0:8888");
    let sy = engine::Engine::start_with_config("file:///tmp/synapse/", config).await?;

    let schema = Schema::builder()
        .field("time")
        .data_type(TensorType::Timestamp)
        .required(true)
        .index(true)
        .finish()
        .field("i")
        .data_type(TensorType::Int32)
        .finish()
        .field("dt")
        .data_type(TensorType::Duration)
        .row_shape((2,))
        .finish()
        .field("x")
        .data_type(TensorType::Float32)
        .row_shape((512,))
        .finish()
        .field("y")
        .data_type(TensorType::String)
        .row_shape((2,))
        .finish()
        .build();

    let pb = sy.topic("point").get_or_create(schema).await?.publish();
    let mut sink = pb.rows(1)?;

    let start = synapse_common::now();
    let end = start + Duration::seconds(5);
    let mut i = 0_i32;
    while synapse_common::now() < end {
        i += 1;
        sink.write((
            synapse_common::now(),
            i,
            tensor::tensor![Duration::milliseconds(50), Duration::milliseconds(2)],
            Tensor::linspace(i as f32, (i + 1) as f32, 512),
            tensor::tensor!["A".to_string(), "B".to_string()],
        ))
        .await?;
    }
    sink.close().await?;
    drop(sink);

    // let df = sy.query("select DISTINCT i from point").await?;
    // let mut sub = df.execute_stream().await?;
    // while let Some(batch) = sub.try_next().await? {
    //     // println!("{:?}", batch);
    // }

    sy.shutdown().await?;
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
