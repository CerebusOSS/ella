use engine::{
    config::{EngineConfig, SynapseConfig},
    table::{info::TopicBuilder, ColumnBuilder},
};
use futures::SinkExt;
use opentelemetry::{
    sdk::{trace, Resource},
    KeyValue,
};
use synapse_common::{Duration, Time};
use synapse_engine as engine;
use synapse_tensor as tensor;
use tensor::{Tensor, Tensor1, TensorType};
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

    let config = SynapseConfig::builder()
        .engine_config(EngineConfig::builder().serve_metrics("0.0.0.0:8888"))
        .build();
    let ctx = engine::create("file:///tmp/synapse/", config, true).await?;

    let topic = TopicBuilder::new()
        .column(ColumnBuilder::new("i", TensorType::Int32))
        .column(ColumnBuilder::new("dt", TensorType::Duration))
        .column(ColumnBuilder::new("x", TensorType::Float32).row_shape((512,)))
        .column(ColumnBuilder::new("y", TensorType::String).row_shape((2, 2)));
    let pb = ctx
        .create_topic("point", topic, true, false)
        .await?
        .publish();

    let mut sink = pb.rows(1)?;

    let start = synapse_common::now();
    let end = start + Duration::seconds(2);
    let mut i = 0_i32;
    while synapse_common::now() < end {
        i += 1;
        sink.feed((
            synapse_common::now(),
            i,
            Duration::milliseconds(50),
            Tensor::linspace(i as f32, (i + 1) as f32, 512),
            tensor::tensor![
                ["A".to_string(), "B".to_string()],
                ["C".to_string(), "D".to_string()]
            ],
        ))
        .await?;
    }

    sink.close().await?;

    let mut rows = ctx
        .query("SELECT * FROM point ORDER BY time")
        .await?
        .rows::<(Time, i32, Duration, Tensor1<f32>, Tensor1<String>)>()
        .await?;
    while let Some(row) = rows.try_next().await? {
        println!("{:?}", row);
    }

    ctx.shutdown().await?;
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
