use engine::{EngineConfig, Schema};
use synapse_engine as engine;
use synapse_tensor as tensor;
use synapse_time::Duration;
use tensor::{Tensor, TensorType};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = EngineConfig::new().with_log_load_metrics(Duration::milliseconds(50));
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
        .row_shape((5,))
        .finish()
        .field("y")
        .data_type(TensorType::String)
        .row_shape((2,))
        .finish()
        .build();

    let pb = sy.topic("point").get_or_create(schema).await?.publish();

    for i in 0..4000 {
        let data = tensor::frame!(
            time = tensor::tensor![synapse_time::now()],
            i = tensor::tensor![i],
            dt = tensor::tensor![[
                synapse_time::Duration::milliseconds(50),
                synapse_time::Duration::milliseconds(2)
            ]],
            x = Tensor::linspace(i as f32, (i + 1) as f32, 5).unsqueeze(0),
            y = tensor::tensor![["A".to_string(), "B".to_string()]],
        );
        pb.try_write(data)?;
    }
    drop(pb);

    let df = sy.query("select DISTINCT i from point").await?;
    let mut sub = df.execute_stream().await?;
    while let Some(batch) = sub.try_next().await? {
        println!("{:?}", batch);
    }

    sy.shutdown().await?;

    Ok(())
}
