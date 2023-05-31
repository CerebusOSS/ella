use runtime::Schema;
use synapse_runtime as runtime;
use synapse_tensor as tensor;
use tensor::{Tensor, TensorType};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let rt = runtime::Runtime::start("file:///tmp/synapse/").await?;

    let schema = Schema::builder()
        .field("x")
        .data_type(TensorType::Float32)
        .row_shape((5,))
        .nullable(false)
        .finish()
        .field("y")
        .data_type(TensorType::Float32)
        .row_shape((2,))
        .nullable(false)
        .finish()
        .build();

    let pb = rt.topic("point").get_or_create(schema).await?.publish();

    for i in 0..1000 {
        let data = tensor::frame!(
            x = Tensor::linspace(i as f32, (i + 1) as f32, 20).reshape((4, 5)),
            y = Tensor::linspace(i as f32, (i - 1) as f32, 8).reshape((4, 2)),
        );
        pb.insert(data)?;
    }
    drop(pb);

    let df = rt.query("select * from point").await?;
    let mut sub = df.execute_stream().await?;
    while let Some(batch) = sub.try_next().await? {
        println!("{:?}", batch);
    }

    rt.shutdown().await?;

    Ok(())
}
