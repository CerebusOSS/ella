use runtime::Schema;
use synapse_runtime as runtime;
use synapse_tensor as tensor;
use tensor::{Tensor, TensorType};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let rt = runtime::Runtime::start("file:///tmp/synapse/").await?;

    let schema = Schema::builder()
        .field("time")
        .data_type(TensorType::Timestamp)
        .required(true)
        .index(true)
        .finish()
        .field("i")
        .data_type(TensorType::Int32)
        .finish()
        .field("x")
        .data_type(TensorType::Float32)
        .row_shape((5,))
        .required(false)
        .finish()
        .field("y")
        .data_type(TensorType::Float32)
        .row_shape((2,))
        .required(false)
        .finish()
        .build();

    let pb = rt.topic("point").get_or_create(schema).await?.publish();

    for i in 0..4000 {
        let data = tensor::frame!(
            time = tensor::tensor![synapse_time::now()],
            i = tensor::tensor![i],
            x = Tensor::linspace(i as f32, (i + 1) as f32, 5).unsqueeze(0),
            y = Tensor::linspace(i as f32, (i - 1) as f32, 2).reshape((1, 2)),
        );
        pb.insert(data)?;
    }
    drop(pb);

    let df = rt.query("select * from point").await?;
    let mut sub = df.execute_stream().await?;
    while let Some(batch) = sub.try_next().await? {
        // println!("{:?}", batch);
    }

    rt.shutdown().await?;

    Ok(())
}
