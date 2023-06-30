use futures::{SinkExt, TryStreamExt};
use synapse_common::{Duration, Time};
use synapse_server::client::SynapseClient;
use synapse_tensor as tensor;
use synapse_tensor::Tensor;
use tensor::Tensor1;
use tonic::transport::Channel;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> synapse_engine::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::EnvFilter::new("INFO")),
        )
        .init();

    let channel = Channel::builder("http://localhost:50051".parse().unwrap())
        .connect()
        .await
        .unwrap();
    let mut client = SynapseClient::new(channel);
    let mut sink = client.publish("point").await?.rows(1)?;

    let mut stream = client
        .query("select time, x, y from point where time > now() order by time nulls first")
        .await?
        .rows::<(Time, Tensor1<f32>, Tensor1<String>)>()
        .await?;
    for i in 0..10 {
        let now = synapse_common::now();
        println!("sending {i}");
        sink.send((
            now,
            i,
            Duration::milliseconds(50),
            Tensor::linspace(i as f32, (i + 1) as f32, 512),
            tensor::tensor!["A".to_string(), "B".to_string()],
        ))
        .await?;
        println!("recieving {i}");
        let _batch = stream.try_next().await?.unwrap();
        let elapsed = synapse_common::now() - now;
        println!("elapsed= {:?} us", elapsed.whole_microseconds());
    }
    sink.close().await?;

    Ok(())
}
