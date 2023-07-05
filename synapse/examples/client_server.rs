use futures::{SinkExt, TryStreamExt};
use synapse_tensor::{Tensor, Tensor2};

#[tokio::main]
async fn main() -> synapse::Result<()> {
    let server = tokio::spawn(async move {
        let mut syn = synapse::start("file:///tmp/synapse/")
            .serve("localhost:50051")?
            .build()
            .await?;

        let mut a = syn
            .topic("a")
            .get_or_create(
                synapse::Schema::builder()
                    .field("time")
                    .data_type(synapse::TensorType::Timestamp)
                    .required(true)
                    .index(true)
                    .finish()
                    .field("x")
                    .data_type(synapse::TensorType::Float32)
                    .row_shape((10, 10))
                    .finish()
                    .build(),
            )
            .await?
            .publish()
            .rows(1)?;
        for _ in 0..10 {
            a.feed(synapse::row!(
                Tensor::linspace(0_f32, 1_f32, 100).reshape((10, 10))
            ))
            .await?;
        }
        a.close().await?;
        drop(a);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let mut b = syn
            .query("select * from b")
            .await?
            .rows::<(synapse::Time, Option<String>, Option<i32>)>()
            .await?;
        while let Some((time, y, z)) = b.try_next().await? {
            println!("time={:?}, y={:?}, z={:?}", time, y, z);
        }
        drop(b);

        synapse::Result::Ok(())
    });
    let client = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let mut syn = synapse::connect("http://localhost:50051").await?;
        let mut b = syn
            .topic("b")
            .get_or_create(
                synapse::Schema::builder()
                    .field("time")
                    .data_type(synapse::TensorType::Timestamp)
                    .required(true)
                    .index(true)
                    .finish()
                    .field("y")
                    .data_type(synapse::TensorType::String)
                    .finish()
                    .field("z")
                    .data_type(synapse::TensorType::Int32)
                    .finish()
                    .build(),
            )
            .await?
            .publish()
            .rows(1)?;
        for i in 0..10 {
            b.feed(synapse::row!("A".to_string(), i,)).await?;
        }
        b.close().await?;
        drop(b);

        let mut a = syn
            .query("select * from a")
            .await?
            .rows::<(synapse::Time, Tensor2<f32>)>()
            .await?;
        while let Some((time, x)) = a.try_next().await? {
            println!("time={:?}, x={:?}", time, x);
        }
        drop(a);

        synapse::Result::Ok(())
    });

    let (a, b) = futures::join!(server, client);
    a.unwrap().unwrap();
    b.unwrap().unwrap();

    Ok(())
}
