use futures::SinkExt;
use synapse_engine::table::{info::TopicInfo, Column};
use synapse_tensor::Tensor;
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer,
};

#[tokio::main]
async fn main() -> synapse::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::EnvFilter::new("INFO")),
        )
        .init();

    let server = tokio::spawn(async move {
        let mut syn = synapse::open("file:///tmp/synapse/")
            .or_create(synapse::SynapseConfig::default())
            .and_serve("localhost:50051")?
            .await?;

        let mut a = syn
            .table("a")
            .or_create(
                TopicInfo::builder()
                    .column(Column::builder("x", synapse::TensorType::Float32).row_shape((10, 10))),
            )
            .await?
            .publish()?
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

        let x = syn
            .query("select x from a")
            .await?
            .create_view("ax")
            .if_not_exists()
            .await?
            .col3::<f32>("x")?
            .execute()
            .await?;
        println!("{:?}", x);
        let _frame = syn.query("select * from b").await?.execute().await?;
        syn.shutdown().await?;

        synapse::Result::Ok(())
    });
    let client = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let syn = synapse::connect("http://localhost:50051").await?;
        let mut b = syn
            .table("b")
            .or_create(
                TopicInfo::builder()
                    .column(("y", synapse::TensorType::String))
                    .column(("z", synapse::TensorType::Int32)),
            )
            .await?
            .publish()?
            .rows(1)?;

        // let mut b = syn
        //     .topic("b")
        //     .get_or_create(
        //         synapse::Schema::builder()
        //             .field("time")
        //             .data_type(synapse::TensorType::Timestamp)
        //             .required(true)
        //             .index(true)
        //             .finish()
        //             .field("y")
        //             .data_type(synapse::TensorType::String)
        //             .finish()
        //             .field("z")
        //             .data_type(synapse::TensorType::Int32)
        //             .finish()
        //             .build(),
        //     )
        //     .await?
        //     .publish()
        //     .rows(1)?;
        for i in 0..10 {
            b.feed(synapse::row!("A".to_string(), i,)).await?;
        }
        b.close().await?;
        drop(b);

        // let x = syn
        //     .query("select x from a")
        //     .await?
        //     .col3::<f32>("x")?
        //     .sin()
        //     .execute()
        //     .await?;
        // println!("{:?}", x);

        // let time = syn
        //     .query("select time from a")
        //     .await?
        //     .col1::<synapse::Time>("time")?
        //     .execute()
        //     .await?;
        // println!("{:?}", time);

        // let mut a = syn
        //     .query("select * from a")
        //     .await?
        //     .rows::<(synapse::Time, Tensor2<f32>)>()
        //     .await?;
        // while let Some((time, x)) = a.try_next().await? {
        //     // println!("time={:?}, x={:?}", time, x);
        // }
        // drop(a);
        syn.shutdown().await?;

        synapse::Result::Ok(())
    });

    let (a, b) = futures::join!(server, client);
    a.unwrap().unwrap();
    b.unwrap().unwrap();

    Ok(())
}
