/// Start an interactive session by connecting to an ella API server
#[derive(Debug, clap::Args)]
pub struct Args {
    /// Address of the API server
    #[arg(default_value = "http://localhost:50052")]
    addr: String,
}

pub async fn run(args: Args, ctx: crate::Context) -> anyhow::Result<()> {
    let rt = ella::connect(args.addr).await?;
    crate::interactive::interactive(rt, 100, ctx).await
}
