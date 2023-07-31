use tracing::metadata::LevelFilter;

/// Open a datastore as a standalone server.
///
/// The datastore will be created if it doesn't already exist.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// Path to the datastore root
    root: ella::Path,
    /// Address where the ella API will be served
    #[arg(short, long, default_value = "localhost:50052")]
    addr: String,
    /// Do not create the datastore if it doesn't already exist
    #[arg(long)]
    no_create: bool,
}

pub async fn run(args: Args, ctx: crate::Context) -> anyhow::Result<()> {
    crate::init_logging(ctx.verbosity.log_level(LevelFilter::INFO));

    tracing::info!("starting elle server");
    let rt = if args.no_create {
        ella::open(args.root.to_string())
            .and_serve(args.addr)?
            .await
    } else {
        ella::open(args.root.to_string())
            .or_create_default()
            .and_serve(args.addr)?
            .await
    }?;
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(?error, "failed to register signal listener");
    }

    tracing::info!("shutting down server");
    rt.shutdown().await?;

    Ok(())
}
