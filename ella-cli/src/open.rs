use ella::Path;

/// Start an interactive session by opening a local datastore
#[derive(Debug, clap::Args)]
pub struct Args {
    root: Path,
    #[arg(long)]
    create: bool,
}

pub async fn run(args: Args, ctx: crate::Context) -> anyhow::Result<()> {
    let rt = if args.create {
        ella::open(args.root.to_string())
            .or_create_default()
            .await?
    } else {
        ella::open(args.root.to_string()).await?
    };
    crate::interactive::interactive(rt, 100, ctx).await
}
