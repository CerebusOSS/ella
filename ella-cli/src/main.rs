mod connect;
mod interactive;
mod open;
mod serve;

use clap::Parser;
use tracing::{metadata::LevelFilter, Level};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer,
};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[command(subcommand)]
    action: Action,
    #[command(flatten)]
    verbosity: Verbosity,
}

#[derive(Debug, clap::Subcommand)]
enum Action {
    Serve(serve::Args),
    Connect(connect::Args),
    Open(open::Args),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let ctx = Context {
        verbosity: args.verbosity,
    };

    use Action::*;
    match args.action {
        Serve(args) => serve::run(args, ctx).await?,
        Connect(args) => connect::run(args, ctx).await?,
        Open(args) => open::run(args, ctx).await?,
    }
    Ok(())
}

#[derive(Debug)]
pub struct Context {
    pub verbosity: Verbosity,
}

#[derive(Debug, Clone, clap::Args)]
pub struct Verbosity {
    #[arg(short, long, action = clap::ArgAction::Count, global = true, conflicts_with = "quiet")]
    verbose: u8,
    #[arg(short, long, action = clap::ArgAction::Count, global = true, conflicts_with = "verbose")]
    quiet: u8,
}

impl Verbosity {
    pub fn log_level(&self, level: LevelFilter) -> LevelFilter {
        let mut level: Option<Level> = level.into();
        for _ in 0..self.verbose {
            level = match level {
                None => Some(Level::ERROR),
                Some(l) => Some(match l {
                    Level::TRACE => Level::TRACE,
                    Level::DEBUG => Level::TRACE,
                    Level::INFO => Level::DEBUG,
                    Level::WARN => Level::INFO,
                    Level::ERROR => Level::WARN,
                }),
            }
        }

        for _ in 0..self.quiet {
            level = match level {
                None | Some(Level::ERROR) => None,
                Some(l) => Some(match l {
                    Level::TRACE => Level::DEBUG,
                    Level::DEBUG => Level::INFO,
                    Level::INFO => Level::WARN,
                    Level::WARN => Level::ERROR,
                    _ => unreachable!(),
                }),
            }
        }
        level.into()
    }
}

fn init_logging(level: LevelFilter) {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                EnvFilter::builder()
                    .with_default_directive(level.into())
                    .with_env_var("ELLE_LOG")
                    .from_env()
                    .unwrap(),
            ),
        )
        .init();
}
