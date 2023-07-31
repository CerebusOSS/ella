use clap::{CommandFactory, Parser};
use dialoguer::{console::style, History, Input};
use ella::Ella;
use std::collections::VecDeque;
use tracing::metadata::LevelFilter;

#[derive(Debug, clap::Parser)]
#[command(
    multicall = true,
    help_template = "{all-args}",
    disable_help_subcommand = true,
    disable_help_flag = true,
    disable_version_flag = true
)]
pub struct Args {
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, clap::Subcommand)]
enum Action {
    /// Quit the session
    #[command(visible_alias = "\\q")]
    Quit,
    /// Display help
    #[command(visible_alias = "\\h")]
    Help,
    #[command(external_subcommand)]
    Sql(Vec<String>),
}

pub async fn interactive(rt: Ella, history: usize, ctx: crate::Context) -> anyhow::Result<()> {
    crate::init_logging(ctx.verbosity.log_level(LevelFilter::WARN));

    let mut history = CmdHistory::new(history);
    loop {
        let cmd = Input::<String>::new()
            .with_prompt(rt.default_catalog().to_string())
            .history_with(&mut history)
            .allow_empty(true)
            .interact_text()?;

        match shlex::split(&cmd) {
            Some(args) => match Args::try_parse_from(args) {
                Ok(args) => match args.action {
                    Action::Quit => break,
                    Action::Help => Args::command().print_help().unwrap(),
                    Action::Sql(sql) => match rt.query(sql.join(" ")).await {
                        Ok(plan) => match plan.execute().await {
                            Ok(df) => {
                                println!("{}", df.pretty_print())
                            }
                            Err(error) => {
                                println!("{}: {}", style("error").red(), error);
                            }
                        },
                        Err(error) => println!("{}: {}", style("error").red(), error),
                    },
                },
                Err(_) => Args::command().print_help().unwrap(),
            },
            None => Args::command().print_help().unwrap(),
        }
    }
    Ok(())
}

#[derive(Debug)]
struct CmdHistory {
    capacity: usize,
    history: VecDeque<String>,
}

impl CmdHistory {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            history: VecDeque::with_capacity(capacity),
        }
    }
}

impl<T: ToString> History<T> for CmdHistory {
    fn read(&self, pos: usize) -> Option<String> {
        self.history.get(pos).cloned()
    }

    fn write(&mut self, val: &T) {
        if self.history.len() == self.capacity {
            self.history.pop_back();
        }
        self.history.push_front(val.to_string());
    }
}
