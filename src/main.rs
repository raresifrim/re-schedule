mod cli;
mod harness;
mod utils;

use crate::cli::commands::{Cli, Commands};
use clap::Parser;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .with_writer(std::io::stderr)
        .compact()
        .init();

    let cli = Cli::parse();
    info!("Running with {:#?}", cli);

    match cli.command {
        Commands::Reschedule(args) => {
            cli::reschedule::run_schedule(args).await?;
        }
        Commands::DownloadSnapshot(args) => {
            cli::download::run_download_single_snapshot(args).await?;
        }
        Commands::DownloadBlocks(args) => {
            cli::download::run_download_blocks(args).await?;
        }
        Commands::DownloadTransactions(args) => {
            cli::download::run_download_transactions(args).await?;
        }
        Commands::DownloadAll(args) => {
            cli::download::run(args).await?;
        }
    };

    Ok(())
}
