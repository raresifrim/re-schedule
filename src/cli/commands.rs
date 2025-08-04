use crate::cli::{download,reschedule::RescheduleArgs};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the replay process
    Reschedule(RescheduleArgs),
    /// Download only the single latest snapshot for a network
    DownloadSnapshot(download::DownloadSingleSnapshotArgs),
    /// Download latest transactions from the last N specified number of blocks Txs  
    DownloadTransactions(download::DownloadBlocksArgs),
    /// Download latest N blocks 
    DownloadBlocks(download::DownloadBlocksArgs),
    /// Download latest snapshot, N blocks, and txs from these N Blocks
    DownloadAll(download::DownloadBlocksArgs)
}

