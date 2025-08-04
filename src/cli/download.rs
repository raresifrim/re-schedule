use crate::utils::config::{JsonConfig, NetworkType};
use anyhow::{Context, Result};
use clap::Parser;
use serde_json;
use crate::utils::custom_client::SolanaClient;
use crate::utils::types::CustomClientTrait;
use std::{
    fs::{File},
    io::{self},
    path::{PathBuf},
};
use tracing::{info, instrument};

// --- Structs ---

#[derive(Parser, Debug)]
pub struct DownloadSingleSnapshotArgs {
    /// Network type to download snapshot for
    #[arg(short, long, value_enum)]
    pub network: NetworkType,

    /// Path to the configuration file (to read RPC URL)
    #[arg(long, default_value = "config.json")]
    pub config_path: PathBuf,
}

#[derive(Parser, Debug)]
pub struct DownloadBlocksArgs {
    /// Network type to download snapshot for
    #[arg(short, long, value_enum)]
    pub network: NetworkType,

    /// Path to the configuration file (to read RPC URL)
    #[arg(long, default_value = "config.json")]
    pub config_path: PathBuf,

    /// Number of blocks to retrieve
    #[arg(short, long)]
    pub blocks: u64
}

// --- Handlers ---

#[instrument(skip(args), name = "run_download_single_snapshot")]
pub async fn run_download_single_snapshot(args: DownloadSingleSnapshotArgs) -> Result<(String, u64)> {
    info!(args = ?args, "Starting download-single-snapshot command");
    let (rpc_url, _) = get_rpc_url(&args.network, &args.config_path)?;
    let solana_client = SolanaClient::new(rpc_url);

    let (snapshot_info, highest_slot) = solana_client.get_and_download_latest_snapshot(&args.network).await?;
    info!(snapshot_info = ?snapshot_info, "Successfully downloaded single snapshot.");

    Ok((snapshot_info, highest_slot))
}


pub async fn run_download_blocks (args: DownloadBlocksArgs) -> Result<PathBuf>{
    info!(args = ?args, "Starting download-blocks command");
    let (rpc_url, _) = get_rpc_url(&args.network, &args.config_path)?;
    let solana_client = SolanaClient::new(rpc_url);
    let highest_slot = solana_client.get_highest_snapshot_slot().await?;
    let blocks_info = solana_client.get_and_download_blocks(&args.network, highest_slot, args.blocks).await?;
    info!(blocks_info = ?blocks_info, "Successfully downloaded {} blocks.", &args.blocks);
    Ok(blocks_info)
}

pub async fn run_download_transactions (args: DownloadBlocksArgs) -> Result<PathBuf>{
    info!(args = ?args, "Starting download-txs command");
    let (rpc_url, _) = get_rpc_url(&args.network, &args.config_path)?;
    let solana_client = SolanaClient::new(rpc_url);
    let highest_slot = solana_client.get_highest_snapshot_slot().await?;
    let (txs_info, txs_num) = solana_client.get_and_download_transactions(&args.network, highest_slot, args.blocks).await?;
    info!(txs_info = ?txs_info, "Successfully downloaded {} transactions.", txs_num);
    Ok(txs_info)
}

/// downloads everything (snapshot, blocks and txs) in one function call
pub async fn run (args: DownloadBlocksArgs) -> Result<usize>{
    info!(args = ?args, "Starting download-txs command");
    let (rpc_url, json_config) = get_rpc_url(&args.network, &args.config_path)?;
    println!("Current JsonConfig: {:?}", json_config);
    let solana_client = SolanaClient::new(rpc_url);
    
    info!(args = ?args, "Starting download-single-snapshot command");
    let (snapshot_info, highest_slot) = solana_client.get_and_download_latest_snapshot(&args.network).await?;
    info!(snapshot_info = ?snapshot_info, "Successfully downloaded single snapshot.");
 
    info!(args = ?args, "Starting download-blocks command");
    let blocks_info = solana_client.get_and_download_blocks(&args.network, highest_slot, args.blocks).await?;
    info!(blocks_info = ?blocks_info, "Successfully downloaded {} snapshot.", &args.blocks);
 
    info!(args = ?args, "Extracting txs from downloaded blocks");
    let (txs_info, txs_num) = solana_client.extract_transactions(&args.network, &blocks_info).await?;
    info!(txs_info = ?txs_info, "Successfully extracted {} nonvoting transactions.", txs_num);
    
    Ok(txs_num)
}

// --- Helper Structs and Functions --- // 

/// Loads JsonConfig, extracts RPC URL for the network.
/// Panics on file read/parse error.
fn get_rpc_url(network: &NetworkType, config_path: &PathBuf) -> Result<(String, JsonConfig)> {
    let file =
        File::open(config_path).context(format!("Failed to open config file {:?}", config_path))?;
    let reader = io::BufReader::new(file);
    let json_config: JsonConfig =
        serde_json::from_reader(reader).context("Failed to parse config file")?;

    let network_key = network.to_string();
    let rpc_url = json_config
        .networks
        .get(&network_key)
        .ok_or_else(|| anyhow::anyhow!("Network '{}' not found in config", network_key))?
        .rpc_url
        .clone();
    Ok((rpc_url, json_config))
}




