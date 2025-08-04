use anyhow::{Context, Result};
use clap::Parser;
use crate::utils::config::Config;
use std::{path::PathBuf};
use tokio::{time::Instant};
use tracing::{info, instrument};
use crate::utils::config::SchedulerType;
use crate::utils::config::NetworkType;


#[derive(Parser, Debug)]
pub struct RescheduleArgs {
    /// Network type to replay (e.g., devnet, testnet)
    #[arg(short, long, value_enum)]
    pub network: NetworkType,

    /// Path to the configuration file
    #[arg(long, default_value = "config.json")]
    pub config_path: PathBuf,

    /// Number of txs to process (optional, overrides config default)
    pub transactions: Option<u64>,

    /// Scheduler type
    #[arg(long, value_enum, default_value = "greedy")]
    pub scheduler_type: Option<SchedulerType>,

    /// Scheduler transaction batch size 
    #[arg(long,default_value = "64")]
    pub batch_size:Option<u64>,

    /// Slot allowed time in ms
    #[arg(long,default_value = "400")]
    pub slot_duration:Option<u64>
}

#[instrument(name = "run_schedule")]
pub async fn run_schedule(args: RescheduleArgs) -> Result<()> {
    
    info!(args = ?args, "Starting replay");
    // Load config
    let config = Config::load_from_json(
        &args.config_path,
        args.network,
        args.scheduler_type.unwrap(),
        args.transactions,
        args.batch_size,
        args.slot_duration
    )
    .await
    .context("Failed to load configuration")?;

    info!(?config, "Loaded configuration for replay");
    let scheduler_harness = SchedulerHarness::new_from_config(config);
    
    
    /* info!(
        total_duration_ms = overall_duration.as_millis(),
        config_load_duration_ms = config_load_duration.as_millis(),
        snapshot_load_duration_ms = snapshot_load_duration.as_millis(),
        replay_duration_ms = replay_duration.as_millis(),
        "Replay command finished successfully."
    );
    
    // Print simplified results to console
    println!("\n------ Replay Results ------");
    println!("Blocks Processed: {}", num_blocks_to_process);
    println!("Scheduler Type: {:?}", config.scheduler_type);
    println!("------ Performance ------");
    println!("Replay Time: {:.2}s", replay_duration.as_secs_f64());
    println!("--------------------------\n"); */

    Ok(())

}
