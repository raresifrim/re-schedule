use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use solana_sdk::genesis_config::GenesisConfig;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::{fs};
use strum_macros::{Display, EnumIter, EnumString};
use tracing::info;

// --- Serde Structs for config.json ---
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonNetworkConfig {
    pub rpc_url: String,
    pub start_snapshot: String,
    pub genesis_folder: String,
    pub num_blocks: u64,
    pub num_txs: u64,
    pub batch_size: u64,
    pub slot_duration: u64,
    pub num_workers: u64
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonConfig {
    pub networks: HashMap<String, JsonNetworkConfig>,
}
// --- End Serde Structs ---

/// Load JsonConfig directly from a file path
pub fn load_json_config(path: &str) -> anyhow::Result<JsonConfig> {
    let config_path = std::path::Path::new(path);
    let file = File::open(config_path)?;
    let reader = BufReader::new(file);
    let json_config: JsonConfig = serde_json::from_reader(reader)?;
    Ok(json_config)
}

/// Write JsonConfig directly to a file path
pub fn upload_json_config(path: &str, json_config: JsonConfig) -> anyhow::Result<()> {
    let config_path = std::path::Path::new(path);
    let mut file = File::create(config_path)?;
    file.write_all(serde_json::json!(json_config).to_string().as_bytes())?;
    Ok(())
}


#[derive(Default, Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Hash, ValueEnum, Display)]
#[strum(serialize_all = "lowercase")]
pub enum NetworkType {
    Devnet,
    Testnet,
    #[default]
    Mainnet,
    Localnet,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, EnumString, Display, Hash, EnumIter, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum SchedulerType {
    Greedy,
    PrioGraph,
    Bloom,
    Sequential,
    RoundRobin
}

// New struct to represent a snapshot with its directories
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub path: PathBuf,
    pub snapshot_dir: PathBuf,
    pub accounts_dir: PathBuf,
}

impl Snapshot {
    pub async fn new(network_type: NetworkType, cache_dir: &Path, snapshot_filename: &str) -> Self {
        let network_str = network_type.to_string(); // Use strum's Display

        let accounts_dir_name = format!("{}-accounts", network_str);
        let accounts_dir = cache_dir.join(accounts_dir_name);

        // Create or clear accounts directory
        if accounts_dir.exists() {
            info!("Found exisitng accounts directory at {:?}. Will use this one", accounts_dir);
        } else {
            fs::create_dir_all(&accounts_dir).expect("Failed to create accounts directory");
        }

        let snapshot_dir_name = format!("snapshots-{}", network_str);
        let snapshot_dir = cache_dir.join(snapshot_dir_name);

        // Create snapshot directory if it doesn't exist
        if !snapshot_dir.exists() {
            fs::create_dir_all(&snapshot_dir).expect("Failed to create snapshot directory");
        }

        Snapshot {
            path: snapshot_dir.join(snapshot_filename),
            snapshot_dir,
            accounts_dir,
        }
    }
}

// Keep the runtime Config struct, but populate it from JsonConfig + overrides
#[derive(Debug, Clone)] // Add Clone
pub struct Config {
    // Snapshot configuration
    pub start_snapshot: Snapshot,
    pub network_type: NetworkType,
    // Genesis configuration
    pub genesis: GenesisConfig,

    // Scheduler type
    pub scheduler_type: SchedulerType,

    // Execution configuration (can be overridden by CLI)
    pub num_txs_to_process: u64,
    pub batch_size: u64,
    pub slot_duration: u64,
    pub num_workers: u64,
    pub simulate: bool
}

impl Config {
    pub async fn load_from_json(
        json_path: &Path,
        network_type: NetworkType,
        scheduler_type: SchedulerType,
        simulate:bool,
        // CLI Overrides (passed from main)
        cli_num_txs: Option<u64>,
        cli_batch_size: Option<u64>,
        cli_slot_duration: Option<u64>,
        cli_num_workers: Option<u64>,
    ) -> anyhow::Result<Self> {
        let file = File::open(json_path)
            .map_err(|e| anyhow::anyhow!("Failed to open config file {:?}: {}", json_path, e))?;
        let reader = BufReader::new(file);
        let json_config: JsonConfig = serde_json::from_reader(reader)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file {:?}: {}", json_path, e))?;

        let network_key = network_type.to_string();
        let network_config = json_config.networks.get(&network_key).ok_or_else(|| {
            anyhow::anyhow!(
                "Network type '{}' not found in config file networks",
                network_key
            )
        })?;

        let cache_dir = std::env::current_dir().unwrap().join("cache");
        info!("Cache directory: {:?}", cache_dir);

        // Create genesis config specific to the network type
        let genesis = Self::create_genesis_config(network_config)?;

        // Create Snapshot struct using data from json
        let start_snapshot =
            Snapshot::new(network_type, &cache_dir, &network_config.start_snapshot).await;

        // Determine the number of txs to process from CLI or config
        let num_txs_to_process = match cli_num_txs {
            Some(n) => {
                if n > network_config.num_txs {
                    network_config.num_txs
                } else {
                    n
                }
            }
            None => network_config.num_txs
        };

        let batch_size = cli_batch_size.or(Some(network_config.batch_size)).unwrap();
        let slot_duration = cli_slot_duration.or(Some(network_config.slot_duration)).unwrap();
        let num_workers = cli_num_workers.or(Some(network_config.num_workers)).unwrap();
        Ok(Self {
            start_snapshot,
            network_type,
            genesis,
            scheduler_type,
            num_txs_to_process,
            batch_size,
            slot_duration,
            num_workers,
            simulate
        })
    }

    fn create_genesis_config(network_config: &JsonNetworkConfig) -> anyhow::Result<GenesisConfig> {
        let genesis_path = std::env::current_dir()
            .expect("Failed to get current directory")
            .join("genesis")
            .join(&network_config.genesis_folder);

        info!("Loading genesis config from: {:?}", genesis_path);

        GenesisConfig::load(&genesis_path).map_err(|e| {
            anyhow::anyhow!(
                "Failed to load genesis config from {:?}: {}",
                genesis_path,
                e
            )
        })
    }
}
