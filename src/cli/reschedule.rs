use anyhow::anyhow;
use anyhow::{Context, Result};
use clap::Parser;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_runtime_transaction::transaction_meta::StaticMeta;
use solana_sdk::transaction::SanitizedVersionedTransaction;
use crate::harness::scheduler::bloom_scheduler::BloomScheduler;
use crate::harness::scheduler::scheduler::{Scheduler, SequentialScheduler};
use crate::utils::config::Config;
use std::{path::PathBuf};
use crate::harness::scheduler_harness::SchedulerHarness;
use tracing::{info, instrument};
use crate::utils::config::SchedulerType;
use crate::utils::config::NetworkType;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::VecDeque;
use std::fs;
use std::io;
use base64::{engine::general_purpose, Engine as _};
use solana_sdk::transaction::SanitizedTransaction;
use solana_svm_transaction::message_address_table_lookup::SVMMessageAddressTableLookup;
use std::collections::HashSet;
use solana_sdk::transaction::SimpleAddressLoader;
use solana_sdk::pubkey::Pubkey;
use solana_runtime::bank::Bank;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::AddressLoaderError;
use solana_message::v0::LoadedAddresses;
use std::sync::Arc;
use crate::utils::snapshot::load_bank_from_snapshot;

#[derive(Parser, Debug)]
pub struct RescheduleArgs {
    /// Network type to replay (e.g., devnet, testnet)
    #[arg(short, long, value_enum, default_value = "mainnet")]
    pub network: NetworkType,

    /// Path to the configuration file
    #[arg(long, default_value = "config.json")]
    pub config_path: PathBuf,

    /// Number of txs to process (optional, overrides config default)
    #[arg(short, long)]
    pub transactions: Option<u64>,

    /// Scheduler type
    #[arg(long, value_enum, default_value = "sequential")]
    pub scheduler_type: Option<SchedulerType>,

    /// Scheduler transaction batch size 
    #[arg(long,default_value = "64")]
    pub batch_size:Option<u64>,

    /// Slot allowed time in ms
    #[arg(long,default_value = "400")]
    pub slot_duration:Option<u64>,

    /// Number of thread workers
    #[arg(long,default_value = "4")]
    pub num_workers:Option<u64>
}

#[instrument(name = "run_schedule")]
pub async fn run_schedule(args: RescheduleArgs) -> Result<()> {
    
    info!("Starting replay");
    // Load config
    let mut config = Config::load_from_json(
        &args.config_path,
        args.network,
        args.scheduler_type.unwrap(),
        args.transactions,
        args.batch_size,
        args.slot_duration,
        args.num_workers
    )
    .await
    .context("Failed to load configuration")?;
    info!("Loaded configuration for replay");
    
    info!("Setting up directories and loading snapshots...");
    let start_bank = load_bank_from_snapshot(&config.start_snapshot, &config.genesis).context("Failed to load start bank from snapshot")?;
    info!("Completed setting up directories and loading snapshots...");

    info!("Loading transactions from local file");
    let transactions = load_transactions(
        start_bank.clone(),
        config.network_type, 
        config.num_txs_to_process
    ).unwrap();
    info!("Loaded {} execution transactions from local file", transactions.len());
    
    info!("Initializing scheduler harness");
    match config.scheduler_type {
        SchedulerType::Bloom => {
            let scheduler = BloomScheduler;
            let scheduler_harness = SchedulerHarness::<BloomScheduler>::new_from_config(config, scheduler, transactions, start_bank)?;
            info!("Initialized scheduler harness");
    
            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        }
        SchedulerType::Greedy => {
            let scheduler = BloomScheduler;
            let scheduler_harness = SchedulerHarness::<BloomScheduler>::new_from_config(config, scheduler, transactions, start_bank)?;
            info!("Initialized scheduler harness");
    
            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        },
        SchedulerType::PrioGraph => {
            let scheduler = BloomScheduler;
            let scheduler_harness = SchedulerHarness::<BloomScheduler>::new_from_config(config, scheduler, transactions, start_bank)?;
            info!("Initialized scheduler harness");
    
            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        },
        SchedulerType::Sequential => {
            let scheduler = SequentialScheduler;
            config.num_workers = 1; //in sequential mode we only use one worker for executing txs
            let scheduler_harness = SchedulerHarness::<SequentialScheduler>::new_from_config(config, scheduler, transactions, start_bank)?;
            info!("Initialized scheduler harness");
    
            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        }
    };
    
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

fn load_transactions(root_bank: Arc<Bank>, network_type: NetworkType, num_txs: u64) -> anyhow::Result<VecDeque<RuntimeTransaction<SanitizedTransaction>>> {
        let mut transactions = VecDeque::<RuntimeTransaction<SanitizedTransaction>>::new();

        let network_key = network_type.to_string();
        let cache_dir = PathBuf::from("./cache");
        let snapshot_dir = cache_dir.join(format!("snapshots-{}", network_key));
        fs::exists(&snapshot_dir).context("Failed to get snapshot directory")?;
        let txs_path = snapshot_dir.join("transactions.json");

        let file = fs::File::open(txs_path).context(format!("Failed to open tx json file"))?;
        let reader = io::BufReader::new(file);
        let base64_str: Vec<Vec<String>> = serde_json::from_reader(reader).context("Failed to parse tx json file")?;
        let base64_str: Vec<String> = base64_str.iter().map(|v| v[0].clone()).collect();
        let versioned_txs: Vec<VersionedTransaction> = base64_str.iter().map(|v| {
            let tx_bytes = general_purpose::STANDARD.decode(&v).expect("Failed to decode base64 encoded tx");
            let tx = bincode::deserialize::<VersionedTransaction>(&tx_bytes).expect("Failed to deserialize tx");
            tx
        }).collect(); 

        if versioned_txs.len() == 0 {
            panic!("No txs found to be replayed and rescheduled, aborting run...");
        }

        let mut counter:u64 = 0;
        //txs cannot be cloned/copied so we need the iterator to consume it and move it permanenty
        //thus we cannot use the counter directly in the loop and also to reference inside the cevtor
        //so we have to increment the above counter variable and break the loop once we reached our goal
        for tx in versioned_txs {
            let message_hash = tx.verify_and_hash_message()?;
            let runtime_tx = RuntimeTransaction::<SanitizedVersionedTransaction>::try_from(
                SanitizedVersionedTransaction::try_from(tx)?,
                solana_sdk::transaction::MessageHash::Precomputed(message_hash),
                None
            )?;
            if !runtime_tx.is_simple_vote_transaction() {
                let final_tx = build_sanitized_transaction(
                        runtime_tx,
                        root_bank.as_ref(),
                        root_bank.get_reserved_account_keys(),
                )?;
                transactions.push_back(final_tx);
            }

            counter += 1;
            if counter == num_txs {
                break;
            }
        }

        if counter < num_txs {
            info!("Found less non-voting txs in the provided blocks than requested.");
        }

        Ok(transactions)
}


 pub fn build_sanitized_transaction(
        tx: RuntimeTransaction<SanitizedVersionedTransaction>,
        bank: &Bank,
        reserved_account_keys: &HashSet<Pubkey>,
    ) -> anyhow::Result<RuntimeTransaction<SanitizedTransaction>> {
       
        // Resolve the lookup addresses and retrieve the min deactivation slot
        let (loaded_addresses, _) = resolve_addresses_with_deactivation(&tx, bank)?;
        let address_loader = SimpleAddressLoader::Enabled(loaded_addresses);
        let tx = 
            RuntimeTransaction::<SanitizedTransaction>::try_from(
                tx,
                address_loader,
                reserved_account_keys,
            )?;
        Ok(tx)
    }

    fn resolve_addresses_with_deactivation(
        transaction: &SanitizedVersionedTransaction,
        bank: &Bank,
    ) -> Result<(LoadedAddresses, Slot), AddressLoaderError> {
        let Some(address_table_lookups) = transaction.get_message().message.address_table_lookups()
        else {
            return Ok((LoadedAddresses::default(), Slot::MAX));
        };

        bank.load_addresses_from_ref(
            address_table_lookups
                .iter()
                .map(SVMMessageAddressTableLookup::from),
        )
    }
