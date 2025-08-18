use crate::harness::scheduler::bloom_scheduler::BloomScheduler;
use crate::harness::scheduler::greedy_scheduler::GreedyScheduler;
use crate::harness::scheduler::round_robin_scheduler::RoundRobinScheduler;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::sequential_scheduler::SequentialScheduler;
use crate::harness::scheduler_harness::SchedulerHarness;
use crate::utils::config::Config;
use crate::utils::config::NetworkType;
use crate::utils::config::SchedulerType;
use crate::utils::snapshot::load_bank_from_snapshot;
use ahash::AHasher;
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use clap::Parser;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Normal};
use solana_account::AccountSharedData;
use solana_account::from_account;
use solana_accounts_db::ancestors::Ancestors;
use solana_message::AccountKeys;
use solana_message::v0::LoadedAddresses;
use solana_program_runtime::execution_budget::MAX_COMPUTE_UNIT_LIMIT;
use solana_runtime::bank::Bank;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_runtime_transaction::transaction_meta::StaticMeta;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Check;
use solana_sdk::slot_history::Slot;
use solana_sdk::sysvar;
use solana_sdk::sysvar::slot_history::*;
use solana_sdk::transaction::AddressLoaderError;
use solana_sdk::transaction::SanitizedTransaction;
use solana_sdk::transaction::SanitizedVersionedTransaction;
use solana_sdk::transaction::SimpleAddressLoader;
use solana_sdk::transaction::VersionedTransaction;
use solana_svm::account_overrides::AccountOverrides;
use solana_svm_transaction::message_address_table_lookup::SVMMessageAddressTableLookup;
use solana_svm_transaction::svm_message::SVMMessage;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::hash::Hasher;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, instrument};

#[derive(Parser, Debug)]
pub struct RescheduleArgs {
    /// Network type to replay (e.g., devnet, testnet)
    #[arg(short, long, value_enum, default_value = "mainnet")]
    pub network: NetworkType,

    /// Path to the configuration file
    #[arg(long, default_value = "config.json")]
    pub config_path: PathBuf,

    /// Number of txs to process (optional, overrides config default)
    #[arg(short, long, value_parser=clap::value_parser!(u64).range(10..))]
    pub transactions: Option<u64>,

    /// Scheduler type
    #[arg(long, value_enum, default_value = "bloom")]
    pub scheduler_type: Option<SchedulerType>,

    /// Scheduler transaction batch size
    #[arg(long, default_value = "64")]
    pub batch_size: Option<u64>,

    /// Slot allowed time in ms
    #[arg(long, default_value = "400")]
    pub slot_duration: Option<u64>,

    /// Number of thread workers
    #[arg(long, default_value = "4")]
    pub num_workers: Option<u64>,

    /// Whether to simulate the tx execution following a gaussian distribution model
    /// or to execute it directly via the SVM
    #[arg(long, default_value = "true")]
    pub simulate: bool,
}

#[instrument(name = "run_schedule")]
pub async fn run_schedule(args: RescheduleArgs) -> Result<()> {
    info!("Starting replay");
    // Load config
    let mut config = Config::load_from_json(
        &args.config_path,
        args.network,
        args.scheduler_type.unwrap(),
        args.simulate,
        args.transactions,
        args.batch_size,
        args.slot_duration,
        args.num_workers,
    )
    .await
    .context("Failed to load configuration")?;
    info!("Loaded configuration for replay");

    info!("Setting up directories and loading snapshots...");
    let (start_bank, bank_forks) =
        load_bank_from_snapshot(&config.start_snapshot, &config.genesis, args.simulate)
            .context("Failed to load start bank from snapshot")?;

    //we must set the fork_graph cache for the extracted bank as it is not set automatically when unpacking snapshot
    let fork_graph = Arc::new(bank_forks);
    start_bank.set_fork_graph_in_program_cache(Arc::downgrade(&fork_graph));
    //clear status cache of the bank in order to execute old txs hat might still be cached and will otherwise return AlreadyProcessed
    start_bank.status_cache.write().unwrap().clear();

    info!("Completed setting up directories and loading snapshots...");

    info!("Loading transactions from local file");
    let transactions = load_runtime_transactions(
        &start_bank,
        config.network_type,
        config.num_txs_to_process as usize,
        args.simulate,
    )
    .unwrap();
    info!(
        "Loaded {} execution transactions from local file",
        transactions.len()
    );

    info!("Initializing scheduler harness");
    match config.scheduler_type {
        SchedulerType::Bloom => { //128KB filter
            let k = 2;
            let w = 64;
            let l = 16384;
            let scheduler = BloomScheduler::new(
                config.num_workers as usize, 
                k, //number of hashes 
                l, //filter length
                w, //filter row width in bits
                config.batch_size as usize);
            let scheduler_harness = SchedulerHarness::new_from_config(
                config,
                scheduler,
                transactions,
                start_bank,
            )?;
            info!("Initialized scheduler harness");

            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        }
        SchedulerType::Greedy => {
            let scheduler = GreedyScheduler::new(
                start_bank.clone(),
                config.num_workers as usize,
                config.batch_size as usize,
                MAX_COMPUTE_UNIT_LIMIT as u64
            );
            let scheduler_harness = SchedulerHarness::new_from_config(
                config,
                scheduler,
                transactions,
                start_bank,
            )?;
            info!("Initialized scheduler harness");

            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        }
        SchedulerType::PrioGraph => {
            let scheduler = SequentialScheduler::new();
            let scheduler_harness = SchedulerHarness::new_from_config(
                config,
                scheduler,
                transactions,
                start_bank,
            )?;
            info!("Initialized scheduler harness");

            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        }
        SchedulerType::Sequential => {
            let scheduler = SequentialScheduler::new();
            config.num_workers = 1; //in sequential mode we only use one worker for executing txs
            let scheduler_harness = SchedulerHarness::new_from_config(
                config,
                scheduler,
                transactions,
                start_bank,
            )?;
            info!("Initialized scheduler harness with Sequential scheduler");

            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        }
        SchedulerType::RoundRobin => {
            let cu_quant = MAX_COMPUTE_UNIT_LIMIT as u64;
            let scheduler =
                RoundRobinScheduler::new(config.num_workers as usize, cu_quant, start_bank.clone());
            let scheduler_harness = SchedulerHarness::new_from_config(
                config,
                scheduler,
                transactions,
                start_bank,
            )?;
            info!("Initialized scheduler harness with RoundRobin Scheduler");

            info!("Starting scheduler harness");
            scheduler_harness.run();
            info!("Finalized scheduler harness");
        }
    };

    Ok(())
}

fn load_runtime_transactions(
    root_bank: &Arc<Bank>,
    network_type: NetworkType,
    num_txs: usize,
    simulate: bool,
) -> anyhow::Result<VecDeque<HarnessTransaction<RuntimeTransaction<SanitizedTransaction>>>> {
    let mut transactions = VecDeque::new();

    let network_key = network_type.to_string();
    let cache_dir = PathBuf::from("./cache");
    let snapshot_dir = cache_dir.join(format!("snapshots-{}", network_key));
    fs::exists(&snapshot_dir).context("Failed to get snapshot directory")?;
    let txs_path = snapshot_dir.join("transactions.json");

    let file = fs::File::open(txs_path).context(format!("Failed to open tx json file"))?;
    let reader = io::BufReader::new(file);
    let base64_str: Vec<Vec<String>> =
        serde_json::from_reader(reader).context("Failed to parse tx json file")?;
    let base64_str: Vec<String> = base64_str.iter().map(|v| v[0].clone()).collect();

    let versioned_txs: Vec<VersionedTransaction> = base64_str
        .iter()
        .map(|v| {
            let tx_bytes = general_purpose::STANDARD
                .decode(&v)
                .expect("Failed to decode base64 encoded tx");
            let tx = bincode::deserialize::<VersionedTransaction>(&tx_bytes)
                .expect("Failed to deserialize tx");
            tx
        })
        .collect();

    if versioned_txs.len() == 0 {
        panic!("No txs found to be replayed and rescheduled, aborting run...");
    }

    let rng = &mut StdRng::seed_from_u64(420); //use same seed every run
    //create a normal distribution simulation times with a standard deviation of 3.16 and mean of 12
    let gaussian_distr = Normal::new(12.0, 3.16).unwrap();
    let mut hasher = AHasher::default();

    //txs cannot be cloned/copied so we need the iterator to consume it and move it permanenty
    //thus we cannot use the counter directly in the loop and also to reference inside the cevtor
    //so we have to increment the above counter variable and break the loop once we reached our goal
    for tx in versioned_txs {
        let message_hash = tx.verify_and_hash_message()?;

        if simulate {
            match tx.into_legacy_transaction() {
                Some(transaction) => {
                    let final_tx =
                        RuntimeTransaction::<SanitizedTransaction>::from_transaction_for_tests(
                            transaction,
                        );
                    if !final_tx.is_simple_vote_transaction() {
                        //get a random simuation time
                        let v = gaussian_distr.sample(rng); //from_zscore(uniform_distr.sample(rng));
                        //convert it to micro_secs
                        let v = (v * 1000.0) as u64;
                        info!("Generating simulation time for non-voting tx as {} us", v);
                        //generate the compressed blockhash
                        hasher.write(&final_tx.recent_blockhash().to_bytes());
                        let blockhash = hasher.finish();
                        //push tx
                        let final_tx = HarnessTransaction {
                            transaction: final_tx,
                            account_overrides: AccountOverrides::default(),
                            simulated_ex_us: Some(v),
                            blockhash,
                            retry: false
                        };
                        transactions.push_back(final_tx);
                    }
                }
                None => {}
            }
        } else {
            let runtime_tx = RuntimeTransaction::<SanitizedVersionedTransaction>::try_from(
                SanitizedVersionedTransaction::try_from(tx)?,
                solana_sdk::transaction::MessageHash::Precomputed(message_hash),
                None,
            )?;
            if !runtime_tx.is_simple_vote_transaction() {
                let mut final_tx = build_sanitized_transaction(
                    runtime_tx,
                    root_bank,
                    root_bank.get_reserved_account_keys().clone(),
                )?;
                hasher.write(&final_tx.transaction.recent_blockhash().to_bytes());
                let blockhash = hasher.finish();
                final_tx.blockhash = blockhash;
                transactions.push_back(final_tx);
            }
        }

        
        if transactions.len() == num_txs {
            break;
        }
    }

    if transactions.len() < num_txs {
        info!("Found less non-voting txs in the provided blocks than requested.");
    }

    Ok(transactions)
}

pub fn build_sanitized_transaction(
    tx: RuntimeTransaction<SanitizedVersionedTransaction>,
    bank: &Arc<Bank>,
    reserved_account_keys: HashSet<Pubkey>,
) -> anyhow::Result<HarnessTransaction<RuntimeTransaction<SanitizedTransaction>>> {
    // Resolve the lookup addresses and retrieve the min deactivation slot
    let address_loader;
    let (loaded_addresses, _) = resolve_addresses_with_deactivation(&tx, bank)?;
    address_loader = SimpleAddressLoader::Enabled(loaded_addresses);

    let tx = RuntimeTransaction::<SanitizedTransaction>::try_from(
        tx,
        address_loader,
        &reserved_account_keys,
    )?;
    //generate account overrides for re-executaion of transactions
    //this is for overcoming the AlreadyProcessed type of error
    let accounts = get_account_overrides_for_simulation(&bank, &tx.account_keys());
    //generate the compressed blockhash
    Ok(HarnessTransaction {
        transaction: tx,
        account_overrides: accounts,
        simulated_ex_us: None,
        blockhash: 0,
        retry: false
    })
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

fn get_account_overrides_for_simulation(
    bank: &Arc<Bank>,
    account_keys: &AccountKeys,
) -> AccountOverrides {
    let mut account_overrides = AccountOverrides::default();
    let slot_history_id = sysvar::slot_history::id();
    if account_keys.iter().any(|pubkey| *pubkey == slot_history_id) {
        let current_account = bank.get_account_with_fixed_root(&slot_history_id);
        let slot_history = current_account
            .as_ref()
            .map(|account| from_account::<SlotHistory, _>(account).unwrap())
            .unwrap_or_default();
        if slot_history.check(bank.slot()) == Check::Found {
            let ancestors = Ancestors::from(proper_ancestors(bank).collect::<Vec<_>>());
            if let Some((account, _)) =
                load_slow_with_fixed_root(bank, &ancestors, &slot_history_id)
            {
                account_overrides.set_slot_history(Some(account));
            }
        }
    }
    account_overrides
}

fn proper_ancestors(bank: &Arc<Bank>) -> impl Iterator<Item = Slot> + '_ {
    bank.ancestors
        .keys()
        .into_iter()
        .filter(move |slot| *slot != bank.slot())
}

fn load_slow_with_fixed_root(
    bank: &Arc<Bank>,
    ancestors: &Ancestors,
    pubkey: &Pubkey,
) -> Option<(AccountSharedData, Slot)> {
    bank.rc.accounts.load_with_fixed_root(ancestors, pubkey)
}
