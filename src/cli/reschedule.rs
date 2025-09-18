use crate::harness::issuer::basic_issuer::BasicIssuer;
use crate::harness::issuer::bloom_counter_issuer::BloomCounterIssuer;
use crate::harness::issuer::thread_aware_issuer::ThreadAwareIssuer;
use crate::harness::scheduler::bloom_counter_scheduler::BloomCounterScheduler;
use crate::harness::scheduler::bloom_scheduler::BloomScheduler;
use crate::harness::scheduler::greedy_scheduler::GreedyScheduler;
use crate::harness::scheduler::priograph_scheduler::PrioGraphScheduler;
use crate::harness::scheduler::round_robin_scheduler::RoundRobinScheduler;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::sequential_scheduler::SequentialScheduler;
use crate::harness::scheduler_harness::RunSummary;
use crate::harness::scheduler_harness::SchedulerHarness;
use crate::utils::config::Config;
use crate::utils::config::NetworkType;
use crate::utils::config::SchedulerType;
use crate::utils::snapshot::load_bank_from_snapshot;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use crate::harness::scheduler::bloom_counter_scheduler::ConflictFamily;
use bloom_1x::bloom_counter::Bloom1Counter;
use ahash::AHasher;
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use clap::Parser;
use itertools::Itertools;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Normal};
use solana_account::AccountSharedData;
use solana_account::from_account;
use solana_cost_model::cost_model::CostModel;
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
use std::sync::RwLock;
use tracing::info;
use std::sync::Mutex;
use rapidhash::quality::RapidBuildHasher;

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

    /// Scheduler type, can be either `bloom` (default), `roundrobin` or `greedy`
    #[arg(long, short = 's', value_enum, default_value = "bloom")]
    pub scheduler_type: SchedulerType,

    /// Scheduler transaction batch size
    #[arg(long)]
    pub batch_size: Option<u64>,

    /// Number of thread workers
    #[arg(long, short = 'w')]
    pub num_workers: Option<u64>,

    /// Whether to simulate the tx execution following a Gaussian distribution model
    /// or to execute it directly via the SVM
    #[arg(long, default_value_t = false)]
    pub simulate: bool,

    #[arg(long, default_value_t = false)]
    pub compute_bloom_fpr: bool,
}

const MAX_BATCH_SIZE: u64 = 128;

#[tracing::instrument(name = "init", skip(args))]
pub async fn run_schedule(args: RescheduleArgs) -> Result<()> {
    // Load config
    let mut config = Config::load_from_json(
        &args.config_path,
        args.network,
        args.scheduler_type,
        args.simulate,
        args.transactions,
        args.batch_size,
        args.num_workers,
        args.compute_bloom_fpr
    )
    .await
    .context("Failed to load configuration")?;
    let (start_bank, bank_forks) =
        load_bank_from_snapshot(&config.start_snapshot, &config.genesis, args.simulate)
            .context("Failed to load start bank from snapshot")?;

    // we must set the fork_graph cache for the extracted bank as it
    // is not set automatically when unpacking snapshot
    start_bank.set_fork_graph_in_program_cache(Arc::downgrade(&bank_forks));
    // clear status cache of the bank in order to execute old txs hat
    // might still be cached and will otherwise return
    // AlreadyProcessed
    start_bank.status_cache.write().unwrap().clear();

    info!("Loading transactions from local file");
    let transactions = load_runtime_transactions(
        &start_bank,
        config.network_type,
        config.num_txs_to_process as usize,
        args.simulate,
    )
    .unwrap();

    assert!(config.batch_size <= MAX_BATCH_SIZE);

    info!(
        "Loaded {} execution transactions from local file",
        transactions.len()
    );

    info!("Initializing scheduler harness");

    let mut summary = match config.scheduler_type {
        SchedulerType::Bloom => {
            {
                let k = 4;
                let w = 256;
                let l = 4096;
                let account_locks =  Arc::new(Mutex::new(ThreadAwareAccountLocks::new(config.num_workers as usize)));
                let scheduler = BloomScheduler::new(
                    config.num_workers as usize,
                    k, //number of hashes
                    l, //filter length
                    w, //filter row width in bits
                    config.batch_size as usize,
                    config.compute_bloom_fpr,
                    Some(account_locks.clone())
                );
                if config.compute_bloom_fpr {
                    let issuer = ThreadAwareIssuer::new(account_locks.clone(), !config.compute_bloom_fpr);
                    let scheduler_harness = SchedulerHarness::new_from_config(config, scheduler, issuer, transactions, start_bank)?;
                    info!("Initialized Bloom harness");
                    scheduler_harness.run()
                } else {
                    let issuer = BasicIssuer::new();
                    let scheduler_harness = SchedulerHarness::new_from_config(config, scheduler, issuer, transactions, start_bank)?;
                    info!("Initialized Bloom harness");
                    scheduler_harness.run()
                }
            }
        }
        SchedulerType::BloomCounter => {
             {
                let k = 4;
                let w = 256;
                let l = 4096;
                let mut conflict_families = vec![];
                for _ in 0..config.num_workers {
                    let conflict_family = ConflictFamily {
                        read_filter: Bloom1Counter::new(k, l, w, 96),
                        write_filter: Bloom1Counter::new(k, l, w, 96),
                    };
                    conflict_families.push(RwLock::new(conflict_family));
                }
                let conflict_families: Arc<Vec<RwLock<ConflictFamily>>> = Arc::new(conflict_families);
                let hasher: rapidhash::inner::RapidBuildHasher<true, true>  = RapidBuildHasher::new(420);
                let scheduler = BloomCounterScheduler::new(
                    config.num_workers as usize,
                    config.batch_size as usize,
                    conflict_families.clone(),
                    hasher
                );
                
                let issuer = BloomCounterIssuer::new(conflict_families.clone(),hasher);
                let scheduler_harness = SchedulerHarness::new_from_config(config, scheduler, issuer, transactions, start_bank)?;
                info!("Initialized BloomCounter harness");
                scheduler_harness.run()
            }
        }
        SchedulerType::Greedy => {
            let scheduler_harness = {
                let account_locks =  Arc::new(Mutex::new(ThreadAwareAccountLocks::new(config.num_workers as usize)));
                let scheduler = GreedyScheduler::new(
                    account_locks.clone(),
                    config.num_workers as usize,
                    config.batch_size as usize,
                    MAX_COMPUTE_UNIT_LIMIT as u64,
                );
                let issuer = ThreadAwareIssuer::new(account_locks.clone(), true);
                SchedulerHarness::new_from_config(config, scheduler, issuer, transactions, start_bank)?
            };
            info!("Initialized Greedy harness");
            scheduler_harness.run()
        }
        SchedulerType::PrioGraph => {
            let scheduler_harness = {
                let account_locks =  Arc::new(Mutex::new(ThreadAwareAccountLocks::new(config.num_workers as usize)));
                let scheduler = PrioGraphScheduler::new(
                    account_locks.clone(),
                    config.num_workers as usize,
                    config.batch_size as usize,
                    MAX_COMPUTE_UNIT_LIMIT as u64,
                );
                let issuer = ThreadAwareIssuer::new(account_locks.clone(), true);
                SchedulerHarness::new_from_config(config, scheduler, issuer, transactions, start_bank)?
            };
            info!("Initialized PrioGraph harness");
            scheduler_harness.run()
        }
        SchedulerType::Sequential => {
            let scheduler_harness = {
                let scheduler = SequentialScheduler::new();
                if config.num_workers != 1 {
                    tracing::warn!(
                        num_workers = config.num_workers,
                        "Sequential scheduler cannot use more than one worker"
                    );
                    config.num_workers = 1;
                };
                let issuer = BasicIssuer::new();
                SchedulerHarness::new_from_config(config, scheduler, issuer, transactions, start_bank)?
            };
            info!("Initialized Sequential harness");
            scheduler_harness.run()
        }
        SchedulerType::RoundRobin => {
            let scheduler_harness = {
                let scheduler = RoundRobinScheduler::new(
                    config.num_workers as usize,
                    MAX_COMPUTE_UNIT_LIMIT as u64,
                    config.batch_size as usize,
                );
                let issuer = BasicIssuer::new();
                SchedulerHarness::new_from_config(config, scheduler, issuer, transactions, start_bank)?
            };
            info!("Initialized RoundRobin harness");
            scheduler_harness.run()
        }
    };
    info!("Finalized scheduler harness");
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).expect("Failed to convert to JSON")
    );

    recompute_execution_summaries(&mut summary);

    Ok(())
}


fn recompute_execution_summaries(summary: &mut RunSummary) {
    let mut ex = summary.executors.clone();
    let slowest_worker = ex.iter().max_by(|a, b| Ord::cmp(&a.execution_time_us, &b.execution_time_us)).unwrap();
    let max_exec_time_us = slowest_worker.execution_time_us;
    let max_exec_time_secs = slowest_worker.total_time_secs;
    let ex = ex.iter_mut().map(|summary |{
        summary.idle_time_us += max_exec_time_us - summary.execution_time_us;
        summary.execution_time_us = max_exec_time_us;
        summary.real_saturation = summary.work_time_us as f64 / max_exec_time_us as f64 * 100.0;
        summary.raw_saturation = (summary.work_time_us + summary.retry_time_us) as f64 / max_exec_time_us as f64 * 100.0;
        summary.total_time_secs = max_exec_time_secs;
        summary
    }).collect_vec();
     println!(
        "Recomputed Bulk Execution Summaries{}",
        serde_json::to_string_pretty(&ex).expect("Failed to convert to JSON")
    );
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

    let file = fs::File::open(txs_path).context("Failed to open tx json file".to_string())?;
    let reader = io::BufReader::new(file);
    let base64_str: Vec<Vec<String>> =
        serde_json::from_reader(reader).context("Failed to parse tx json file")?;
    let base64_str: Vec<String> = base64_str.iter().map(|v| v[0].clone()).collect();

    let versioned_txs: Vec<VersionedTransaction> = base64_str
        .iter()
        .map(|v| {
            let tx_bytes = general_purpose::STANDARD
                .decode(v)
                .expect("Failed to decode base64 encoded tx");

            bincode::deserialize::<VersionedTransaction>(&tx_bytes)
                .expect("Failed to deserialize tx")
        })
        .collect();

    assert!(
        !versioned_txs.is_empty(),
        "No transactions to reschedule. Aborting"
    );

    let rng = &mut StdRng::seed_from_u64(420); //use same seed every run
    //create a normal distribution simulation times with a standard deviation of 3.16 and mean of 12
    let gaussian_distr = Normal::new(12.0, 3.16).unwrap();
    let mut hasher = AHasher::default();

    // txs cannot be cloned/copied so we need the iterator to consume
    // it and move it permanenty thus we cannot use the counter
    // directly in the loop and also to reference inside the cevtor so
    // we have to increment the above counter variable and break the
    // loop once we reached our goal
    'outer: for tx in versioned_txs {
        let message_hash = tx.verify_and_hash_message()?;

        if simulate {
            if let Some(transaction) = tx.into_legacy_transaction() {
                let final_tx = RuntimeTransaction::from_transaction_for_tests(transaction);
                if !final_tx.is_simple_vote_transaction() {
                    let simulated_ex_us = {
                        //get a random simuation time
                        let v = gaussian_distr.sample(rng); //from_zscore(uniform_distr.sample(rng));
                        //convert it to micro_secs
                        Some((v * 1000.0) as u64)
                        
                    };
                    //generate the compressed blockhash
                    let blockhash = {
                        hasher.write(&final_tx.recent_blockhash().to_bytes());
                        hasher.finish()
                    };
                    //push tx
                    let cu_cost =  CostModel::calculate_cost(&final_tx, &root_bank.feature_set).sum();
                    let final_tx = HarnessTransaction {
                        transaction: final_tx,
                        account_overrides: AccountOverrides::default(),
                        simulated_ex_us,
                        blockhash,
                        retry: false,
                        cu_cost 
                    };
                    transactions.push_back(final_tx);
                }
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
                final_tx.blockhash = {
                    hasher.write(&final_tx.transaction.recent_blockhash().to_bytes());
                    hasher.finish()
                };
                transactions.push_back(final_tx);
            }
        }

        if transactions.len() == num_txs {
            break 'outer;
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

    let (loaded_addresses, _) = resolve_addresses_with_deactivation(&tx, bank)?;
    let address_loader = SimpleAddressLoader::Enabled(loaded_addresses);

    let tx = RuntimeTransaction::<SanitizedTransaction>::try_from(
        tx,
        address_loader,
        &reserved_account_keys,
    )?;
    //generate account overrides for re-executaion of transactions
    //this is for overcoming the AlreadyProcessed type of error
    let accounts = get_account_overrides_for_simulation(bank, &tx.account_keys());
    let cu_cost =  CostModel::calculate_cost(&tx, &bank.feature_set).sum();
    Ok(HarnessTransaction {
        transaction: tx,
        account_overrides: accounts,
        simulated_ex_us: None,
        blockhash: 0,
        retry: false,
        cu_cost
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
