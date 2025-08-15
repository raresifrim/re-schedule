use anyhow::Context;
use solana_accounts_db::accounts_db::ACCOUNTS_DB_CONFIG_FOR_BENCHMARKS;
use solana_pubkey::Pubkey;
use solana_runtime::{
    bank::Bank, bank_forks::{self, BankForks}, epoch_stakes::EpochStakes, runtime_config::RuntimeConfig, snapshot_archive_info::FullSnapshotArchiveInfo, snapshot_bank_utils::bank_from_snapshot_archives
};
use solana_sdk::{clock::Epoch, genesis_config::GenesisConfig};
use std::sync::{atomic::AtomicBool, Arc};
use tracing::info;
use std::sync::RwLock;


use super::config::Snapshot;

pub fn load_bank_from_snapshot(
    snapshot: &Snapshot,
    genesis: &GenesisConfig,
    simulate: bool
) -> anyhow::Result<(Arc<solana_runtime::bank::Bank>,Arc<RwLock<BankForks>>)> {

    if simulate {
        info!("Simulation mode enabled, creating a mock bank for execution.");
        let (mock_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(genesis);
        return Ok((mock_bank, bank_forks));
    }

    let snapshot_path = snapshot.path.clone();
    let accounts_dir = snapshot.accounts_dir.clone();
    let snapshot_dir = snapshot.snapshot_dir.clone();

    if !snapshot_path.exists() {
        panic!("Snapshot archive path provided in the config.json does not exist. Please update it with an exisitng location or download a new snapshot using the download-snapshot command");   
    }

    let full_snapshot_archive = FullSnapshotArchiveInfo::new_from_path(snapshot_path)
        .context("Failed to load snapshot archive info")?;

     let (snapshot_bank, _) = bank_from_snapshot_archives(
        &[accounts_dir.clone()],
        snapshot_dir,
        &full_snapshot_archive,
        None,
        genesis,
        &RuntimeConfig::default(),
        None,
        None,
        None,
        false,
        false,
        false,
        false,
        Some(ACCOUNTS_DB_CONFIG_FOR_BENCHMARKS),
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .context("Failed to create bank from snapshot archives")?;
    
    let bank_forks = BankForks::new_rw_arc(snapshot_bank);
    //this one is frozen
    let snapshot_bank = bank_forks.read().unwrap().working_bank();
    //so we should create a new one
    let snapshot_bank = Bank::new_from_parent(snapshot_bank.clone(), &Pubkey::new_unique(), snapshot_bank.slot()+1);
    
    //recreate the bank_fork for the unfrozen bank
    let bank_forks = BankForks::new_rw_arc(snapshot_bank);
    let snapshot_bank = bank_forks.read().unwrap().working_bank();
    
    info!(
        "Successfully built bank from snapshot (slot: {})!",
        snapshot_bank.slot()
    );

    Ok((snapshot_bank, bank_forks))
}
