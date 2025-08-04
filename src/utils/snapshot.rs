use anyhow::Context;
use solana_accounts_db::accounts_db::ACCOUNTS_DB_CONFIG_FOR_BENCHMARKS;
use solana_runtime::{
    bank_forks::BankForks, runtime_config::RuntimeConfig,
    snapshot_archive_info::FullSnapshotArchiveInfo,
    snapshot_bank_utils::bank_from_snapshot_archives,
};
use solana_sdk::genesis_config::GenesisConfig;
use std::sync::{atomic::AtomicBool, Arc};
use tracing::info;

use super::config::Snapshot;

pub fn load_bank_from_snapshot(
    snapshot: &Snapshot,
    genesis: &GenesisConfig,
) -> anyhow::Result<Arc<solana_runtime::bank::Bank>> {
    let snapshot_path = snapshot.path.clone();
    let accounts_dir = snapshot.accounts_dir.clone();
    let snapshot_dir = snapshot.snapshot_dir.clone();

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
    let snapshot_bank = bank_forks.read().unwrap().working_bank();

    info!(
        "Successfully built bank from snapshot (slot: {})!",
        snapshot_bank.slot()
    );

    Ok(snapshot_bank)
}
