use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use crate::harness::scheduler::thread_aware_account_locks::TryLockError;
use ahash::AHashMap;
use itertools::Itertools as _;
use solana_pubkey::Pubkey;

pub struct SharedAccountLocks(AHashMap<Pubkey, AtomicAccountLock>);

#[derive(Debug)]
struct AtomicAccountLock {
    lock_type: AtomicU8,
    lock_counter: AtomicU64,
    num_read_access: AtomicU64,
    num_write_access: AtomicU64,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct AccountLockSummary {
    lock_type: u8,
    lock_counter: u64,
    num_read_access: u64,
    num_write_access: u64,
}

impl<'a> From<&'a AtomicAccountLock> for AccountLockSummary {
    fn from(value: &'a AtomicAccountLock) -> Self {
        Self {
            lock_type: value.lock_type.load(Ordering::Relaxed),
            lock_counter: value.lock_counter.load(Ordering::Relaxed),
            num_read_access: value.num_read_access.load(Ordering::Relaxed),
            num_write_access: value.num_write_access.load(Ordering::Relaxed),
        }
    }
}

const NO_LOCK: u8 = 0;
const READ_LOCK: u8 = 2;
const WRITE_LOCK: u8 = 1;

impl Default for AtomicAccountLock {
    fn default() -> Self {
        Self {
            lock_type: AtomicU8::new(NO_LOCK),
            lock_counter: AtomicU64::new(0),
            num_read_access: AtomicU64::new(0),
            num_write_access: AtomicU64::new(0),
        }
    }
}

#[must_use]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LockSummary(Vec<(String, AccountLockSummary)>);

impl SharedAccountLocks {
    pub fn new() -> Self {
        SharedAccountLocks(AHashMap::new())
    }

    pub fn try_lock_tx_accounts(
        &mut self,
        write_accounts: &[&Pubkey],
        read_accounts: &[&Pubkey],
    ) -> anyhow::Result<(), TryLockError> {
        match self.check_tx_accounts(write_accounts, read_accounts) {
            Ok(_) => {
                self.lock_tx_accounts(write_accounts, read_accounts);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub fn check_tx_accounts(
        &self,
        write_accounts: &[&Pubkey],
        read_accounts: &[&Pubkey],
    ) -> anyhow::Result<(), TryLockError> {
        for account in write_accounts {
            if let Some(value) = self.0.get(account) {
                if value.lock_type.load(std::sync::atomic::Ordering::SeqCst) != NO_LOCK {
                    return Err(TryLockError::MultipleConflicts);
                }
            }
        }

        for account in read_accounts {
            if let Some(value) = self.0.get(account) {
                if value.lock_type.load(std::sync::atomic::Ordering::SeqCst) == WRITE_LOCK {
                    return Err(TryLockError::MultipleConflicts);
                }
            }
        }

        Ok(())
    }

    pub fn lock_tx_accounts(&mut self, write_accounts: &[&Pubkey], read_accounts: &[&Pubkey]) {
        for account in write_accounts {
            self.0
                .entry(**account)
                .and_modify(|a| {
                    a.lock_type
                        .store(WRITE_LOCK, std::sync::atomic::Ordering::SeqCst);
                    a.lock_counter.store(1, std::sync::atomic::Ordering::SeqCst);
                    a.num_write_access
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
                .or_insert(AtomicAccountLock {
                    lock_type: AtomicU8::new(WRITE_LOCK),
                    lock_counter: AtomicU64::new(1),
                    num_read_access: AtomicU64::new(0),
                    num_write_access: AtomicU64::new(1),
                });
        }

        for account in read_accounts {
            self.0
                .entry(**account)
                .and_modify(|a| {
                    let previous_counter = a
                        .lock_counter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if previous_counter == 0 {
                        a.lock_type
                            .store(READ_LOCK, std::sync::atomic::Ordering::SeqCst);
                    }
                    a.num_read_access
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
                .or_insert(AtomicAccountLock {
                    lock_type: AtomicU8::new(READ_LOCK),
                    lock_counter: AtomicU64::new(1),
                    num_read_access: AtomicU64::new(1),
                    num_write_access: AtomicU64::new(0),
                });
        }
    }

    pub fn unlock_tx_accounts(&mut self, write_accounts: &[&Pubkey], read_accounts: &[&Pubkey]) {
        for account in write_accounts {
            self.0.entry(**account).and_modify(|a| {
                a.lock_type
                    .store(NO_LOCK, std::sync::atomic::Ordering::SeqCst);
                a.lock_counter.store(0, std::sync::atomic::Ordering::SeqCst);
            });
        }

        for account in read_accounts {
            self.0.entry(**account).and_modify(|a| {
                let previous_counter = a
                    .lock_counter
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                if previous_counter == 1 {
                    a.lock_type
                        .store(NO_LOCK, std::sync::atomic::Ordering::SeqCst);
                }
            });
        }
    }

    pub fn get_top_read_locks(&self, truncate: usize) -> LockSummary {
        let top = self
            .0
            .iter()
            // TODO: May be expensive `memcpy`
            .map(|(a, b)| (a.to_string(), b.into()))
            .sorted_unstable_by(
                |a: &(String, AccountLockSummary), b: &(String, AccountLockSummary)| {
                    // In descending order
                    Ord::cmp(&b.1.num_read_access, &a.1.num_read_access)
                },
            )
            .take(truncate)
            .collect_vec();
        LockSummary(top)
    }

    pub fn get_top_write_locks(&self, truncate: usize) -> LockSummary {
        let top = self
            .0
            .iter()
            // TODO: May be expensive `memcpy`
            .map(|(a, b)| (a.to_string(), b.into()))
            .sorted_unstable_by(
                |a: &(String, AccountLockSummary), b: &(String, AccountLockSummary)| {
                    // In descending order
                    Ord::cmp(&b.1.num_write_access, &a.1.num_write_access)
                },
            )
            .take(truncate)
            .collect_vec();
        LockSummary(top)
    }
}
