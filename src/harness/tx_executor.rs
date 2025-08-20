#![allow(
    mismatched_lifetime_syntaxes,
    reason = "The lifetime is only named because inference is weak"
)]
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkEntry;
use crate::harness::scheduler::thread_aware_account_locks::ThreadId;
use crate::harness::scheduler::thread_aware_account_locks::TryLockError;
use anyhow::Context;
use crossbeam_channel::{Receiver, Sender};
use itertools::Itertools;
use solana_runtime::bank::Bank;
use solana_runtime::bank::LoadAndExecuteTransactionsOutput;
use solana_runtime::transaction_batch::OwnedOrBorrowed;
use solana_runtime::transaction_batch::TransactionBatch;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_sdk::clock::MAX_PROCESSING_AGE;
use solana_sdk::transaction::*;
use solana_svm::account_loader::LoadedTransaction;
use solana_svm::account_overrides::AccountOverrides;
use solana_svm::transaction_error_metrics::TransactionErrorMetrics;
use solana_svm::transaction_execution_result::ExecutedTransaction;
use solana_svm::transaction_execution_result::TransactionExecutionDetails;
use solana_svm::transaction_processing_result::ProcessedTransaction;
use solana_svm::transaction_processor::ExecutionRecordingConfig;
use solana_svm::transaction_processor::TransactionProcessingConfig;
use solana_timings::ExecuteTimings;
use std::collections::HashMap;
use std::slice;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use support::SharedAccountLocks;
use tracing::info;

#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct TxExecutorSummary {
    /// u64 types are provided as time in micros
    /// execution time regardless of txs regardless if they are unique, retried and if they succeeded or failed
    pub execution_time_us: u64,
    /// work time represents the time spent executing unique txs successfuly
    pub work_time_us: u64,
    /// idle time represents the time spent waiting for txs + time spent on failed account locks
    pub idle_time_us: u64,
    /// retry time represents the time spent executing failed txs again
    pub retry_time_us: u64,
    /// total time is the time elapsed since the spawn of the thread until its exit (in secs)
    pub total_time_secs: f64,
    /// saturation of worker in regard of unique txs that got successfully executed
    pub real_saturation: f64,
    /// saturation of worker in regard of unique + retried txs (total work) that got successfully executed
    pub raw_saturation: f64,
    ///percentage of how much time was spent on executing unique txs out of the total amount of work time
    pub useful_workload_saturation: f64,
    /// how many txs we get duting the waiting for receive time
    pub txs_received_per_sec: f64,
}

/// Message: [Worker -> Issuer]
/// Processed transactions.
pub struct FinishedWork<Tx> {
    pub completed_entry: Option<WorkEntry<Tx>>,
    pub failed_entry: Option<WorkEntry<Tx>>,
}

#[derive(Debug)]
pub struct TxExecutor<Tx> {
    thread_id: ThreadId,
    //channel to receive txs from TxScheduler
    work_receiver: Receiver<Work<Tx>>,
    //channel to send txs and results of execution back to TxIssuer
    completed_work_sender: Sender<FinishedWork<Tx>>,
    bank: Arc<Bank>,
    //simulate execution time provided inside tx instead of actually executing it
    simulate: bool,
    summary: TxExecutorSummary
}

impl<Tx> TxExecutor<Tx>
where
    Tx: TransactionWithMeta + Send + Sync + 'static,
{
    pub fn new(
        thread_id: ThreadId,
        work_receiver: Receiver<Work<Tx>>,
        completed_work_sender: Sender<FinishedWork<Tx>>,
        bank: Arc<Bank>,
        simulate: bool,
    ) -> Self {
        Self {
            thread_id,
            work_receiver,
            completed_work_sender,
            bank,
            simulate,
            summary: TxExecutorSummary { 
                work_time_us: 0, 
                idle_time_us: 0, 
                retry_time_us: 0,
                execution_time_us: 0,
                txs_received_per_sec: 0.0,
                ..Default::default()
            }
        }
    }

    #[tracing::instrument(skip(self, account_locks))]
    pub fn run(
        mut self,
        account_locks: Option<Arc<Mutex<SharedAccountLocks>>>,
    ) -> std::thread::JoinHandle<TxExecutorSummary> {
        std::thread::spawn(move || {
            if self.simulate {
                self.execute_txs(account_locks.unwrap())
            } else {
                //create a dummy lock that will not be used for real execution
                let empty_locks = Arc::new(Mutex::new(SharedAccountLocks::new()));
                self.execute_txs(empty_locks)
            }
        })
    }

    #[tracing::instrument(skip(self, account_locks))]
    fn execute_txs(&mut self, account_locks: Arc<Mutex<SharedAccountLocks>>) -> TxExecutorSummary {

        let mut total_txs: usize = 0;
        let mut receive_time_sec = 0.0;
        
        loop {
            
            let loop_time_start = Instant::now();
            let work = match self.work_receiver.recv(){
                Ok(w) => w,
                Err(_) => break 
            };
            
            tracing::debug!("Received new batch of work...");

            let mut harness_transactions = vec![];
            match work.entry {
                WorkEntry::SingleTx(tx) => harness_transactions.push(tx),
                WorkEntry::MultipleTxs(txs) => harness_transactions = txs,
            }

            let receive_duration = loop_time_start.elapsed();
            self.summary.idle_time_us += receive_duration.as_micros() as u64;
            total_txs += harness_transactions.len();
            receive_time_sec += receive_duration.as_secs_f64();

            let execution_time = Instant::now();
            let (processed_output, execution_times) =
                self.process_transactions_one_by_one(&harness_transactions, &account_locks);
            self.summary.execution_time_us += execution_time.elapsed().as_micros() as u64;

            let mut completed_txs = vec![];
            let mut failed_txs = vec![];
            for ((index, processed_result), tx) in processed_output.iter().enumerate().zip(harness_transactions) {
                match processed_result.as_ref() {
                    Ok(pt) => {
                        match pt.status() {
                            Ok(_) => {
                                tracing::debug!(
                                    msg = ?tx.transaction.message_hash(),
                                    sig = ?tx.transaction.signature(),
                                    "Execute success",
                                );
                                
                                if tx.retry {
                                    self.summary.retry_time_us += execution_times[index];
                                } else {
                                    self.summary.work_time_us += execution_times[index];
                                }
                                
                                completed_txs.push(tx);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    err = ?e,
                                    msg = ?tx.transaction.message_hash(),
                                    // sig = ?tx.transaction.signature(),
                                    "Execution failed",
                                );
                                self.summary.idle_time_us += execution_times[index];
                                failed_txs.push(tx);
                            }
                        };
                    }
                    Err(e) => {
                        info!(
                            "Execution of transaction identified by message hash and signature: {:?}, {:?} failed with following details:{:?}",
                            tx.transaction.message_hash(),
                            tx.transaction.signature(),
                            e
                        );
                        self.summary.idle_time_us += execution_times[index];
                        failed_txs.push(tx);
                    }
                };
            }

            let completed_entries = match completed_txs.len() {
                0 => None,
                _ => Some(WorkEntry::MultipleTxs(completed_txs)),
            };

            let failed_entries = match failed_txs.len() {
                0 => None,
                _ => Some(WorkEntry::MultipleTxs(failed_txs)),
            };

            if self
                .completed_work_sender
                .send(FinishedWork {
                    completed_entry: completed_entries,
                    failed_entry: failed_entries,
                })
                .is_err()
            {
                // kill this worker if finished_work channel is broken
                info!("Tx issuer not present anymore, exiting as well...");
                break;
            }
            let loop_end_time = loop_time_start.elapsed().as_secs_f64();
            self.summary.total_time_secs += loop_end_time;
            tracing::debug!("Transaction executed and result sent back for recording");
        }

        self.summary.txs_received_per_sec = total_txs as f64 / receive_time_sec;
        self.summary.real_saturation = self.summary.work_time_us as f64 / self.summary.execution_time_us as f64 * 100.0;
        self.summary.raw_saturation = (self.summary.work_time_us + self.summary.retry_time_us) as f64 / self.summary.execution_time_us as f64 * 100.0;
        self.summary.useful_workload_saturation = self.summary.work_time_us as f64 / (self.summary.work_time_us + self.summary.retry_time_us) as f64 * 100.0;
        self.summary.clone()
    }

    fn process_transactions_one_by_one(
        &self,
        harness_transactions: &[HarnessTransaction<Tx>],
        account_locks: &Arc<Mutex<SharedAccountLocks>>,
    ) -> (Vec<Result<ProcessedTransaction>>, Vec<u64>) {
        let mut actual_execute_time: u64 = 0;
        let mut transaction_results = vec![];
        let mut transaction_times= vec![];

        for tx in harness_transactions {
            let tx_result;
            let tx_time;
            if self.simulate {
                (tx_result, tx_time) = self.simulate_transaction(
                    &tx.transaction,
                    tx.simulated_ex_us.unwrap(),
                    account_locks,
                );
            } else {
                (tx_result, tx_time) =
                    self.process_single_transaction(&tx.transaction, &tx.account_overrides);
            }
            transaction_results.extend(tx_result);
            transaction_times.push(tx_time);
            actual_execute_time += tx_time;
        }

        tracing::debug!(
            "Executed {} transactions in {} us",
            harness_transactions.len(),
            actual_execute_time
        );

        //return execution result
        (transaction_results, transaction_times)
    }

    fn process_single_transaction(
        &self,
        transaction: &Tx,
        account_override: &AccountOverrides,
    ) -> (Vec<Result<ProcessedTransaction>>, u64) {
        let batch = TransactionBatch::new(
            self.bank.try_lock_accounts_with_results(
                slice::from_ref(transaction),
                slice::from_ref(transaction).iter().map(|_| Ok(())),
            ),
            &self.bank,
            OwnedOrBorrowed::Borrowed(slice::from_ref(transaction)),
        );

        info!(
            "Processing tx with signature and message hash: {:?}, {:?}",
            transaction.signature(),
            transaction.message_hash()
        );

        //prepare bank for current tx as it might be older than the current snapshot
        self.bank
            .load_addresses_from_ref(transaction.message_address_table_lookups())
            .context("Failed to load addresses from ALT")
            .unwrap();
        self.bank
            .register_recent_blockhash_for_test(transaction.recent_blockhash(), None);

        // TODO: Check after line 353 in the transaction_processor.rs
        let mut timings = ExecuteTimings::default();
        let LoadAndExecuteTransactionsOutput {
            processing_results, ..
        } = self.bank.load_and_execute_transactions(
            &batch,
            MAX_PROCESSING_AGE,
            &mut timings,
            &mut TransactionErrorMetrics::default(),
            TransactionProcessingConfig {
                account_overrides: Some(account_override),
                check_program_modification_slot: false,
                log_messages_bytes_limit: None,
                limit_to_load_programs: false,
                recording_config: ExecutionRecordingConfig {
                    enable_cpi_recording: true,
                    enable_log_recording: true,
                    enable_return_data_recording: true,
                    enable_transaction_balance_recording: false,
                },
            },
        );

        info!("Processed transaction timings: {:?}", timings);
        let actual_execute_time = timings.execute_accessories.process_instructions.total_us.0;
        (processing_results, actual_execute_time)
    }

    fn simulate_transaction(
        &self,
        transaction: &Tx,
        simulated_ex_time: u64,
        account_locks: &Arc<Mutex<SharedAccountLocks>>,
    ) -> (Vec<Result<ProcessedTransaction>>, u64) {
        //let time pass even if we have an account lock in order to count it in the overall execution time
        let start = Instant::now();

        let account_keys = transaction.account_keys();
        let write_accounts = account_keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| transaction.is_writable(index).then_some(key))
            .collect_vec();
        let read_accounts = account_keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| (!transaction.is_writable(index)).then_some(key))
            .collect_vec();

        let lock_result;
        {
            let mut mutex = account_locks.lock().unwrap();
            lock_result = mutex.try_lock_tx_accounts(&write_accounts, &read_accounts);
        }

        let status = match lock_result {
            Ok(()) => {
                tracing::debug!(
                    "Thread {} can lock current tx and start executing it",
                    self.thread_id
                );
                thread::sleep(Duration::from_micros(simulated_ex_time)); //Simulate ex time
                let mut mutex = account_locks.lock().unwrap();
                mutex.unlock_tx_accounts(&write_accounts, &read_accounts);
                Ok(())
            }
            Err(TryLockError::MultipleConflicts) => {
                tracing::debug!("Got conflict: TryLockError::MultipleConflicts");
                Err(TransactionError::AccountInUse)
            }
            Err(TryLockError::ThreadNotAllowed) => {
                panic!("Got conflict: TryLockError::ThreadNotAllowed"); //should not happen
            }
        };

        //prepare the simulated output
        let executed_transaction = ExecutedTransaction {
            loaded_transaction: LoadedTransaction::default(),
            execution_details: TransactionExecutionDetails {
                executed_units: simulated_ex_time,
                status,
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: HashMap::with_capacity(0),
        };
        let processed_transaction = ProcessedTransaction::Executed(Box::new(executed_transaction));

        let end = start.elapsed();
        (vec![Ok(processed_transaction)], end.as_micros() as u64)
    }
}

pub mod support {

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
    pub struct LockSummary(Vec<(Pubkey, AccountLockSummary)>);

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

        pub fn unlock_tx_accounts(
            &mut self,
            write_accounts: &[&Pubkey],
            read_accounts: &[&Pubkey],
        ) {
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
                .map(|(a, b)| (*a, b.into()))
                .sorted_unstable_by(
                    |a: &(Pubkey, AccountLockSummary), b: &(Pubkey, AccountLockSummary)| {
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
                .map(|(a, b)| (*a, b.into()))
                .sorted_unstable_by(
                    |a: &(Pubkey, AccountLockSummary), b: &(Pubkey, AccountLockSummary)| {
                        // In descending order
                        Ord::cmp(&b.1.num_write_access, &a.1.num_write_access)
                    },
                )
                .take(truncate)
                .collect_vec();
            LockSummary(top)
        }
    }
}
