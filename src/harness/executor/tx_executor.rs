#![allow(
    mismatched_lifetime_syntaxes,
    reason = "The lifetime is only named because inference is weak"
)]
use crate::harness::executor::execution_tracker::ExecutionTracker;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::thread_aware_account_locks::ThreadId;
use crate::harness::scheduler::thread_aware_account_locks::TryLockError;
use super::shared_account_locks::SharedAccountLocks;
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
    pub completed_entry: Option<Vec<HarnessTransaction<Tx>>>,
    pub failed_entry: Option<Vec<HarnessTransaction<Tx>>>,
}

#[derive(Debug)]
pub struct TxExecutor<Tx> {
    thread_id: ThreadId,
    /// channel to receive txs from TxScheduler
    work_receiver: Receiver<Work<Tx>>,
    /// channel to send txs and results of execution back to TxIssuer
    completed_work_sender: Sender<FinishedWork<Tx>>,
    /// Structure that holds the current number of txs executed, amount of time spend executing them and total CUs
    /// This is a common reference shared with the scheduler
    tracker: Arc<ExecutionTracker>,
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
            tracker: Arc::new(ExecutionTracker::new()),
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

    pub fn get_tracker_ref(&self) -> Arc<ExecutionTracker> {
        Arc::clone(&self.tracker)
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

            let harness_transactions = work.entry;

            let receive_duration = loop_time_start.elapsed();
            self.summary.idle_time_us += receive_duration.as_micros() as u64;
            total_txs += harness_transactions.len();
            receive_time_sec += receive_duration.as_secs_f64();

            let mut completed_txs = vec![];
            let mut failed_txs = vec![];
            let execution_time = Instant::now();
            self.process_transactions_one_by_one(harness_transactions, &account_locks, &mut completed_txs, &mut failed_txs);
            self.summary.execution_time_us += execution_time.elapsed().as_micros() as u64;

            let completed_entries = match completed_txs.len() {
                0 => None,
                _ => Some(completed_txs),
            };

            let failed_entries = match failed_txs.len() {
                0 => None,
                _ => Some(failed_txs),
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
        &mut self,
        harness_transactions: Vec<HarnessTransaction<Tx>>,
        account_locks: &Arc<Mutex<SharedAccountLocks>>,
        completed_txs: &mut Vec<HarnessTransaction<Tx>>,
        failed_txs: &mut Vec<HarnessTransaction<Tx>>,
    ) {
       
        for tx in harness_transactions {
            let mut tx_result;
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
            match tx_result.pop().unwrap() {
                Ok(pt) => {
                        match pt.status() {
                            Ok(_) => {
                                tracing::debug!(
                                    msg = ?tx.transaction.message_hash(),
                                    sig = ?tx.transaction.signature(),
                                    "Execute success",
                                );
                                self.tracker.update(1, tx_time, pt.executed_units());
                                if tx.retry {
                                    self.summary.retry_time_us += tx_time;
                                } else {
                                    self.summary.work_time_us += tx_time;
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
                                self.tracker.update(0, tx_time, 0);
                                self.summary.idle_time_us += tx_time;
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
                        self.tracker.update(0, tx_time, 0);
                        self.summary.idle_time_us += tx_time;
                        failed_txs.push(tx);
                    }
            };

        }

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

