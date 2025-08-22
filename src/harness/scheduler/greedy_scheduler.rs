use crate::harness::executor::execution_tracker::ExecutionTracker;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use crate::harness::scheduler::thread_aware_account_locks::ThreadId;
use crate::harness::scheduler::thread_aware_account_locks::ThreadSet;
use crate::harness::scheduler::thread_aware_account_locks::TryLockError;
use crate::harness::scheduler::thread_aware_account_locks::select_thread;
use crate::harness::scheduler::scheduler::WorkerId;
use ahash::{HashMap, HashMapExt};
use crossbeam_channel::{Receiver, Sender};
use itertools::Itertools;
use solana_runtime::bank::Bank;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;
use tracing::info;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

/// Dead-simple scheduler that is efficient and will attempt to schedule
/// in priority order, scheduling anything that can be immediately
/// scheduled, up to the limits.
pub struct GreedyScheduler {
    num_workers: usize,
    batch_size: usize,
    target_scheduled_cus: u64,
    bank: Arc<Bank>,
    working_account_set: ReadWriteAccountSet,
    unschedulables: VecDeque<HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>>,
    scheduling_summary: SchedulingSummary,
    container: VecDeque<HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>>,
    work_lanes: HashMap<WorkerId, Work<<GreedyScheduler as Scheduler>::Tx>>,
    thread_trackers: Vec<Arc<ExecutionTracker>>,
}

impl GreedyScheduler {
    pub fn new(
        bank: Arc<Bank>,
        num_workers: usize,
        batch_size: usize,
        target_scheduled_cus: u64,
    ) -> Self {
        let mut txs_per_worker = HashMap::with_capacity(num_workers);
        for i in 0..num_workers {
            txs_per_worker.insert(i, Default::default());
        }

        let scheduling_summary = SchedulingSummary {
            txs_per_worker,
            useful_txs: 0,
            total_txs: 0,
        };

        let container =
            VecDeque::<HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>>::with_capacity(
                batch_size,
            );
        
        let mut work_lanes = HashMap::with_capacity(num_workers);
        for i in 0..num_workers {
            work_lanes.insert(i, Work { entry: vec![], total_cus: 0 });
        }

        Self {
            bank,
            num_workers,
            batch_size,
            target_scheduled_cus,
            working_account_set: ReadWriteAccountSet::default(),
            unschedulables: VecDeque::with_capacity(batch_size),
            scheduling_summary,
            container,
            work_lanes,
            //work trackers should be added through a separate function call defined by the Scheduler trait
            thread_trackers: vec![],
        }
    }

    fn get_next_txs(&mut self, account_locks: &Arc<Mutex<ThreadAwareAccountLocks>>) {
        let num_threads = self.num_workers;
        let target_cu_per_thread = self.target_scheduled_cus / num_threads as u64;
        let mut schedulable_threads = ThreadSet::any(num_threads);
        let mut num_scanned: usize = 0;

        while num_scanned < self.batch_size && !self.container.is_empty() {
            num_scanned += 1;

            // Should always be in the container, during initial testing phase panic.
            // Later, we can replace with a continue in case this does happen.
            let Some(harness_tx) = self.container.pop_front() else {
                panic!("transaction state must exist")
            };

            if !self
                .working_account_set
                .check_locks(&harness_tx.transaction)
            {
                tracing::debug!(
                    "Found exisitng lock on accounts of new popped tx, sending all txs out..."
                );
                self.working_account_set.clear();
                self.container.push_back(harness_tx);
                // Push unschedulables back into the queue
                self.container.append(&mut self.unschedulables);
                //get back to parent function to send available txs in work_lanes
                return;
            }

            // Now check if the transaction can actually be scheduled.
            match self.try_schedule_transaction(&harness_tx, schedulable_threads, account_locks) {
                Err(TransactionSchedulingError::UnschedulableConflicts) => {
                    self.unschedulables.push_back(harness_tx);
                }
                Err(TransactionSchedulingError::UnschedulableThread) => {
                    self.unschedulables.push_back(harness_tx);
                }
                Ok(thread_id ) => {
                    assert!(
                        self.working_account_set
                            .take_locks(&harness_tx.transaction),
                        "locks must be available"
                    );

                    let retry = harness_tx.retry;
                    let cu_cost = harness_tx.cu_cost;
                    let mut v = vec![harness_tx];
                    self.work_lanes.entry(thread_id)
                    .and_modify(|f| {
                        f.entry.append(&mut v);
                        f.total_cus += cu_cost;
                    })
                    .or_insert(Work { entry: v, total_cus: cu_cost });

                    tracing::debug!("Added one more tx to the batch of worker {}", thread_id);
                    
                    self.scheduling_summary.total_txs += 1;
                    let report = self
                        .scheduling_summary
                        .txs_per_worker
                        .get_mut(&thread_id)
                        .unwrap();
                    report.total += 1;
                    if retry {
                        report.retried += 1;
                    } else {
                        report.unique += 1;
                        self.scheduling_summary.useful_txs += 1;
                    }

                    let work_lane = self.work_lanes.get(&thread_id).unwrap();
                    let total_cus = work_lane.total_cus;
                    let workload = work_lane.entry.len();
                    
                    // If target batch size is reached, send all the batches
                    if workload >= self.batch_size / self.num_workers {
                        self.working_account_set.clear();
                        // Push unschedulables back into the queue
                        self.container.append(&mut self.unschedulables);
                        //return back to parent function to send available txs
                        return;
                    }

                    // if the thread is at target_cu_per_thread, remove it from the schedulable threads
                    // if there are no more schedulable threads, stop scheduling.
                    if total_cus >= target_cu_per_thread {
                        schedulable_threads.remove(thread_id);
                        if schedulable_threads.is_empty() {
                            break;
                        }
                    }
                }
            }
        }

        self.working_account_set.clear();
        // Push unschedulables back into the queue
        self.container.append(&mut self.unschedulables);
    }

    fn try_schedule_transaction(
        &mut self,
        harness_tx: &HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>,
        schedulable_threads: ThreadSet,
        account_locks: &Arc<Mutex<ThreadAwareAccountLocks>>
    ) -> Result<ThreadId, TransactionSchedulingError> {
        
        let account_keys = harness_tx.transaction.account_keys();
        let write_account_locks = account_keys.iter().enumerate().filter_map(|(index, key)| {
            harness_tx
                .transaction
                .is_writable(index)
                .then_some(key)
        }).collect_vec();
        let read_account_locks = account_keys.iter().enumerate().filter_map(|(index, key)| {
            (!harness_tx.transaction.is_writable(index)).then_some(key)
        }).collect_vec();

        let mut mutex = account_locks.lock().unwrap();
        let thread_id = match mutex.try_lock_accounts(
            &write_account_locks,
            &read_account_locks,
            schedulable_threads,
            |thread_set| {
                select_thread::<Self>(
                    thread_set,
                    &self.thread_trackers,
                    &self.work_lanes,
                    1,
                    harness_tx.cu_cost
                )
            },
        ) {
            Ok(thread_id) => thread_id,
            Err(TryLockError::MultipleConflicts) => {
                return Err(TransactionSchedulingError::UnschedulableConflicts);
            }
            Err(TryLockError::ThreadNotAllowed) => {
                return Err(TransactionSchedulingError::UnschedulableThread);
            }
        };

        
        Ok(thread_id)
    }
}

const NUM_INGEST_RETRIES: u8 = 5;

impl Scheduler for GreedyScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;

    fn add_thread_trackers(&mut self, execution_trackers: Vec<Arc<ExecutionTracker>>) {
        self.thread_trackers = execution_trackers;
    }

    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>],
        account_locks: Arc<Mutex<ThreadAwareAccountLocks>>
    ) -> Result<(), SchedulerError> {
        //set a number of retries to accumulate txs
        let mut num_retries = NUM_INGEST_RETRIES;
        let mut current_buffer_len = 0;

        loop {
            //STAGE 1: Ingest txs from the TxIssuer
            //quickly check if there are new incoming txs
            match issue_channel.try_recv() {
                Ok(work) => {
                    tracing::debug!("Received txs from TxIssuer");
                    for tx in work.entry {
                        self.container.push_back(tx);
                    }
                }

                //error might be actualy just empty or a real error like disconnected
                Err(e) => {
                    match e {
                        crossbeam_channel::TryRecvError::Empty => {
                            if self.container.is_empty() {
                                //info!("No txs on the channel and no txs buffered locally. Maybe we receive something later...")
                            }
                        }
                        crossbeam_channel::TryRecvError::Disconnected => {
                            //if disconnected we exit as there is no tx issuer to give us work
                            return Err(SchedulerError::DisconnectedRecvChannel(String::from(
                                "TxIssuer Channel is closed",
                            )));
                        }
                    }
                }
            };

            //STAGE 2: Check and schedule accumulated txs in the local buffer
            if self.container.len() <= self.batch_size {
                //not enough work
                if current_buffer_len < self.container.len() {
                    current_buffer_len = self.container.len();
                } else {
                    //if we still have same buffered txs subtract the number or retries
                    num_retries -= 1;
                }
                if num_retries > 0 {
                    //try to get more txs before scheduling
                    continue;
                }
            }
            //once we got here, we know we can start scheduling the txs
            self.get_next_txs(&account_locks);
            num_retries = NUM_INGEST_RETRIES;
            current_buffer_len = 0;

            //STAGE 3: send the current scheduled txs to the workers
            for worker_index in 0..self.num_workers {
                match self.work_lanes.remove_entry(&worker_index) {
                    Some(lane) => {
                        match execution_channels[lane.0].send(lane.1) {
                            Ok(_) => {}
                            Err(_) => {
                                //for the moment we stop the entire execution if we see that one worker is not responding anymore
                                //we don't have a safety mechanism to handle what happens when a worker gets disconnected
                                return Err(SchedulerError::DisconnectedSendChannel(format!(
                                    "TxExecutor Channel {} got disconnected",
                                    worker_index
                                )));
                            }
                        };
                    }
                    None => continue,
                };
            }
        }
    }

    fn get_summary(&self) -> SchedulingSummary {
        self.scheduling_summary.clone()
    }
}

//////////////////////////////////////// Suport Structures ////////////////////////////////////////

use {ahash::AHashSet, solana_pubkey::Pubkey, solana_svm_transaction::svm_message::SVMMessage};

/// Wrapper struct to accumulate locks for a batch of transactions.
#[derive(Debug, Default)]
pub struct ReadWriteAccountSet {
    /// Set of accounts that are locked for read
    read_set: AHashSet<Pubkey>,
    /// Set of accounts that are locked for write
    write_set: AHashSet<Pubkey>,
}

impl ReadWriteAccountSet {
    /// Returns true if all account locks were available and false otherwise.
    pub fn check_locks(&self, message: &impl SVMMessage) -> bool {
        message
            .account_keys()
            .iter()
            .enumerate()
            .all(|(index, pubkey)| {
                if message.is_writable(index) {
                    self.can_write(pubkey)
                } else {
                    self.can_read(pubkey)
                }
            })
    }

    /// Add all account locks.
    /// Returns true if all account locks were available and false otherwise.
    pub fn take_locks(&mut self, message: &impl SVMMessage) -> bool {
        message
            .account_keys()
            .iter()
            .enumerate()
            .fold(true, |all_available, (index, pubkey)| {
                if message.is_writable(index) {
                    all_available & self.add_write(pubkey)
                } else {
                    all_available & self.add_read(pubkey)
                }
            })
    }

    /// Clears the read and write sets
    pub fn clear(&mut self) {
        self.read_set.clear();
        self.write_set.clear();
    }

    /// Check if an account can be read-locked
    fn can_read(&self, pubkey: &Pubkey) -> bool {
        !self.write_set.contains(pubkey)
    }

    /// Check if an account can be write-locked
    fn can_write(&self, pubkey: &Pubkey) -> bool {
        !self.write_set.contains(pubkey) && !self.read_set.contains(pubkey)
    }

    /// Add an account to the read-set.
    /// Returns true if the lock was available.
    fn add_read(&mut self, pubkey: &Pubkey) -> bool {
        let can_read = self.can_read(pubkey);
        self.read_set.insert(*pubkey);

        can_read
    }

    /// Add an account to the write-set.
    /// Returns true if the lock was available.
    fn add_write(&mut self, pubkey: &Pubkey) -> bool {
        let can_write = self.can_write(pubkey);
        self.write_set.insert(*pubkey);

        can_write
    }
}


/// Error type for reasons a transaction could not be scheduled.
pub enum TransactionSchedulingError {
    /// Transaction cannot be scheduled due to conflicts, or
    /// higher priority conflicting transactions are unschedulable.
    UnschedulableConflicts,
    /// Thread is not allowed to be scheduled on at this time.
    UnschedulableThread,
}