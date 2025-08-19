use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkEntry;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use crate::harness::scheduler::thread_aware_account_locks::ThreadId;
use crate::harness::scheduler::thread_aware_account_locks::ThreadSet;
use crate::harness::scheduler::thread_aware_account_locks::TryLockError;
use ahash::{HashMap, HashMapExt};
use crossbeam_channel::{Receiver, Sender};
use solana_cost_model::cost_model::CostModel;
use solana_runtime::bank::Bank;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;
use std::collections::VecDeque;
use std::sync::Arc;

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
    account_locks: ThreadAwareAccountLocks,
    container: VecDeque<HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>>,
    batches: Batches<HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>>,
    in_flight_tracker: InFlightTracker,
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
            unique_txs: 0,
            total_txs: 0,
        };

        let container =
            VecDeque::<HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>>::with_capacity(
                batch_size,
            );

        Self {
            bank,
            num_workers,
            batch_size,
            target_scheduled_cus,
            working_account_set: ReadWriteAccountSet::default(),
            unschedulables: VecDeque::with_capacity(batch_size),
            scheduling_summary,
            container,
            batches: Batches::new(num_workers, batch_size / num_workers),
            account_locks: ThreadAwareAccountLocks::new(num_workers),
            in_flight_tracker: InFlightTracker::new(num_workers),
        }
    }

    fn get_next_txs(&mut self) {
        let num_threads = self.num_workers;
        let target_cu_per_thread = self.target_scheduled_cus / num_threads as u64;
        let mut schedulable_threads = ThreadSet::any(num_threads);
        let mut num_scanned: usize = 0;

        while num_scanned < self.batch_size && !self.container.is_empty() {
            num_scanned += 1;

            // Should always be in the container, during initial testing phase panic.
            // Later, we can replace with a continue in case this does happen.
            let Some(transaction_state) = self.container.pop_front() else {
                panic!("transaction state must exist")
            };

            if !self
                .working_account_set
                .check_locks(&transaction_state.transaction)
            {
                tracing::debug!(
                    "Found exisitng lock on accounts of new popped tx, sending all txs out..."
                );
                self.working_account_set.clear();
                self.container.push_back(transaction_state);
                // Push unschedulables back into the queue
                self.container.append(&mut self.unschedulables);
                //get back to parent function to send available txs in work_lanes
                return;
            }

            // Now check if the transaction can actually be scheduled.
            match self.try_schedule_transaction(&transaction_state, schedulable_threads) {
                Err(TransactionSchedulingError::UnschedulableConflicts) => {
                    self.unschedulables.push_back(transaction_state);
                }
                Err(TransactionSchedulingError::UnschedulableThread) => {
                    self.unschedulables.push_back(transaction_state);
                }
                Ok(TransactionSchedulingInfo { thread_id, cost }) => {
                    assert!(
                        self.working_account_set
                            .take_locks(&transaction_state.transaction),
                        "locks must be available"
                    );

                    let retry = transaction_state.retry;
                    self.scheduling_summary.total_txs += 1;
                    self.batches
                        .add_transaction_to_batch(thread_id, transaction_state, cost);
                    tracing::debug!("Added one more tx to the batch of worker {}", thread_id);

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
                        self.scheduling_summary.unique_txs += 1;
                    }

                    // If target batch size is reached, send all the batches
                    if self.batches.transactions()[thread_id].len()
                        >= self.batch_size / self.num_workers
                    {
                        self.working_account_set.clear();
                        // Push unschedulables back into the queue
                        self.container.append(&mut self.unschedulables);
                        //return back to parent function to send available txs
                        return;
                    }

                    // if the thread is at target_cu_per_thread, remove it from the schedulable threads
                    // if there are no more schedulable threads, stop scheduling.
                    if self.batches.total_cus()[thread_id] >= target_cu_per_thread {
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
        transaction_state: &HarnessTransaction<<GreedyScheduler as Scheduler>::Tx>,
        schedulable_threads: ThreadSet,
    ) -> Result<TransactionSchedulingInfo, TransactionSchedulingError> {
        let account_keys = transaction_state.transaction.account_keys();
        let write_account_locks = account_keys.iter().enumerate().filter_map(|(index, key)| {
            transaction_state
                .transaction
                .is_writable(index)
                .then_some(key)
        });
        let read_account_locks = account_keys.iter().enumerate().filter_map(|(index, key)| {
            (!transaction_state.transaction.is_writable(index)).then_some(key)
        });

        let thread_id = match self.account_locks.try_lock_accounts(
            write_account_locks,
            read_account_locks,
            schedulable_threads,
            |thread_set| {
                select_thread(
                    thread_set,
                    self.batches.total_cus(),
                    self.in_flight_tracker.cus_in_flight_per_thread(),
                    self.batches.transactions(),
                    self.in_flight_tracker.num_in_flight_per_thread(),
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

        let cost =
            CostModel::calculate_cost(&transaction_state.transaction, &self.bank.feature_set).sum();

        Ok(TransactionSchedulingInfo { thread_id, cost })
    }
}

const NUM_INGEST_RETRIES: u8 = 5;

impl Scheduler for GreedyScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;
    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>],
    ) -> Result<(), SchedulerError> {
        //set a number of retries to accumulate txs
        let mut num_retries = NUM_INGEST_RETRIES;
        let mut current_buffer_len = 0;

        loop {
            //STAGE 1: Ingest txs from the TxIssuer
            //quickly check if there are new incoming txs
            match issue_channel.try_recv() {
                Ok(tx) => {
                    tracing::debug!("Received txs from TxIssuer");
                    match tx.entry {
                        WorkEntry::SingleTx(tx) => {
                            self.container.push_back(tx);
                        }
                        WorkEntry::MultipleTxs(txs) => {
                            for tx in txs {
                                self.container.push_back(tx);
                            }
                        }
                    };
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
            self.get_next_txs();
            num_retries = NUM_INGEST_RETRIES;
            current_buffer_len = 0;

            //STAGE 3: send the current scheduled txs to the workers
            let tmp = 0..self.num_workers;
            for worker_index in tmp {
                let (txs, total_cus) = self
                    .batches
                    .take_batch(worker_index, self.batch_size / self.num_workers);

                if !txs.is_empty() {
                    tracing::debug!("Sending {} txs to worker {}", txs.len(), worker_index);

                    self.in_flight_tracker
                        .track_batch(txs.len(), total_cus, worker_index);

                    txs.iter().for_each(|tx| {
                        let account_keys = tx.transaction.account_keys();
                        let write_account_locks =
                            account_keys.iter().enumerate().filter_map(|(index, key)| {
                                tx.transaction.is_writable(index).then_some(key)
                            });
                        let read_account_locks =
                            account_keys.iter().enumerate().filter_map(|(index, key)| {
                                (!tx.transaction.is_writable(index)).then_some(key)
                            });

                        //unlock accounts now so we can schedule next round of txs
                        self.account_locks.unlock_accounts(
                            write_account_locks,
                            read_account_locks,
                            worker_index,
                        );
                    });

                    match execution_channels[worker_index].send(Work {
                        entry: WorkEntry::MultipleTxs(txs),
                    }) {
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

#[derive(Debug)]
pub struct Batches<Tx> {
    transactions: Vec<Vec<Tx>>,
    total_cus: Vec<u64>,
}

impl<Tx> Batches<Tx> {
    pub fn new(num_threads: usize, target_num_transactions_per_batch: usize) -> Self {
        Self {
            transactions: (0..num_threads)
                .map(|_| Vec::with_capacity(target_num_transactions_per_batch))
                .collect(),
            total_cus: vec![0; num_threads],
        }
    }

    pub fn total_cus(&self) -> &[u64] {
        &self.total_cus
    }

    pub fn transactions(&self) -> &[Vec<Tx>] {
        &self.transactions
    }

    pub fn add_transaction_to_batch(&mut self, thread_id: ThreadId, transaction: Tx, cus: u64) {
        self.transactions[thread_id].push(transaction);
        self.total_cus[thread_id] += cus;
    }

    pub fn take_batch(
        &mut self,
        thread_id: ThreadId,
        target_num_transactions_per_batch: usize,
    ) -> (Vec<Tx>, u64) {
        (
            core::mem::replace(
                &mut self.transactions[thread_id],
                Vec::with_capacity(target_num_transactions_per_batch),
            ),
            core::mem::replace(&mut self.total_cus[thread_id], 0),
        )
    }
}

/// A transaction has been scheduled to a thread.
pub struct TransactionSchedulingInfo {
    pub thread_id: ThreadId,
    pub cost: u64,
}

/// Error type for reasons a transaction could not be scheduled.
pub enum TransactionSchedulingError {
    /// Transaction cannot be scheduled due to conflicts, or
    /// higher priority conflicting transactions are unschedulable.
    UnschedulableConflicts,
    /// Thread is not allowed to be scheduled on at this time.
    UnschedulableThread,
}

/// Given the schedulable `thread_set`, select the thread with the least amount
/// of work queued up.
/// Currently, "work" is just defined as the number of transactions.
///
/// If the `chain_thread` is available, this thread will be selected, regardless of
/// load-balancing.
///
/// Panics if the `thread_set` is empty. This should never happen, see comment
/// on `ThreadAwareAccountLocks::try_lock_accounts`.
fn select_thread<Tx>(
    thread_set: ThreadSet,
    batch_cus_per_thread: &[u64],
    in_flight_cus_per_thread: &[u64],
    batches_per_thread: &[Vec<Tx>],
    in_flight_per_thread: &[usize],
) -> ThreadId {
    thread_set
        .contained_threads_iter()
        .map(|thread_id| {
            (
                thread_id,
                batch_cus_per_thread[thread_id] + in_flight_cus_per_thread[thread_id],
                batches_per_thread[thread_id].len() + in_flight_per_thread[thread_id],
            )
        })
        .min_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.cmp(&b.2)))
        .map(|(thread_id, _, _)| thread_id)
        .unwrap()
}

/// Tracks the number of transactions that are in flight for each thread.
#[derive(Debug)]
pub struct InFlightTracker {
    num_in_flight_per_thread: Vec<usize>,
    cus_in_flight_per_thread: Vec<u64>,
    batches: HashMap<TransactionBatchId, BatchEntry>,
    batch_id_generator: BatchIdGenerator,
}

#[derive(Debug)]
pub struct BatchEntry {
    thread_id: ThreadId,
    num_transactions: usize,
    total_cus: u64,
}

impl InFlightTracker {
    pub fn new(num_threads: usize) -> Self {
        Self {
            num_in_flight_per_thread: vec![0; num_threads],
            cus_in_flight_per_thread: vec![0; num_threads],
            batches: HashMap::new(),
            batch_id_generator: BatchIdGenerator::default(),
        }
    }

    /// Returns the number of transactions that are in flight for each thread.
    pub fn num_in_flight_per_thread(&self) -> &[usize] {
        &self.num_in_flight_per_thread
    }

    /// Returns the number of cus that are in flight for each thread.
    pub fn cus_in_flight_per_thread(&self) -> &[u64] {
        &self.cus_in_flight_per_thread
    }

    /// Tracks number of transactions and CUs in-flight for the `thread_id`.
    /// Returns a `TransactionBatchId` that can be used to stop tracking the batch
    /// when it is complete.
    pub fn track_batch(
        &mut self,
        num_transactions: usize,
        total_cus: u64,
        thread_id: ThreadId,
    ) -> TransactionBatchId {
        let batch_id = self.batch_id_generator.next();
        self.num_in_flight_per_thread[thread_id] += num_transactions;
        self.cus_in_flight_per_thread[thread_id] += total_cus;
        self.batches.insert(
            batch_id,
            BatchEntry {
                thread_id,
                num_transactions,
                total_cus,
            },
        );

        batch_id
    }

    /// Stop tracking the batch with given `batch_id`.
    /// Removes the number of transactions for the scheduled thread.
    /// Returns the thread id that the batch was scheduled on.
    ///
    /// # Panics
    /// Panics if the batch id does not exist in the tracker.
    pub fn complete_batch(&mut self, batch_id: TransactionBatchId) -> ThreadId {
        let Some(BatchEntry {
            thread_id,
            num_transactions,
            total_cus,
        }) = self.batches.remove(&batch_id)
        else {
            panic!("batch id is not being tracked");
        };
        self.num_in_flight_per_thread[thread_id] -= num_transactions;
        self.cus_in_flight_per_thread[thread_id] -= total_cus;

        thread_id
    }
}

#[derive(Default, Debug)]
pub struct BatchIdGenerator {
    next_id: u64,
}

impl BatchIdGenerator {
    pub fn next(&mut self) -> TransactionBatchId {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_sub(1);
        TransactionBatchId::new(id)
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TransactionBatchId(u64);
impl TransactionBatchId {
    pub fn new(index: u64) -> Self {
        Self(index)
    }
}
