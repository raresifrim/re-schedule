use crate::harness::executor::execution_tracker::ExecutionTracker;
use crate::harness::scheduler::greedy_scheduler::ReadWriteAccountSet;
use crate::harness::scheduler::greedy_scheduler::TransactionSchedulingError;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkerId;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use crate::harness::scheduler::thread_aware_account_locks::ThreadId;
use crate::harness::scheduler::thread_aware_account_locks::ThreadSet;
use crate::harness::scheduler::thread_aware_account_locks::TryLockError;
use crate::harness::scheduler::thread_aware_account_locks::select_thread;
use ahash::{HashMap, HashMapExt};
use crossbeam_channel::{Receiver, Sender};
use itertools::Itertools;
use prio_graph::{AccessKind, GraphNode, PrioGraph, TopLevelId};
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;
use std::sync::Arc;
use std::sync::Mutex;

/// Dead-simple scheduler that is efficient and will attempt to schedule
/// in priority order, scheduling anything that can be immediately
/// scheduled, up to the limits.
pub struct PrioGraphScheduler {
    num_workers: usize,
    batch_size: usize,
    target_scheduled_cus: u64,
    prio_graph: SchedulerPrioGraph,
    working_account_set: ReadWriteAccountSet,
    scheduling_summary: SchedulingSummary,
    container: HashMap<TransactionId, HarnessTransaction<<PrioGraphScheduler as Scheduler>::Tx>>,
    unschedulables: Vec<HarnessTransaction<<PrioGraphScheduler as Scheduler>::Tx>>,
    ids: Vec<usize>,
    work_lanes: HashMap<WorkerId, Work<<PrioGraphScheduler as Scheduler>::Tx>>,
    thread_trackers: Vec<Arc<ExecutionTracker>>,
    account_locks: Arc<Mutex<ThreadAwareAccountLocks>>,
}

impl PrioGraphScheduler {
    pub fn new(
        account_locks: Arc<Mutex<ThreadAwareAccountLocks>>,
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

        let mut ids = Vec::with_capacity(batch_size);
        let container = HashMap::with_capacity(batch_size);
        for i in 0..batch_size {
            ids.push(i); //push available ids that can be taken by incoming txs
        }

        let mut work_lanes = HashMap::with_capacity(num_workers);
        for i in 0..num_workers {
            work_lanes.insert(
                i,
                Work {
                    entry: vec![],
                    total_cus: 0,
                },
            );
        }

        Self {
            prio_graph: PrioGraph::new(passthrough_priority),
            account_locks,
            num_workers,
            batch_size,
            target_scheduled_cus,
            working_account_set: ReadWriteAccountSet::default(),
            scheduling_summary,
            container,
            ids,
            unschedulables: vec![],
            work_lanes,
            //work trackers should be added through a separate function call defined by the Scheduler trait
            thread_trackers: vec![],
        }
    }

    fn get_next_txs(
        &mut self,
        execution_channels: &[Sender<Work<<PrioGraphScheduler as Scheduler>::Tx>>],
    ) {
        let num_threads = self.num_workers;
        let target_cu_per_thread = self.target_scheduled_cus / num_threads as u64;
        let mut schedulable_threads = ThreadSet::any(num_threads);
        let mut num_scanned: usize = 0;

        for id in 0..self.container.capacity() {
            let tx = match self.container.get(&id) {
                Some(t) => t,
                None => continue, //check next entry in the map
            };
            self.prio_graph.insert_transaction(
                TransactionPriorityId {
                    priority: 1, //same prioroty for all txs
                    id: id,
                },
                Self::get_transaction_account_access(&tx.transaction),
            );
        }

        let mut unblock_this_batch = Vec::with_capacity(self.batch_size);

        while num_scanned < self.batch_size {
            if self.prio_graph.is_empty() {
                break;
            }

            while let Some(id) = self.prio_graph.pop() {
                num_scanned += 1;
                unblock_this_batch.push(id);
                let tx = self.container.remove(&id.id).unwrap();
                // Now check if the transaction can actually be scheduled.
                match self.try_schedule_transaction(&tx, schedulable_threads) {
                    Err(_) => {
                        tracing::debug!("Got UnschedulableConflicts");
                        self.container.insert(id.id, tx);
                    }
                    Ok(thread_id) => {
                        let retry = tx.retry;
                        let cu_cost = tx.cu_cost;

                        let mut v = vec![tx];
                        self.work_lanes
                            .entry(thread_id)
                            .and_modify(|f| {
                                f.entry.append(&mut v);
                                f.total_cus += cu_cost;
                            })
                            .or_insert(Work {
                                entry: v,
                                total_cus: cu_cost,
                            });

                        tracing::debug!("Added one more tx to the batch of worker {}", thread_id);
                        self.ids.push(id.id); //reserve this id as free for use when receiving new transactions

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

                        // If target batch size is reached on a thread, then send that batch
                        if workload >= self.batch_size / self.num_workers {
                            match self.work_lanes.remove_entry(&thread_id) {
                                Some(lane) => {
                                    let _ = execution_channels[lane.0].send(lane.1);
                                }
                                None => {}
                            };
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

                if num_scanned >= self.batch_size {
                    break;
                }
            }

            // Send all non-empty batches
            for worker_index in 0..self.num_workers {
                match self.work_lanes.remove_entry(&worker_index) {
                    Some(lane) => {
                        let _ = execution_channels[lane.0].send(lane.1);
                    }
                    None => {}
                };
            }

            // Unblock all transactions that were blocked by the transactions that were just sent.
            for id in unblock_this_batch.drain(..) {
                self.prio_graph.unblock(&id);
            }
        }

        // Send all non-empty batches
        for worker_index in 0..self.num_workers {
            match self.work_lanes.remove_entry(&worker_index) {
                Some(lane) => {
                    let _ = execution_channels[lane.0].send(lane.1);
                }
                None => {}
            };
        }

        //clear structures for next round
        self.prio_graph.clear();
        self.working_account_set.clear();
    }

    fn try_schedule_transaction(
        &mut self,
        harness_tx: &HarnessTransaction<<PrioGraphScheduler as Scheduler>::Tx>,
        schedulable_threads: ThreadSet,
    ) -> Result<ThreadId, TransactionSchedulingError> {
        if !self
            .working_account_set
            .check_locks(&harness_tx.transaction)
        {
            self.working_account_set.take_locks(&harness_tx.transaction);
            return Err(TransactionSchedulingError::UnschedulableConflicts);
        }

        let account_keys = harness_tx.transaction.account_keys();
        let write_account_locks = account_keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| harness_tx.transaction.is_writable(index).then_some(key))
            .collect_vec();
        let read_account_locks = account_keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| (!harness_tx.transaction.is_writable(index)).then_some(key))
            .collect_vec();

        let mut mutex = self.account_locks.lock().unwrap();
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
                    harness_tx.cu_cost,
                )
            },
        ) {
            Ok(thread_id) => thread_id,
            Err(TryLockError::MultipleConflicts) => {
                self.working_account_set.take_locks(&harness_tx.transaction);
                return Err(TransactionSchedulingError::UnschedulableConflicts);
            }
            Err(TryLockError::ThreadNotAllowed) => {
                self.working_account_set.take_locks(&harness_tx.transaction);
                return Err(TransactionSchedulingError::UnschedulableThread);
            }
        };

        Ok(thread_id)
    }

    fn get_transaction_account_access(
        message: &impl SVMMessage,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + '_ {
        message
            .account_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| {
                if message.is_writable(index) {
                    (*key, AccessKind::Write)
                } else {
                    (*key, AccessKind::Read)
                }
            })
    }
}

const NUM_INGEST_RETRIES: u8 = 3;

impl Scheduler for PrioGraphScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;

    fn add_thread_trackers(&mut self, execution_trackers: Vec<Arc<ExecutionTracker>>) {
        self.thread_trackers = execution_trackers;
    }

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
            //check if we have any txs in the unschedulables queue
            while self.unschedulables.len() > 0
                && let Some(id) = self.ids.pop()
            {
                self.container
                    .insert(id, self.unschedulables.pop().unwrap());
            }

            if (self.ids.len() > 0) { //if we still have ids left to occupy
                //quickly check if there are new incoming txs
                match issue_channel.try_recv() {
                    Ok(work) => {
                        tracing::debug!("Received txs from TxIssuer");
                        for tx in work.entry {
                            if let Some(id) = self.ids.pop(){
                                //use the main container if an id is available
                                self.container.insert(id, tx);
                            } else {
                                // otherwise same the tx for later use
                                self.unschedulables.push(tx);
                            }
                        }
                    }

                    //error might be actualy just empty or a real error like disconnected
                    Err(e) => {
                        match e {
                            crossbeam_channel::TryRecvError::Empty => {
                                if self.container.is_empty() {
                                    tracing::debug!(
                                        "No txs on the channel and no txs buffered locally. Maybe we receive something later..."
                                    )
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
            }

            //STAGE 2: Check and schedule accumulated txs in the local buffer
            if self.ids.len() > 0 {
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
            self.get_next_txs(execution_channels);
            num_retries = NUM_INGEST_RETRIES;
            current_buffer_len = 0;
        }
    }

    fn get_summary(&self) -> SchedulingSummary {
        self.scheduling_summary.clone()
    }
}

//////////////////////////////////////// Suport Structures ////////////////////////////////////////

use {
    solana_pubkey::Pubkey, solana_svm_transaction::svm_message::SVMMessage, std::hash::Hash,
    std::hash::Hasher,
};

type TransactionId = usize;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TransactionPriorityId {
    pub(crate) priority: u64,
    pub(crate) id: TransactionId,
}

impl TransactionPriorityId {
    pub(crate) fn new(priority: u64, id: TransactionId) -> Self {
        Self { priority, id }
    }
}

impl Hash for TransactionPriorityId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl TopLevelId<Self> for TransactionPriorityId {
    fn id(&self) -> Self {
        *self
    }
}

#[inline(always)]
fn passthrough_priority(
    id: &TransactionPriorityId,
    _graph_node: &GraphNode<TransactionPriorityId>,
) -> TransactionPriorityId {
    *id
}

type SchedulerPrioGraph = PrioGraph<
    TransactionPriorityId,
    Pubkey,
    TransactionPriorityId,
    fn(&TransactionPriorityId, &GraphNode<TransactionPriorityId>) -> TransactionPriorityId,
>;
