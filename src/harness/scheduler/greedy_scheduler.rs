use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use crate::harness::scheduler::scheduler_controller::Transaction;
use crate::harness::scheduler::scheduling_common::SchedulingCommon;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::transaction_container::Container;
use crossbeam_channel::{Receiver, Sender};
use crate::harness::consume_worker::ConsumeWork;
use crate::harness::consume_worker::FinishedConsumeWork;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::thread_aware_account_locks::ThreadSet;
use crate::harness::scheduler::scheduling_common::select_thread;
use crate::harness::scheduler::scheduling_common::Batches;
use crate::harness::scheduler::scheduling_common::{TransactionSchedulingError, TransactionSchedulingInfo};
use std::thread::ThreadId;
use crate::harness::scheduler::thread_aware_account_locks::TryLockError;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use crate::harness::scheduler::scheduler::SchedulingSummary;

pub struct GreedySchedulerConfig {
    pub target_scheduled_cus: u64,
    pub max_scanned_transactions_per_scheduling_pass: usize,
    pub target_transactions_per_batch: usize,
}

impl Default for GreedySchedulerConfig {
    fn default() -> Self {
        Self {
            target_scheduled_cus: 60000000 / 4,
            max_scanned_transactions_per_scheduling_pass: 100_000,
            target_transactions_per_batch: 64,
        }
    }
}

/// Dead-simple scheduler that is efficient and will attempt to schedule
/// in priority order, scheduling anything that can be immediately
/// scheduled, up to the limits.
pub struct GreedyScheduler<Tx: TransactionWithMeta> {
    pub common: SchedulingCommon<Tx>,
    working_account_set: ReadWriteAccountSet,
    unschedulables: Vec<TransactionPriorityId>,
    pub config: GreedySchedulerConfig,
}

impl<Tx: TransactionWithMeta> GreedyScheduler<Tx> {
    pub fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        config: GreedySchedulerConfig,
    ) -> Self {
        Self {
            common: SchedulingCommon::new(consume_work_senders, finished_consume_work_receiver),
            working_account_set: ReadWriteAccountSet::default(),
            unschedulables: Vec::with_capacity(config.max_scanned_transactions_per_scheduling_pass),
            config,
        }
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx> for GreedyScheduler<Tx> {
    
    fn schedule<S: Container<Tx>>(
        &mut self,
        container: &mut S,
       
    ) -> Result<SchedulingSummary, SchedulerError> {
        
        let num_threads = self.common.consume_work_senders.len();
        let target_cu_per_thread = self.config.target_scheduled_cus / num_threads as u64;
        
        let mut schedulable_threads = ThreadSet::any(num_threads);
        
        if schedulable_threads.is_empty() {
            return Ok(SchedulingSummary {
                ..SchedulingSummary::default()
            });
        }

        // Track metrics on filter.
        let mut num_scanned: usize = 0;
        let mut num_scheduled:usize = 0;
        let mut num_sent: usize = 0;
        let mut num_unschedulable_conflicts: usize = 0;
        let mut num_unschedulable_threads: usize = 0;

        let mut batches = Batches::new(num_threads, self.config.target_transactions_per_batch);
        while num_scanned < self.config.max_scanned_transactions_per_scheduling_pass
            && !schedulable_threads.is_empty()
            && !container.is_empty()
        {
            let Some(transaction) = container.pop() else {
                unreachable!("container is not empty")
            };

            num_scanned += 1;

            // Should always be in the container, during initial testing phase panic.
            // Later, we can replace with a continue in case this does happen.
            
            if !self
                .working_account_set
                .check_locks(transaction)
            {
                //println!("Conflict between accounts detected..sending current txs out!");
                self.working_account_set.clear();
                num_sent += self
                    .common
                    .send_batches(&mut batches, self.config.target_transactions_per_batch)?;
            }

            // Now check if the transaction can actually be scheduled.
            match try_schedule_transaction(
                transaction,
                &mut self.common.account_locks,
                schedulable_threads,
                |thread_set| {
                    select_thread(
                        thread_set,
                        batches.total_cus(),
                        self.common.in_flight_tracker.cus_in_flight_per_thread(),
                        batches.transactions(),
                        self.common.in_flight_tracker.num_in_flight_per_thread(),
                    )
                },
            ) {
                Err(TransactionSchedulingError::UnschedulableConflicts) => {
                    //println!("Got unschedulable conflict!");
                    num_unschedulable_conflicts += 1;
                    self.unschedulables.push(id);
                }
                Err(TransactionSchedulingError::UnschedulableThread) => {
                    //println!("Got unschedulable thread!");
                    num_unschedulable_threads += 1;
                    self.unschedulables.push(id);
                }
                Ok(TransactionSchedulingInfo {
                    thread_id,
                    transaction,
                }) => {
                    assert!(
                        self.working_account_set.take_locks(&transaction),
                        "locks must be available"
                    );
                    num_scheduled += 1;
                    batches.add_transaction_to_batch(thread_id, id.id, transaction);
                    //println!("Tx {:?} added to batch", id.id);
                    // If target batch size is reached, send all the batches
                    if batches.transactions()[thread_id].len()
                        >= self.config.target_transactions_per_batch
                    {
                        self.working_account_set.clear();
                        num_sent += self.common.send_batches(
                            &mut batches,
                            self.config.target_transactions_per_batch,
                        )?;
                    }

                    // if the thread is at target_cu_per_thread, remove it from the schedulable threads
                    // if there are no more schedulable threads, stop scheduling.
                    if self.common.in_flight_tracker.cus_in_flight_per_thread()[thread_id]
                        + batches.total_cus()[thread_id]
                        >= target_cu_per_thread
                    {
                        schedulable_threads.remove(thread_id);
                        if schedulable_threads.is_empty() {
                            break;
                        }
                    }
                }
            }
        }
        //println!("Final batch: {:?}", batches.ids);
        self.working_account_set.clear();
        // Use zero here to avoid allocating since we are done with `Batches`.
        num_sent += self.common.send_batches(&mut batches, 0)?;
        assert_eq!(
            num_scheduled, num_sent,
            "number of scheduled and sent transactions must match"
        );

        // Push unschedulables back into the queue
        container.push_ids_into_queue(self.unschedulables.drain(..));

        Ok(SchedulingSummary {
            num_scheduled,
            num_unschedulable_conflicts,
            num_unschedulable_threads,
        })
    }

    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx> {
        &mut self.common
    }
}

fn try_schedule_transaction(
    transaction: &mut Transaction,
    account_locks: &mut ThreadAwareAccountLocks,
    schedulable_threads: ThreadSet,
    thread_selector: impl Fn(ThreadSet) -> ThreadId,
) -> Result<TransactionSchedulingInfo<Transaction>, TransactionSchedulingError> {

    // Schedule the transaction if it can be.
    let account_keys = transaction.account_keys();
    let write_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| transaction.is_writable(index).then_some(key));
    let read_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| (!transaction.is_writable(index)).then_some(key));
    //println!("Thread aware total locks:{:?}", account_locks.locks.len());
    let thread_id = match account_locks.try_lock_accounts(
        write_account_locks,
        read_account_locks,
        schedulable_threads,
        thread_selector,
    ) {
        Ok(thread_id) => thread_id,
        Err(TryLockError::MultipleConflicts) => {
            return Err(TransactionSchedulingError::UnschedulableConflicts);
        }
        Err(TryLockError::ThreadNotAllowed) => {
            return Err(TransactionSchedulingError::UnschedulableThread);
        }
    };

    Ok(TransactionSchedulingInfo {
        thread_id,
        transaction,
    })
}