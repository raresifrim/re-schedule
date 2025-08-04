use crate::harness::scheduler::transaction_container::TransactionId;
use std::thread::ThreadId;
use crate::harness::scheduler::thread_aware_account_locks::ThreadSet;
use crossbeam_channel::{Sender,Receiver};
use crate::harness::consume_worker::{ConsumeWork,FinishedConsumeWork, TransactionBatchId};
use crate::harness::scheduler::thread_aware_account_locks::{ThreadAwareAccountLocks, MAX_THREADS};
use crate::harness::scheduler::scheduler::SchedulerError;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use crossbeam_channel::TryRecvError;
use crate::harness::scheduler::transaction_container::Container;
use itertools::izip;
use std::sync::{Arc,Mutex};

#[derive(Debug)]
pub struct Batches<Tx> {
    pub ids: Vec<Vec<TransactionId>>,
    transactions: Vec<Vec<Tx>>,
}

impl<Tx> Batches<Tx> {
    pub fn new(num_threads: usize, target_num_transactions_per_batch: usize) -> Self {
        Self {
            ids: vec![Vec::with_capacity(target_num_transactions_per_batch); num_threads],

            transactions: (0..num_threads)
                .map(|_| Vec::with_capacity(target_num_transactions_per_batch))
                .collect(),
        }
    }

    pub fn transactions(&self) -> &[Vec<Tx>] {
        &self.transactions
    }

    pub fn add_transaction_to_batch(
        &mut self,
        thread_id: usize,
        transaction_id: TransactionId,
        transaction: Tx,
    ) {
        self.ids[thread_id].push(transaction_id);
        self.transactions[thread_id].push(transaction);
    }

    pub fn take_batch(
        &mut self,
        thread_id: usize,
        target_num_transactions_per_batch: usize,
    ) -> (Vec<TransactionId>, Vec<Tx>) {
        (
            core::mem::replace(
                &mut self.ids[thread_id],
                Vec::with_capacity(target_num_transactions_per_batch),
            ),
            core::mem::replace(
                &mut self.transactions[thread_id],
                Vec::with_capacity(target_num_transactions_per_batch),
            )
        )
    }
}

/// A transaction has been scheduled to a thread.
pub struct TransactionSchedulingInfo<Tx> {
    pub thread_id: ThreadId,
    pub transaction: Tx
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
pub fn select_thread<Tx>(
    thread_set: ThreadSet,
    batch_cus_per_thread: &[u64],
    in_flight_cus_per_thread: &[u64],
    batches_per_thread: &[Vec<Tx>],
    in_flight_per_thread: &[usize],
) -> usize {
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

/// Common scheduler communication structure.
#[derive(Debug)]
pub struct SchedulingCommon<Tx> {
    pub consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
    pub finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    pub account_locks: ThreadAwareAccountLocks,
}

impl<Tx> SchedulingCommon<Tx> {
    pub fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    ) -> Self {
        let num_threads = consume_work_senders.len();
        assert!(num_threads > 0, "must have at least one worker");
        assert!(
            num_threads <= MAX_THREADS,
            "cannot have more than {MAX_THREADS} workers"
        );
        Self {
            consume_work_senders,
            finished_consume_work_receiver,
            account_locks: ThreadAwareAccountLocks::new(num_threads),
        }
    }

    /// Send a batch of transactions to the given thread's `ConsumeWork` channel.
    /// Returns the number of transactions sent.
    pub fn send_batch(
        &mut self,
        batches: &mut Batches<Tx>,
        thread_index: usize,
        target_transactions_per_batch: usize,
    ) -> Result<usize, SchedulerError> {
        
        if batches.ids[thread_index].is_empty() {
            return Ok(0);
        }

        let (ids, transactions) =
            batches.take_batch(thread_index, target_transactions_per_batch);

        let num_scheduled = ids.len();
        let work = ConsumeWork {
            batch_id: thread_index,
            ids,
            transactions,
        };
        self.consume_work_senders[thread_index]
            .send(work)
            .map_err(|_| SchedulerError::DisconnectedSendChannel("consume work sender"))?;

        Ok(num_scheduled)
    }

    /// Send all batches of transactions to the worker threads.
    /// Returns the number of transactions sent.
    pub fn send_batches(
        &mut self,
        batches: &mut Batches<Tx>,
        target_transactions_per_batch: usize,
    ) -> Result<usize, SchedulerError> {
        (0..self.consume_work_senders.len())
            .map(|thread_index| {
                self.send_batch(batches, thread_index, target_transactions_per_batch)
            })
            .sum()
    }
}

impl<Tx: TransactionWithMeta> SchedulingCommon<Tx> {
    /// Receive completed batches of transactions.
    /// Returns `Ok((num_transactions, num_retryable))` if a batch was received, `Ok((0, 0))` if no batch was received.
    pub fn try_receive_completed<C:Container<Tx>>(
        &mut self,
        container_mutex: &Arc<Mutex<C>>,
    ) -> Result<usize, SchedulerError> {
        //println!("Entered receiver");
        match self.finished_consume_work_receiver.try_recv() {
            Ok(FinishedConsumeWork {
                work:
                    ConsumeWork {
                        batch_id,
                        ids,
                        transactions,
                    },
                retryable_indexes,
            }) => {
                //println!("Consumed work: {:?}\n", ids);
                let num_transactions = ids.len();
                let num_retryable = retryable_indexes.len();

                // Free the locks
                self.complete_batch(batch_id, &transactions);

                Ok(num_transactions)
            }

            Err(TryRecvError::Empty) => {println!("No txs received"); Ok(0)},
            Err(TryRecvError::Disconnected) => {
                println!("Disconected from worker");
                Err(SchedulerError::DisconnectedRecvChannel("finished consume work",))
            },
        }
    }

    /// Mark a given `TransactionBatchId` as completed.
    /// This will update the internal tracking, including account locks.
    fn complete_batch(&mut self, batch_id: usize, transactions: &[Tx]) {
        for transaction in transactions {
            let account_keys = transaction.account_keys();
            let write_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| transaction.is_writable(index).then_some(key));
            let read_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| (!transaction.is_writable(index)).then_some(key));
            self.account_locks
                .unlock_accounts(write_account_locks, read_account_locks, batch_id);
        }
    }
}