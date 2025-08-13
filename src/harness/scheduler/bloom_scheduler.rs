use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkEntry;
use ahash::{HashMap, HashMapExt};
use bloom_1x::bloom::Bloom1X;
use bloom_1x::bloom::QueryResult;
use crossbeam_channel::{Receiver, Sender};
use solana_cost_model::cost_model::CostModel;
use solana_runtime::bank::Bank;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_sdk::transaction::SanitizedTransaction;
use solana_svm_transaction::svm_message::SVMMessage;
use std::collections::VecDeque;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use tracing::info;

//number of tries to fill in the buffer to max capacity
const NUM_INGEST_RETRIES: usize = 3;
//maximum number of accounts that a tx can use
const MAX_NUM_TX_ACCOUNTS:usize = 64;
pub type WorkerId = usize;

pub struct BloomScheduler {
    num_workers: usize,
    /// data structure to hold the current conflicts per worker
    conflict_families: Vec<ConflictFamily>,
    /// number of txs to collect before starting ti schedule
    batch_size: usize,
    /// local buffer to accumulate multiple txs for burst mode
    buffer: VecDeque<HarnessTransaction<<BloomScheduler as Scheduler>::Tx>>,
    /// structure to hold the current txs to be scheduled to each worker
    work_lanes: HashMap<WorkerId, Vec<HarnessTransaction<<BloomScheduler as Scheduler>::Tx>>>,
    /// results from querying the Read and Write filters for each account of a tx
    /// only one set needed as we iterate over each conflicting family sequentially 
    r_query_results: Vec<QueryResult>,
    w_query_results: Vec<QueryResult>
}

struct ConflictFamily {
    read_filter: Bloom1X,
    write_filter: Bloom1X,
}

impl BloomScheduler {
    /// create a Bloom-based Scheduler where one single hash (XooDoo-NC) function is used with state size equal to 96 bits
    /// k -> number of hashes to extract from main hash digest
    /// l -> number of rows per bloom filter
    /// w -> width of row inside bloom filter
    /// each scheduler maintains a read and write filter per each worker
    /// mode -> stream txs one after the other as soon as possible, or wait for larger burst of txs
    pub fn new(num_workers: usize, k: usize, l: usize, w: usize, batch_size: usize) -> Self {
        let mut conflict_families = vec![];

        for _ in 0..num_workers {
            let conflict_family = ConflictFamily {
                read_filter: Bloom1X::new(k, l, w, 96),
                write_filter: Bloom1X::new(k, l, w, 96),
            };
            conflict_families.push(conflict_family);
        }

        let buffer =
            VecDeque::<HarnessTransaction<<BloomScheduler as Scheduler>::Tx>>::with_capacity(
                batch_size,
            );
        let work_lanes = HashMap::with_capacity(num_workers);

        let r_query_results = Vec::with_capacity(MAX_NUM_TX_ACCOUNTS);
        let w_query_results= Vec::with_capacity(MAX_NUM_TX_ACCOUNTS);
        Self {
            num_workers,
            conflict_families,
            batch_size,
            buffer,
            work_lanes,
            r_query_results,
            w_query_results
        }
    }

    pub fn flush_filters(&mut self) {
        for i in 0..self.conflict_families.len() {
            self.conflict_families[i].read_filter.clear();
            self.conflict_families[i].write_filter.clear();
        }
    }

    fn schedule_burst(&mut self) -> SchedulingSummary {
        
        let mut num_scheduled = 0;
        let mut hasher = DefaultHasher::new();

        while let Some(harness_tx) = self.buffer.pop_front() {
            //if we arrived here, we are sure that there is at least a tx inside the buffer
            
            //default to first worker if no conflict is found
            let mut next_worker: usize = 0;
            //get read and write accounts stated in the tx
            let tx_accounts = harness_tx.transaction.get_account_locks_unchecked();

            //iterate over each conflict family until the current tx account sets intersect with the conflict families
            'main_loop: for (worker_index, cf) in self.conflict_families.iter().enumerate() {
                let mut and_result = 0;

                //each tx is a blackbox with worst case scenario -> locks imediatly and unlocks after execution
                //simulate execution with sleep
                //get utilization percentage of each worker, how much is it used?
                //(optional)
                //1: when each write happens
                //2: when each read happens
                //3: assuming RoundRobin, how many actual conflicts occur
                //4: using 1+2, estimate 3

                for write_account in tx_accounts.writable.iter() {
                    write_account.hash(&mut hasher);
                    let index = hasher.finish();
                    let write_filter_result = cf.write_filter.query_u64_with_result(index);
                    let read_filter_result = cf.read_filter.query_u64_with_result(index);
                    and_result |= read_filter_result.and_result | write_filter_result.and_result;
                    self.w_query_results.push(write_filter_result);
                }

                for read_account in tx_accounts.readonly.iter() {
                    read_account.hash(&mut hasher);
                    let index = hasher.finish();
                    let write_filter_result = cf.write_filter.query_u64_with_result(index);
                    let read_filter_result = cf.read_filter.query_u64_with_result(index);
                    and_result |= write_filter_result.and_result;
                    self.r_query_results.push(read_filter_result);
                }

                if and_result == 1 {
                    //found conflict within current family
                    next_worker = worker_index;
                    break 'main_loop;
                }

                //if no conflict found here, clear and move forward
                self.r_query_results.clear();
                self.w_query_results.clear();
            }

            while let Some(read_result) = self.r_query_results.pop(){
                self.conflict_families[next_worker]
                    .read_filter
                    .update_filter(read_result);
            }
            
            while let Some(write_result) = self.w_query_results.pop() {
                self.conflict_families[next_worker]
                    .write_filter
                    .update_filter(write_result);
            }

            let mut v = vec![harness_tx];
            self.work_lanes
                .entry(next_worker)
                .and_modify(|f| {
                    f.append(&mut v);
                })
                .or_insert(v);
            num_scheduled += 1;

        }
        
        info!("Scheduling summary per worker:");
        for (worker_index, work) in self.work_lanes.iter().by_ref() {
            info!("Worker {} -> {} transactions", worker_index, work.len());
        }
        SchedulingSummary {
            num_scheduled,
            num_unschedulable_conflicts: 0,
            num_unschedulable_threads: 0,
        }
    }
}

impl Scheduler for BloomScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;
    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>],
    ) -> Result<SchedulingSummary, SchedulerError> {
        //set a number of retries to accumulate txs
        let mut num_retries = NUM_INGEST_RETRIES;
        let mut current_buffer_len = 0;

        loop {
            //STAGE 1: Ingest txs from the TxIssuer
            //quickly check if there are new incoming txs
            match issue_channel.try_recv() {
                Ok(tx) => {
                    info!("Received txs from TxIssuer");
                    match tx.entry {
                        WorkEntry::SingleTx(tx) => {
                            self.buffer.push_back(tx);
                        }
                        WorkEntry::MultipleTxs(txs) => {
                            for tx in txs {
                                self.buffer.push_back(tx);
                            }
                        }
                    };
                }

                //error might be actualy just empty or a real error like disconnected
                Err(e) => {
                    match e {
                        crossbeam_channel::TryRecvError::Empty => {
                            if self.buffer.len() == 0 {
                                info!("No txs on the channel and no txs buffered locally.")
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

            if self.buffer.len() == 0 {
                continue;
            }

            //STAGE 2: Check and schedule accumulated txs in the local buffer
            if self.buffer.len() <= self.batch_size {
                //not enough work
                if current_buffer_len < self.buffer.len() {
                    current_buffer_len = self.buffer.len();
                } else {
                    //if we still have same buffered txs subtract the number or retries
                    num_retries -= 1;
                }
                if num_retries != 0 {
                    //try to get more txs before scheduling
                    continue;
                }
            }
            //once we got here, we know we can start scheduling the txs
            let scheduling_summary = self.schedule_burst();

            //STAGE 3: send the current scheduled txs to the workers
            for worker_index in 0..self.num_workers {
                match self.work_lanes.remove_entry(&worker_index) {
                    Some(lane) => {
                        match execution_channels[lane.0].send(Work {
                            entry: WorkEntry::MultipleTxs(lane.1),
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
                    None => continue,
                };
            }

            return Ok(scheduling_summary);
        }
    }
}
