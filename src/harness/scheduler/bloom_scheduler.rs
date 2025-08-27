use crate::harness::executor::execution_tracker::ExecutionTracker;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkerId;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use crate::harness::scheduler::thread_aware_account_locks::ThreadSet;
use crate::harness::scheduler::thread_aware_account_locks::select_thread;
use ahash::{HashMap, HashMapExt};
use bloom_1x::bloom::Bloom1X;
use bloom_1x::bloom::QueryResult;
use crossbeam_channel::{Receiver, Sender};
use itertools::{EitherOrBoth::*, Itertools};
use rapidhash::quality::RapidBuildHasher;
use solana_accounts_db::account_locks;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;
use std::collections::VecDeque;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::info;

//number of tries to fill in the buffer to max capacity
const NUM_INGEST_RETRIES: usize = 5;
//maximum number of accounts that a tx can use
const MAX_NUM_TX_ACCOUNTS: usize = 64;

pub struct BloomScheduler {
    num_workers: usize,
    /// data structure to hold the current conflicts per worker
    conflict_families: Vec<ConflictFamily>,
    /// number of txs to collect before starting ti schedule
    batch_size: usize,
    /// local buffer to accumulate multiple txs for burst mode
    container: VecDeque<HarnessTransaction<<BloomScheduler as Scheduler>::Tx>>,
    /// structure to hold the current txs to be scheduled to each worker
    work_lanes: HashMap<WorkerId, Work<<BloomScheduler as Scheduler>::Tx>>,
    /// results from querying the Read and Write filters for each account of a tx
    r_query_results: Vec<QueryResult>,
    w_query_results: Vec<QueryResult>,
    thread_trackers: Vec<Arc<ExecutionTracker>>,
    scheduling_summary: SchedulingSummary,
    /// should be used when computing the False Positive Rate
    account_locks: Option<Arc<Mutex<ThreadAwareAccountLocks>>>,
    /// if enabled, use the ThreadAwareAccountLocks to check if the filter correctly matched accounts
    compute_false_positive_rate: bool,
    /// computes the false positive rate of the bloom filter
    false_positive_rate: f64,
    total_bloom_checks: u64,
    /// computes the conflict rate as the number of txs conflicting on multiple threads at once
    multiple_conflict_rate: f64,
    total_accounts_checked: u64,
}

struct ConflictFamily {
    read_filter: Bloom1X,
    write_filter: Bloom1X,
}

// TODO: Unit benchmarks for false positive fail rate
// TODO: Identify size of batch
// TODO: Size of burst
impl BloomScheduler {
    /// create a Bloom-based Scheduler where one single hash (XooDoo-NC) function is used with state size equal to 96 bits
    /// k -> number of hashes to extract from main hash digest
    /// l -> number of rows per bloom filter
    /// w -> width of row inside bloom filter
    /// each scheduler maintains a read and write filter per each worker
    pub fn new(
        num_workers: usize,
        k: usize,
        l: usize,
        w: usize,
        batch_size: usize,
        compute_false_positive_rate: bool,
        account_locks: Option<Arc<Mutex<ThreadAwareAccountLocks>>>
    ) -> Self {
        let mut conflict_families = vec![];

        for _ in 0..num_workers {
            let conflict_family = ConflictFamily {
                read_filter: Bloom1X::new(k, l, w, 96),
                write_filter: Bloom1X::new(k, l, w, 96),
            };
            conflict_families.push(conflict_family);
        }

        let container =
            VecDeque::<HarnessTransaction<<BloomScheduler as Scheduler>::Tx>>::with_capacity(
                batch_size,
            );

        let work_lanes = HashMap::with_capacity(num_workers);

        let r_query_results = Vec::with_capacity(MAX_NUM_TX_ACCOUNTS);
        let w_query_results = Vec::with_capacity(MAX_NUM_TX_ACCOUNTS);

        let mut txs_per_worker = HashMap::with_capacity(num_workers);
        for i in 0..num_workers {
            txs_per_worker.insert(i, Default::default());
        }
        let scheduling_summary = SchedulingSummary {
            txs_per_worker,
            useful_txs: 0,
            total_txs: 0,
        };

        /* 
        let new_account_locks;
        if compute_false_positive_rate {
            new_account_locks = account_locks.unwrap();
        } else {
            // just create an empty struct if we don't want to compute fpr
            new_account_locks = Arc::new(Mutex::new(ThreadAwareAccountLocks::new(num_workers)));
        }
        */

        Self {
            num_workers,
            conflict_families,
            account_locks,
            batch_size,
            container,
            work_lanes,
            r_query_results,
            w_query_results,
            thread_trackers: vec![],
            scheduling_summary,
            compute_false_positive_rate,
            false_positive_rate: 0.0,
            total_bloom_checks: 0,
            multiple_conflict_rate: 0.0,
            total_accounts_checked: 0
        }
    }

    pub fn flush_filters(&mut self) {
        for i in 0..self.conflict_families.len() {
            self.conflict_families[i].read_filter.clear();
            self.conflict_families[i].write_filter.clear();
        }
    }

    fn schedule_burst(
        &mut self,
        hasher: rapidhash::inner::RapidBuildHasher<true, true>,
        schedulable_threads: ThreadSet,
    ) {
        //save worker that should receive the scheduled work
        let mut next_worker: usize = self.num_workers;

        while let Some(harness_tx) = self.container.pop_front() {
            //if we arrived here, we are sure that there is at least a tx inside the buffer
            //get read and write accounts stated in the tx
            let tx_accounts = harness_tx.transaction.get_account_locks_unchecked();

            for write_account in tx_accounts.writable.iter() {
                // hasher finish does not reset the internal state so we must recreate the hasher each time
                let index = hasher.hash_one(write_account);
                let write_filter_result = self.conflict_families[0].read_filter.search_u64(index);
                self.w_query_results.push(write_filter_result);
            }

            for read_account in tx_accounts.readonly.iter() {
                let index = hasher.hash_one(read_account);
                //all conflict families have the same filer
                //given a hash, any bloom filter will return the same location where the response of the filter will be located
                //so we can query the filter of the first conflict family to just get the location
                let read_filter_result = self.conflict_families[0].read_filter.search_u64(index);
                self.r_query_results.push(read_filter_result);
            }

            let mut bloom_threadset = ThreadSet::none();
            'main_loop: for pair in self
                .w_query_results
                .iter()
                .zip_longest(self.r_query_results.iter())
            {
                let mut and_result = 0;
                //iterate over each conflict family until the current tx account sets intersect with the conflict families
                for (worker_index, cf) in self.conflict_families.iter().enumerate() {
                    //using the previous generated locations
                    //just quickly read the actual values of the bits at those locations
                    match pair {
                        Both(w, r) => {
                            let waw_hazard = cf.write_filter.query_by_result(w);
                            let war_hazard = cf.read_filter.query_by_result(w);
                            let raw_hazard = cf.write_filter.query_by_result(r);
                            and_result |= war_hazard | waw_hazard | raw_hazard;
                        }
                        Left(w) => {
                            let waw_hazard = cf.write_filter.query_by_result(w);
                            let war_hazard = cf.read_filter.query_by_result(w);
                            and_result |= war_hazard | waw_hazard;
                        }
                        Right(r) => {
                            let raw_hazard = cf.write_filter.query_by_result(r);
                            and_result |= raw_hazard;
                        }
                    }
                    if and_result == 1 {
                        bloom_threadset.insert(worker_index);
                        // TODO: add switch between breaking on first conflict or collecting all
                        break 'main_loop;
                    }
                }
            }

            if bloom_threadset.is_empty() {
                next_worker = select_thread::<Self>(
                    schedulable_threads,
                    &self.thread_trackers,
                    &self.work_lanes,
                    1,
                    harness_tx.cu_cost,
                );
            } else if bloom_threadset.only_one_contained().is_some(){
                // we sould always arive here if we found a conflict and we are not computing the fpr
                next_worker = bloom_threadset.only_one_contained().unwrap();
            } else {
                next_worker = select_thread::<Self>(
                    bloom_threadset,
                    &self.thread_trackers,
                    &self.work_lanes,
                    1,
                    harness_tx.cu_cost,
                );
            }

            if self.compute_false_positive_rate {
                self.compute_bloom_fpr(&harness_tx, hasher, next_worker);
            }

            for pair in self
                .w_query_results
                .iter()
                .zip_longest(self.r_query_results.iter())
            {
                //using the previous generated locations just quickly
                //update the values of the bits at those locations for the selected conflict family
                match pair {
                    Both(w, r) => {
                        self.conflict_families[next_worker]
                            .write_filter
                            .update_filter(w);

                        self.conflict_families[next_worker]
                            .read_filter
                            .update_filter(r);
                    }
                    Left(w) => {
                        self.conflict_families[next_worker]
                            .write_filter
                            .update_filter(w);
                    }
                    Right(r) => {
                        self.conflict_families[next_worker]
                            .read_filter
                            .update_filter(r);
                    }
                }
            }

            // TODO: Maybe bug
            self.r_query_results.clear();
            self.w_query_results.clear();

            let retry = harness_tx.retry;
            let cu_cost = harness_tx.cu_cost;
            let mut v = vec![harness_tx];

            self.work_lanes
                .entry(next_worker)
                .and_modify(|f| {
                    f.entry.append(&mut v);
                    f.total_cus += cu_cost;
                })
                .or_insert(Work {
                    entry: v,
                    total_cus: cu_cost,
                });

            let report = self
                .scheduling_summary
                .txs_per_worker
                .get_mut(&next_worker)
                .unwrap();
            report.total += 1;
            if retry {
                report.retried += 1;
            } else {
                report.unique += 1;
                self.scheduling_summary.useful_txs += 1;
            }
            self.scheduling_summary.total_txs += 1;
        }
    }

    fn compute_bloom_fpr(
        &mut self,
        harness_tx: &HarnessTransaction<<BloomScheduler as Scheduler>::Tx>,
        hasher: rapidhash::inner::RapidBuildHasher<true, true>,
        next_worker: usize,
    ) {
        use crate::harness::scheduler::thread_aware_account_locks::AccountLocks;

        let tx_accounts = harness_tx.transaction.get_account_locks_unchecked();
        let binding = self.account_locks.clone().unwrap();
        let mut mutex = binding.lock().unwrap();
        
        for write_account in tx_accounts.writable.iter() {
            let index = hasher.hash_one(write_account);
            let write_filter_result = self.conflict_families[0].write_filter.search_u64(index);
            tracing::debug!("Writable Account {:?} ---- Hash: {:?} ---- Query: {:?}", write_account.to_string(), index, write_filter_result);
            let mut bloom_set = ThreadSet::none();
            for (worker_index, cf) in self.conflict_families.iter().enumerate() {
                let waw_hazard = cf.write_filter.query_by_result(&write_filter_result);
                let war_hazard = cf.read_filter.query_by_result(&write_filter_result);
                tracing::debug!("ConflictFamily {worker_index}: WAW({waw_hazard}); WAR({war_hazard})");
                if (war_hazard | waw_hazard) == 0x1 {
                    bloom_set.insert(worker_index);
                }
            }
            let conflicting_threads = match mutex.locks.get(write_account) {
                None => ThreadSet::none(),
                Some(AccountLocks {
                    write_locks: Some(write_locks),
                    read_locks: Some(read_locks),
                }) => write_locks.thread_set | read_locks.thread_set,
                Some(AccountLocks {
                    write_locks: None,
                    read_locks: Some(read_locks),
                }) => read_locks.thread_set,
                Some(AccountLocks {
                    write_locks: Some(write_locks),
                    read_locks: None,
                }) => write_locks.thread_set,
                Some(AccountLocks {
                    write_locks: None,
                    read_locks: None,
                }) => ThreadSet::none(),
            };
            tracing::debug!("Writable Account {:?}: \n BloomConflicts={:?} \n ActualConflicts={:?}", write_account.to_string(), bloom_set, conflicting_threads);
            for thread_id in 0..self.num_workers {
                if bloom_set.contains(thread_id) != conflicting_threads.contains(thread_id) {
                    self.false_positive_rate += 1.0;
                }
                self.total_bloom_checks += 1;
            }

            if !conflicting_threads.is_empty() && conflicting_threads.only_one_contained().is_none() {
                self.multiple_conflict_rate += 1.0;
            }
            self.total_accounts_checked += 1;
        }

        for read_account in tx_accounts.readonly.iter() {
            let index = hasher.hash_one(read_account);
            let read_filter_result = self.conflict_families[0].read_filter.search_u64(index);
            tracing::debug!("Readonly Account {:?} ---- Hash: {:?} ---- Query: {:?}", read_account.to_string(), index, read_filter_result);
            let mut bloom_set = ThreadSet::none();
            for (worker_index, cf) in self.conflict_families.iter().enumerate() {
                let raw_hazard = cf.write_filter.query_by_result(&read_filter_result);
                tracing::debug!("ConflictFamily {worker_index}: RAW({raw_hazard})");
                if raw_hazard == 0x1 {
                    bloom_set.insert(worker_index);
                }
            }
            let conflicting_threads = match mutex.locks.get(read_account) {
                None => ThreadSet::none(),
                Some(AccountLocks {
                    write_locks: Some(write_locks),
                    read_locks: _,
                }) => write_locks.thread_set,
                Some(AccountLocks {
                    write_locks: None,
                    read_locks: _,
                }) => ThreadSet::none(),
            };
            tracing::debug!("Readonly Account {:?}: \n BloomConflicts={:?} \n ActualConflicts={:?}", read_account.to_string(), bloom_set, conflicting_threads);
            for thread_id in 0..self.num_workers {
                if bloom_set.contains(thread_id) != conflicting_threads.contains(thread_id) {
                    self.false_positive_rate += 1.0;
                }
                self.total_bloom_checks += 1;
            }
            if !conflicting_threads.is_empty() && conflicting_threads.only_one_contained().is_none() {
                self.multiple_conflict_rate += 1.0;
            }
            self.total_accounts_checked += 1;
        }

        // locks the same accounts as bloom scheduler in order to check them in next rounds
        mutex.lock_accounts(&tx_accounts.writable, &tx_accounts.readonly, next_worker);
    }
}

impl Scheduler for BloomScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;

    fn add_thread_trackers(&mut self, execution_trackers: Vec<std::sync::Arc<ExecutionTracker>>) {
        self.thread_trackers = execution_trackers;
    }

    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>],
    ) -> Result<(), SchedulerError> {

        assert!((self.compute_false_positive_rate && self.account_locks.is_some()) || !self.compute_false_positive_rate);

        //set a number of retries to accumulate txs
        let mut num_retries = NUM_INGEST_RETRIES;
        let mut current_buffer_len = 0;
        let hasher  = RapidBuildHasher::new(420);
        let schedulable_threads = ThreadSet::any(self.num_workers);
        
        loop {
            //STAGE 1: Ingest txs from the TxIssuer
            //quickly check if there are new incoming txs
            match issue_channel.try_recv() {
                Ok(txs) => {
                    tracing::debug!("Received txs from TxIssuer");
                    for tx in txs.entry {
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

                            //compute false positive rate before exiting
                            if self.compute_false_positive_rate {
                                self.false_positive_rate = self.false_positive_rate
                                    / self.total_bloom_checks as f64
                                    * 100.0;
                                self.multiple_conflict_rate = self.multiple_conflict_rate
                                    / self.total_accounts_checked as f64
                                    * 100.0;
                                println!(
                                    "BloomScheduler: {{\n \"false_positive_rate\": {}, \n \"multiple_conflict_rate\": {}, \n}}",
                                    self.false_positive_rate, self.multiple_conflict_rate
                                );
                            }

                            return Err(SchedulerError::DisconnectedRecvChannel(String::from(
                                "TxIssuer Channel is closed",
                            )));
                        }
                    }
                }
            };

            if self.container.is_empty() {
                continue;
            }

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
            self.schedule_burst(hasher, schedulable_threads);
            num_retries = NUM_INGEST_RETRIES;

            //STAGE 3: send the current scheduled txs to the workers
            for worker_index in 0..self.num_workers {
                match self.work_lanes.remove_entry(&worker_index) {
                    Some(lane) => {
                        match execution_channels[lane.0].send(lane.1) {
                            Ok(_) => {}
                            Err(_) => {
                                //for the moment we stop the entire execution if we see that one worker is not responding anymore
                                //we don't have a safety mechanism to handle what happens when a worker gets disconnected

                                //compute false positive rate before exiting
                                if self.compute_false_positive_rate {
                                    self.false_positive_rate = self.false_positive_rate
                                    / self.total_bloom_checks as f64
                                    * 100.0;
                                    self.multiple_conflict_rate = self.multiple_conflict_rate
                                    / self.total_accounts_checked as f64
                                    * 100.0;
                                    println!(
                                        "BloomScheduler: {{\n \"false_positive_rate\": {}, \n \"multiple_conflict_rate\": {}, \n}}",
                                        self.false_positive_rate, self.multiple_conflict_rate
                                    );
                                }

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
