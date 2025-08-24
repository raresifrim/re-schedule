use crate::harness::executor::execution_tracker::ExecutionTracker;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkerId;
use crate::harness::scheduler::thread_aware_account_locks::ThreadSet;
use crate::harness::scheduler::thread_aware_account_locks::select_thread;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use ahash::{HashMap, HashMapExt};
use crossbeam_channel::{Receiver, Sender};
use solana_runtime::bank::Bank;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::info;


pub struct RoundRobinScheduler {
    bank: Arc<Bank>,
    num_workers: usize,
    work_lanes: HashMap<WorkerId, Work<<RoundRobinScheduler as Scheduler>::Tx>>,
    target_cus: u64,
    target_txs: usize,
    scheduling_summary: SchedulingSummary,
    thread_trackers: Vec<Arc<ExecutionTracker>>,
}

impl RoundRobinScheduler {
    pub fn new(num_workers: usize, target_cus: u64, target_txs: usize, bank: Arc<Bank>) -> Self {
        let mut txs_per_worker = HashMap::with_capacity(num_workers);
        for i in 0..num_workers {
            txs_per_worker.insert(i, super::scheduler::WorkerSummary::default());
        }

        let scheduling_summary = SchedulingSummary {
            txs_per_worker,
            useful_txs: 0,
            total_txs: 0,
        };

        let work_lanes = HashMap::with_capacity(num_workers);

        Self {
            num_workers,
            target_cus,
            target_txs,
            work_lanes,
            bank,
            scheduling_summary,
            //work trackers should be added through a separate function call defined by the Scheduler trait
            thread_trackers: vec![],
        }
    }
}

impl Scheduler for RoundRobinScheduler {
    fn add_thread_trackers(&mut self, execution_trackers: Vec<Arc<ExecutionTracker>>) {
        self.thread_trackers = execution_trackers;
    }

    type Tx = RuntimeTransaction<SanitizedTransaction>;
    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>],
        _account_locks: Arc<Mutex<ThreadAwareAccountLocks>>
    ) -> Result<(), SchedulerError> {
        
        let schedulable_threads = ThreadSet::any(self.num_workers);
        loop {
            let mut can_send = false;

            match issue_channel.try_recv() {
                Ok(work) => {
                    tracing::debug!("Received txs from TxIssuer");

                    for harness_tx in work.entry {
                        
                        let thread_id = select_thread::<Self>(
                            schedulable_threads,
                            &self.thread_trackers,
                            &self.work_lanes,
                            1,
                            harness_tx.cu_cost,
                        );

                        let retry = harness_tx.retry;
                        let cu_cost = harness_tx.cu_cost;
                        let mut v = vec![harness_tx];

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
                    }

                    for work_lane in &self.work_lanes {
                        let total_cus = work_lane.1.total_cus;
                        let workload = work_lane.1.entry.len();

                        // If target batch size or target cus are reached, send all the batches
                        if workload >= self.target_txs / self.num_workers
                            || total_cus >= self.target_cus / self.num_workers as u64
                        {
                            can_send = true;
                            break;
                        }
                    }
                }

                //error might be actualy just empty or a real error like disconnected
                Err(e) => {
                    match e {
                        crossbeam_channel::TryRecvError::Empty => {
                            //if no more work is available, check if there is something to send right away to not waste execution time
                            can_send = true;
                        }
                        crossbeam_channel::TryRecvError::Disconnected => {
                            //if disconnected we exit as there is no tx issuer to give us work
                            return Err(SchedulerError::DisconnectedRecvChannel(String::from(
                                "TxIssuer Channel is closed",
                            )));
                        }
                    }
                }
            }

            if can_send {
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
    }

    fn get_summary(&self) -> SchedulingSummary {
        self.scheduling_summary.clone()
    }
}
