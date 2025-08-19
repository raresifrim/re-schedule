use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::RETRY_TXS;
use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::TOTAL_TXS;
use crate::harness::scheduler::scheduler::UNIQUE_TXS;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkEntry;
use ahash::{HashMap, HashMapExt};
use crossbeam_channel::{Receiver, Sender};
use solana_cost_model::cost_model::CostModel;
use solana_runtime::bank::Bank;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;
use std::sync::Arc;
use tracing::info;

pub struct RoundRobinScheduler {
    bank: Arc<Bank>,
    num_workers: usize,
    rr_distribution: HashMap<
        usize,
        (
            u64,
            Vec<HarnessTransaction<<RoundRobinScheduler as Scheduler>::Tx>>,
        ),
    >,
    cu_quant: u64,
    last_worker: usize,
    scheduling_summary: SchedulingSummary,
}

impl RoundRobinScheduler {
    pub fn new(num_workers: usize, cu_quant: u64, bank: Arc<Bank>) -> Self {
        let mut rr_distribution = HashMap::with_capacity(num_workers);
        for i in 0..num_workers {
            rr_distribution.insert(i, (cu_quant / num_workers as u64, vec![]));
        }
        let mut txs_per_worker = HashMap::with_capacity(num_workers);
        for i in 0..num_workers {
            txs_per_worker.insert(i, [0u64; 4]);
        }
        let scheduling_summary = SchedulingSummary {
            txs_per_worker,
            unique_txs: 0,
            total_txs: 0,
        };
        Self {
            num_workers,
            cu_quant,
            rr_distribution,
            bank,
            last_worker: 0,
            scheduling_summary,
        }
    }
}

impl Scheduler for RoundRobinScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;
    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>],
    ) -> Result<(), SchedulerError> {
        loop {
            match issue_channel.try_recv() {
                Ok(tx) => {
                    info!("Received txs from TxIssuer");
                    match tx.entry {
                        WorkEntry::SingleTx(tx) => {
                            let cost_of_tx =
                                CostModel::calculate_cost(&tx.transaction, &self.bank.feature_set)
                                    .sum();
                            let current_entry = self.rr_distribution.entry(self.last_worker);
                            current_entry.and_modify(|v| {
                                // TODO: Ask Alex how to make this clousure re-usable?
                                let txs_per_worker = self
                                    .scheduling_summary
                                    .txs_per_worker
                                    .get_mut(&self.last_worker)
                                    .unwrap();
                                if tx.retry {
                                    txs_per_worker[RETRY_TXS] += 1;
                                } else {
                                    txs_per_worker[UNIQUE_TXS] += 1;
                                    self.scheduling_summary.unique_txs += 1;
                                }
                                txs_per_worker[TOTAL_TXS] += 1;
                                self.scheduling_summary.total_txs += 1;
                                v.1.push(tx);
                                v.0 = v.0.saturating_sub(cost_of_tx);
                                if v.0 == 0 {
                                    self.last_worker = (self.last_worker + 1) % self.num_workers;
                                }
                            });
                        }
                        WorkEntry::MultipleTxs(txs) => {
                            //quite naive way of balancing multiple txs to the workers
                            //TODO: try to collect costs of multiple txs at once and allocate them to most available worker
                            for tx in txs {
                                let cost_of_tx = CostModel::calculate_cost(
                                    &tx.transaction,
                                    &self.bank.feature_set,
                                )
                                .sum();
                                let current_entry = self.rr_distribution.entry(self.last_worker);
                                current_entry.and_modify(|v| {
                                    let txs_per_worker = self
                                        .scheduling_summary
                                        .txs_per_worker
                                        .get_mut(&self.last_worker)
                                        .unwrap();
                                    if tx.retry {
                                        txs_per_worker[RETRY_TXS] += 1;
                                    } else {
                                        txs_per_worker[UNIQUE_TXS] += 1;
                                        self.scheduling_summary.unique_txs += 1;
                                    }
                                    txs_per_worker[TOTAL_TXS] += 1;
                                    self.scheduling_summary.total_txs += 1;
                                    v.1.push(tx);
                                    v.0 = v.0.saturating_sub(cost_of_tx);
                                    if v.0 == 0 {
                                        self.last_worker =
                                            (self.last_worker + 1) % self.num_workers;
                                    }
                                });
                            }
                        }
                    };

                    //now that we mapped the new txs to the available workers, try to send them if we reached the cu_quant
                    let mut num_scheduled = 0;

                    //get current workers that reached their quant
                    let extracted = self.rr_distribution.extract_if(|_k, v| v.0 == 0);

                    //and send their txs
                    let mut used_workers = vec![];
                    for (worker_index, (_cuant, txs)) in extracted {
                        let num_txs = txs.len();
                        //channel must be unbounded so only possible error is a Disconnected one
                        match execution_channels[worker_index].send(Work {
                            entry: WorkEntry::MultipleTxs(txs),
                        }) {
                            Ok(_) => {
                                num_scheduled += num_txs;
                                used_workers.push(worker_index);
                                info!("Scheduled {} txs to worker {}", num_scheduled, worker_index);
                            }
                            Err(_) => {
                                //for the moment we stop the entire execution if we see that one worker is not responding anymore
                                //we don't have a safety mechanism to handle what happens when a worker gets disconnected
                                //TODO: shrink rr_distribution hashmap and send failed tx to next available worker as emergency case
                                return Err(SchedulerError::DisconnectedSendChannel(format!(
                                    "TxExecutor Channel {} got disconnected",
                                    worker_index
                                )));
                            }
                        };
                    }

                    //reset entries for drained workers
                    for index in used_workers {
                        self.rr_distribution.insert(index, (self.cu_quant, vec![]));
                    }
                }

                //error might be actualy just empty or a real error like disconnected
                Err(e) => {
                    match e {
                        crossbeam_channel::TryRecvError::Empty => {
                            //if no more work is available, check if there is something to send right away to not waste execution time
                            let mut num_scheduled = 0;
                            for (worker_index, (_, txs)) in self.rr_distribution.drain() {
                                let num_txs = txs.len();
                                if num_txs == 0 {
                                    //nothing to send for this worker
                                    continue;
                                }

                                match execution_channels[worker_index].send(Work {
                                    entry: WorkEntry::MultipleTxs(txs),
                                }) {
                                    Ok(_) => {
                                        num_scheduled += num_txs;
                                        info!(
                                            "Schedued {} txs to worker {}",
                                            num_scheduled, worker_index
                                        );
                                    }
                                    Err(_) => {
                                        //for the moment we stop the entire execution if we see that one worker is not responding anymore
                                        //we don't have a safety mechanism to handle what happens when a worker gets disconnected
                                        //TODO: shrink rr_distribution hashmap and send failed tx to next available worker as emergency case
                                        return Err(SchedulerError::DisconnectedSendChannel(
                                            format!(
                                                "TxExecutor Channel {} got disconnected",
                                                worker_index
                                            ),
                                        ));
                                    }
                                };
                            }

                            //refill rr_distribution and reset last_worker
                            for index in 0..self.num_workers {
                                self.rr_distribution.insert(index, (self.cu_quant, vec![]));
                            }
                            self.last_worker = 0;
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
        }
    }

    fn get_summary(&self) -> SchedulingSummary {
        self.scheduling_summary.clone()
    }
}
