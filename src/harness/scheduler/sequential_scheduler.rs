use crate::harness::scheduler::scheduler::Scheduler;
use crate::harness::scheduler::scheduler::SchedulerError;
use crate::harness::scheduler::scheduler::SchedulingSummary;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkEntry;
use ahash::HashMap;
use ahash::HashMapExt;
use crossbeam_channel::{Receiver, Sender};
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;

pub struct SequentialScheduler {
    scheduling_summary: SchedulingSummary,
}

impl Scheduler for SequentialScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;

    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>],
    ) -> Result<(), SchedulerError> {
        //this implements a dummy sequential scheduler for a single worker
        let worker_id = 0;
        let txs_per_worker = self
            .scheduling_summary
            .txs_per_worker
            .get_mut(&worker_id)
            .unwrap();

        loop {
            match issue_channel.recv() {
                Ok(tx) => {
                    if let WorkEntry::SingleTx(tx) = tx.entry {
                        if tx.retry {
                            txs_per_worker.retried += 1;
                        } else {
                            txs_per_worker.unique += 1;
                            self.scheduling_summary.unique_txs += 1;
                        }
                        txs_per_worker.total += 1;
                        self.scheduling_summary.total_txs += 1;
                        if let Err(e) = execution_channels[worker_id].send(Work {
                            entry: WorkEntry::SingleTx(tx),
                        }) {
                            return Err(SchedulerError::DisconnectedSendChannel(format!(
                                "{:?}",
                                e
                            )));
                        };
                    };
                }
                Err(e) => return Err(SchedulerError::DisconnectedRecvChannel(format!("{:?}", e))),
            }
        }
    }

    /// retrieve scheduling summary
    fn get_summary(&self) -> SchedulingSummary {
        self.scheduling_summary.clone()
    }
}

impl SequentialScheduler {
    pub fn new() -> Self {
        let mut txs_per_worker = HashMap::with_capacity(1);
        txs_per_worker.insert(0, Default::default());
        let scheduling_summary = SchedulingSummary {
            txs_per_worker,
            unique_txs: 0,
            total_txs: 0,
        };
        Self { scheduling_summary }
    }
}
