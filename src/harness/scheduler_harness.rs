use crate::harness::scheduler::scheduler::{HarnessTransaction, Scheduler};
use crate::harness::scheduler::tx_scheduler::TxScheduler;
use crate::harness::tx_executor::TxExecutorSummary;
use crate::harness::tx_executor::{TxExecutor, support::SharedAccountLocks};
use crate::harness::tx_issuer::TxIssuer;
use crate::utils::config::Config;
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use solana_runtime::bank::Bank;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::scheduler::scheduler::SchedulingSummary;
use super::tx_executor::support::LockSummary;
use super::tx_issuer::TxIssuerSummary;

pub struct SchedulerHarness<S>
where
    S: Scheduler + Send + Sync + 'static,
{
    config: Config,
    tx_issuer: TxIssuer<S::Tx>,
    tx_scheduler: TxScheduler<S>,
    tx_executors: Vec<TxExecutor<S::Tx>>,
}

impl<S> SchedulerHarness<S>
where
    S: Scheduler + Send + Sync + 'static,
{
    pub fn new_from_config(
        config: Config,
        scheduler: S,
        transactions: VecDeque<HarnessTransaction<S::Tx>>,
        bank: Arc<Bank>,
    ) -> anyhow::Result<Self> {
        //create channels which will be used between scheduler, workers and issuer
        let (issuer_send_channel, scheduler_receiver_channel) =
            bounded(config.batch_size as usize * 2);

        let (schedule_to_execute_send_channels, execute_to_schedule_receive_channels): (
            Vec<Sender<_>>,
            Vec<Receiver<_>>,
        ) = (0..config.num_workers).map(|_| unbounded()).unzip();

        let (execute_to_issuer_send_channels, issuer_to_execute_receive_channels): (
            Vec<Sender<_>>,
            Vec<Receiver<_>>,
        ) = (0..config.num_workers).map(|_| unbounded()).unzip();

        let tx_issuer = TxIssuer::new(
            issuer_to_execute_receive_channels,
            issuer_send_channel,
            transactions,
        );

        let tx_scheduler = TxScheduler {
            scheduler,
            work_issuer: scheduler_receiver_channel,
            work_executors: schedule_to_execute_send_channels,
        };

        let mut tx_executors = vec![];
        for i in 0..config.num_workers {
            tx_executors.push(TxExecutor::new(
                i as usize,
                execute_to_schedule_receive_channels[i as usize].clone(),
                execute_to_issuer_send_channels[i as usize].clone(),
                bank.clone(),
                config.simulate,
            ));
        }

        Ok(Self {
            config,
            tx_issuer,
            tx_scheduler,
            tx_executors,
        })
    }

    pub fn run(self) -> RunSummary {
        let mut harness_hdls = vec![];
        //the issuer will return the overall summary of the executiom
        let issuer_handle = self.tx_issuer.run();
        let scheduler_handle = self.tx_scheduler.run();

        let account_locks = Arc::new(Mutex::new(SharedAccountLocks::new()));
        for ex in self.tx_executors {
            if self.config.simulate {
                harness_hdls.push(ex.run(Some(Arc::clone(&account_locks))));
            } else {
                harness_hdls.push(ex.run(None));
            }
        }

        let mut executor_summaries = vec![];
        for h in harness_hdls {
            executor_summaries.push(h.join().unwrap());
        }

        let issuer_summary = issuer_handle.join().unwrap();
        let scheduling_summary = scheduler_handle.join().unwrap();
        let (read_locks, write_locks) = {
            let mutex = account_locks.lock().unwrap();
            (
                mutex.get_top_read_locks(self.config.num_report_locks),
                mutex.get_top_write_locks(self.config.num_report_locks),
            )
        };

        RunSummary {
            scheduling: scheduling_summary,
            issuer: issuer_summary,
            executors: executor_summaries,
            read_locks,
            write_locks,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[must_use]
pub struct RunSummary {
    scheduling: SchedulingSummary,
    issuer: TxIssuerSummary,
    pub executors: Vec<TxExecutorSummary>,
    read_locks: LockSummary,
    write_locks: LockSummary,
}
