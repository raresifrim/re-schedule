use std::collections::VecDeque;
use crate::harness::scheduler::tx_scheduler::{TxScheduler};
use crate::harness::tx_executor::TxExecutor;
use crate::{harness::tx_issuer::TxIssuer};
use crate::harness::scheduler::scheduler::{HarnessTransaction, Scheduler};
use crate::utils::config::Config;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use solana_svm::account_overrides::AccountOverrides;
use std::sync::Arc;
use solana_runtime::bank::Bank;

pub struct SchedulerHarness<S> where S:Scheduler
{
    config: Config,
    tx_issuer: TxIssuer<S::Tx>,
    tx_scheduler: TxScheduler<S>,
    tx_executors: Vec<TxExecutor<S::Tx>>
}

impl<S> SchedulerHarness<S>  where 
S: Scheduler + Send + Sync + 'static
{
    pub fn new_from_config(config: Config, scheduler: S, transactions: VecDeque<HarnessTransaction<S::Tx>>, bank:Arc<Bank>) -> anyhow::Result<Self> {
        
        //create channels which will be used between scheduler, workers and issuer
        let (issuer_send_channel, scheduler_receiver_channel) = bounded(config.batch_size as usize * 2);
    
        let (schedule_to_execute_send_channels, execute_to_schedule_receive_channels): (Vec<Sender<_>>, Vec<Receiver<_>>) =
            (0..config.num_workers).map(|_| unbounded()).unzip();

        let (execute_to_issuer_send_channels, issuer_to_execute_receive_channels): (Vec<Sender<_>>, Vec<Receiver<_>>) =
            (0..config.num_workers).map(|_| unbounded()).unzip();

        let tx_issuer = TxIssuer::new(issuer_to_execute_receive_channels, issuer_send_channel, transactions);
        
        let tx_scheduler = TxScheduler::new(scheduler, scheduler_receiver_channel, schedule_to_execute_send_channels);

        let mut tx_executors = vec![];
        for i in 0..config.num_workers {
            tx_executors.push(TxExecutor::new(
                execute_to_schedule_receive_channels[i as usize].clone(),
                execute_to_issuer_send_channels[i as usize].clone(),
                bank.clone()
            ));
        }

        Ok(Self{
            config,
            tx_issuer,
            tx_scheduler,
            tx_executors
        })
    }


    pub fn run(self) {
        //TODO: Need to compute execution time and return it
        let mut harness_hdls = vec![];
        harness_hdls.push(self.tx_issuer.run());
        harness_hdls.push(self.tx_scheduler.run());
        for ex in self.tx_executors {
            harness_hdls.push(ex.run());
        }

        for h in harness_hdls {
            h.join().unwrap();
        }
    }
}

