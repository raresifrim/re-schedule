use std::collections::VecDeque;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use tracing::info;
use crate::harness::scheduler;
use crate::harness::scheduler::bloom_scheduler::BloomScheduler;
use crate::harness::scheduler::tx_scheduler::{TxScheduler};
use crate::harness::tx_executor::TxExecutor;
use crate::{harness::tx_issuer::TxIssuer};
use crate::harness::scheduler::scheduler::{Scheduler};
use crate::utils::config::Config;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};


#[derive(Debug)]
pub struct SchedulerHarness<S, Tx>
{
    config: Config,
    tx_issuer: TxIssuer<Tx>,
    tx_scheduler: TxScheduler<S,Tx>,
    tx_executors: Vec<TxExecutor<Tx>>
}

impl<S, Tx> SchedulerHarness<S,Tx>  where 
Tx: TransactionWithMeta + Send + Sync + 'static,
S: Scheduler<Tx> + Send + Sync + 'static
{
    pub fn new_from_config(config: Config, scheduler: S) -> anyhow::Result<Self> {
        
        info!("Setting up directories and loading snapshots...");
        //let start_bank = load_bank_from_snapshot(&config.start_snapshot, &config.genesis).context("Failed to load start bank from snapshot")?;
        //let start_accounts_hash = start_bank.get_accounts_hash().context("Failed to get accounts hash")?;

        //create channels which will be used between scheduler, workers and issuer
        let (issuer_send_channel, scheduler_receiver_channel) = bounded(config.batch_size as usize * 2);
    
        let (schedule_to_execute_send_channels, execute_to_schedule_receive_channels): (Vec<Sender<_>>, Vec<Receiver<_>>) =
            (0..config.num_workers).map(|_| unbounded()).unzip();

        let (execute_to_issuer_send_channels, issuer_to_execute_receive_channels): (Vec<Sender<_>>, Vec<Receiver<_>>) =
            (0..config.num_workers).map(|_| unbounded()).unzip();

        let transactions = VecDeque::<Tx>::new();
        let tx_issuer = TxIssuer::new(issuer_to_execute_receive_channels, issuer_send_channel, transactions);
        
        let tx_scheduler = TxScheduler::new(scheduler, scheduler_receiver_channel, schedule_to_execute_send_channels);

        let mut tx_executors = vec![];
        for i in 0..config.num_workers {
            tx_executors.push(TxExecutor::new(
                execute_to_schedule_receive_channels[i as usize].clone(),
                execute_to_issuer_send_channels[i as usize].clone()
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
        let mut harness_hdls = vec![];
        harness_hdls.push(self.tx_issuer.run());
        harness_hdls.push(self.tx_scheduler.run());
        for ex in self.tx_executors {
            harness_hdls.push(ex.run());
        }

        for h in harness_hdls {
            h.join();
        }
    }
}

