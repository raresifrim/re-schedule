use std::collections::VecDeque;
use crate::harness::scheduler::tx_scheduler::{TxScheduler};
use crate::harness::tx_executor::TxExecutor;
use crate::{harness::tx_issuer::TxIssuer};
use crate::harness::scheduler::scheduler::{HarnessTransaction, Scheduler, TOTAL_TXS, UNIQUE_TXS, RETRY_TXS, SATURATION};
use crate::utils::config::Config;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use std::sync::Arc;
use solana_runtime::bank::Bank;

pub struct SchedulerHarness<S> where
S: Scheduler + Send + Sync + 'static
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
                bank.clone(),
                config.simulate
            ));
        }

        Ok(Self{
            config,
            tx_issuer,
            tx_scheduler,
            tx_executors
        })
    }


    pub fn run(self)  {
        let mut harness_hdls = vec![];
        //the issuer will return the overall summary of the executiom
        let issuer_handle = self.tx_issuer.run();
        let scheduler_handle = self.tx_scheduler.run();
        for ex in self.tx_executors {
            harness_hdls.push(ex.run());
        }

        for h in harness_hdls {
            h.join().unwrap();
        }

        let issuer_summary = issuer_handle.join().unwrap();
        let scheduling_summary = scheduler_handle.join().unwrap();

        println!("\n-------- Transaction Issuing Results -------");
        println!("{:#?}", issuer_summary);
        println!("-------------------------------------------\n");
        
        println!("\n------ Transaction Scheduling Results ------");
        for i in 0..(self.config.num_workers as usize){
            println!("------------- Worker {i} Summary -------------");
            let worker_summary = scheduling_summary.txs_per_worker.get(&i).unwrap();
            println!("Total unique txs executed: {}", worker_summary[UNIQUE_TXS]);
            println!("Total txs executed: {} of which retried: {}", worker_summary[TOTAL_TXS], worker_summary[RETRY_TXS]);
            println!("Worker saturation: {}%", worker_summary[SATURATION]);
        }
        println!("-------------------------------------------\n");
        
        println!("\n------- Transaction Execution Results ------");
        println!("-------------------------------------------\n");
    }
}

