use solana_runtime::bank::Bank;
use solana_accounts_db::accounts_hash::AccountsHash;
use serde::{Serialize,Deserialize};
use tracing::info;
use crate::utils::config::SchedulerType;
use crate::utils::snapshot::load_bank_from_snapshot;
use crate::harness::scheduler::scheduler_controller::SchedulerController;
use crate::harness::consume_worker::ConsumeWorker;
use std::sync::Arc;
use crate::utils::config::Config;
use solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_with_meta::TransactionWithMeta,
};
use crate::harness::scheduler::scheduler::Scheduler;
use solana_sdk::transaction::SanitizedTransaction;
use crate::harness::scheduler::transaction_container::Container;
use crate::harness::read_and_buffer::TransactionBuffer;
use crate::harness::scheduler::greedy_scheduler::{GreedyScheduler,GreedySchedulerConfig};
use crossbeam_channel::{unbounded, Receiver, Sender};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SchedulerHarness<C,S,T> where
    C: Container<RuntimeTransaction<SanitizedTransaction>>,
    S: Scheduler<RuntimeTransaction<SanitizedTransaction>>,
    T: TransactionWithMeta 
{
    bank: Arc<Bank>,
    accounts_hash: AccountsHash,
    tx_reader: TransactionBuffer,
    scheduler: SchedulerController<C,S>,
    workers: Vec<ConsumeWorker<T>>
    bank_thread_hdls: Vec<JoinHandle<()>>
}

impl<C,S,T> SchedulerHarness<C,S,T> where
    C: Container<RuntimeTransaction<SanitizedTransaction>>,
    S: Scheduler<RuntimeTransaction<SanitizedTransaction>>,
    T: TransactionWithMeta 
{
    pub fn new_from_config(config: Config) -> anyhow::Result<Self> {
        
        info!("Setting up directories and loading snapshots...");
        let start_bank = load_bank_from_snapshot(&config.start_snapshot, &config.genesis).context("Failed to load start bank from snapshot")?;
        let start_accounts_hash = start_bank.get_accounts_hash().context("Failed to get accounts hash")?;

        //create channels which will be used between scheduler and workers
        let (consume_work_senders, consume_work_receivers) =
            (0..config.num_workers).map(|_| unbounded()).unzip();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();

        //Create the scheduller controller which can instantiate different types of scheduling strategies
        let scheduler_controller = match config.scheduler_type {
            SchedulerType::Greedy => {
                let scheduler = GreedyScheduler::new(
                    consume_work_senders,
                    finished_consume_work_receiver,
                    GreedySchedulerConfig::default(),
                );
                SchedulerController::new(start_bank,scheduler)
            },
            SchedulerType::PrioGraph => {
                let scheduler = GreedyScheduler::new(
                    consume_work_senders,
                    finished_consume_work_receiver,
                    GreedySchedulerConfig::default(),
                );
                SchedulerController::new(start_bank,scheduler)
            },
            SchedulerType::Bloom => {
                let scheduler = GreedyScheduler::new(
                    consume_work_senders,
                    finished_consume_work_receiver,
                    GreedySchedulerConfig::default(),
                );
                SchedulerController::new(start_bank,scheduler)
            }
        };

        //Create Worker Objects
        let workers = vec![];
        for (index, work_receiver) in consume_work_receivers.into_iter().enumerate() {
            let consume_worker = ConsumeWorker::new(
                id,
                work_receiver,
                Consumer::new(
                    committer.clone(),
                    transaction_recorder.clone(),
                    QosService::new(id),
                    log_messages_bytes_limit,
                ),
                finished_work_sender.clone(),
                poh_recorder.read().unwrap().new_leader_bank_notifier(),
            );
            workers.push(consume_workers);
        }

        Self{
            bank:start_bank,
            accounts_hash: start_accounts_hash,
            scheduler: scheduler_controller,
            bank_thread_hdls:vec![JoinHandle<()>; config.num_workers + 1],
            workers
        }
    }


    pub fn run(&mut self) {
        
        self.bank_thread_hdls.push(
            std::thread::spawn(move || {
                match self.scheduler_controller.run() {
                    Ok(_) => {info!("Scheduler Worker finished!")},
                    Err() => {info!("Scheduler Worker ended unexpected!")}
                }
            })
        );
        
        self.bank_thread_hdls.push(
            std::thread::spawn(move || {
                let _ = consume_worker.run();
            })
        );
    }
}