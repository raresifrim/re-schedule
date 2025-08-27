use crate::harness::executor::tx_executor::FinishedWork;
use crate::harness::scheduler::scheduler::{HarnessTransaction, Work};
use crate::harness::issuer::issuer::Issuer;
use crossbeam_channel::{Receiver, Sender};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;


use std::collections::VecDeque;

pub struct TxIssuer<I, Tx>
where
    I: Issuer<Tx> + Send + Sync + 'static,
    Tx: TransactionWithMeta + Send + Sync + 'static
{
    transactions: VecDeque<HarnessTransaction<Tx>>,
    completed_work_receiver: Vec<Receiver<FinishedWork<Tx>>>,
    work_sender: Sender<Work<Tx>>,
    pub issuer: I,
}

#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct TxIssuerSummary {
    /// number of txs received at the beginning of the reschedule operation
    pub num_initial_txs: usize,
    /// total number of txs that were completely executed, either directly or via retry
    pub num_txs_executed: usize,
    /// number of txs that were retried because of error
    pub num_txs_retried: usize,
    /// total execution time measured from the tx issuer perspective
    pub total_exec_time: f64,
    /// throughput as unique txs executed over execution time as txs/s
    pub useful_tx_throughput: f64,
    /// throughput as total amount txs executed over execution time as txs/s
    pub raw_tx_throughput: f64,
}

impl<I, Tx> TxIssuer<I, Tx>
where
    I: Issuer<Tx> + Send + Sync + 'static,
    Tx: TransactionWithMeta + Send + Sync + 'static
{
    pub fn new(
        issuer: I,
        completed_work_receiver: Vec<Receiver<FinishedWork<Tx>>>,
        work_sender: Sender<Work<Tx>>,
        transactions: VecDeque<HarnessTransaction<Tx>>,
    ) -> Self {
        Self {
            issuer,
            transactions,
            completed_work_receiver,
            work_sender,
        }
    }

    /// if unlock_accounts==true then ThreadAwareAccountLocks is used to unlock the accounts locked by the scheduler
    /// unlock_accounts set to false can be used for testing/collecting such as the bloom scheduler to compute the false positive rate
    pub fn run(
        mut self,
    ) -> std::thread::JoinHandle<TxIssuerSummary> {
        //return handle
        std::thread::spawn(move || self.issuer.issue_txs(self.transactions, self.work_sender, self.completed_work_receiver))
    }

}
