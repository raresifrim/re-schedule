use crossbeam_channel::{Sender,Receiver};
use crate::harness::scheduler::scheduler::Work;
use crate::harness::executor::tx_executor::FinishedWork;
use crate::harness::issuer::tx_issuer::TxIssuerSummary;
use std::collections::VecDeque;
use crate::harness::scheduler::scheduler::HarnessTransaction;

pub trait Issuer<Tx> {
    fn issue_txs(
        &mut self,
        transactions: VecDeque<HarnessTransaction<Tx>>,
        work_sender: Sender<Work<Tx>>,
        completed_work_receiver: Vec<Receiver<FinishedWork<Tx>>>,
    ) -> TxIssuerSummary;
}