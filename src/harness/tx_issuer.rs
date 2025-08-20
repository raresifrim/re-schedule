use crate::harness::scheduler::scheduler::WorkEntry;
use crate::harness::scheduler::scheduler::{HarnessTransaction, Work};
use crate::harness::tx_executor::FinishedWork;
use crossbeam_channel::TrySendError;
use crossbeam_channel::{Receiver, Select, Sender};
use std::collections::VecDeque;
use std::time::Instant;
use tracing::info;

pub struct TxIssuer<Tx> {
    transactions: VecDeque<HarnessTransaction<Tx>>,
    completed_work_receiver: Vec<Receiver<FinishedWork<Tx>>>,
    work_sender: Sender<Work<Tx>>,
    summary: TxIssuerSummary,
}

#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct TxIssuerSummary {
    /// number of txs received at the beginning of the reschedule operation
    num_initial_txs: usize,
    /// total number of txs that were completely executed, either directly or via retry
    num_txs_executed: usize,
    /// number of txs that were retried because of error
    num_txs_retried: usize,
    /// total execution time measured from the tx issuer perspective
    total_exec_time: f64,
    /// throughput as unique txs executed over execution time as txs/s
    useful_tx_throughput: f64,
    /// throughput as total amount txs executed over execution time as txs/s
    raw_tx_throughput: f64,
}

impl<Tx> TxIssuer<Tx>
where
    Tx: Send + Sync + 'static,
{
    pub fn new(
        completed_work_receiver: Vec<Receiver<FinishedWork<Tx>>>,
        work_sender: Sender<Work<Tx>>,
        transactions: VecDeque<HarnessTransaction<Tx>>,
    ) -> Self {
        let num_initial_txs = transactions.len();

        Self {
            transactions,
            completed_work_receiver,
            work_sender,
            summary: TxIssuerSummary {
                num_initial_txs,
                ..Default::default()
            },
        }
    }

    pub fn run(mut self) -> std::thread::JoinHandle<TxIssuerSummary> {
        
        //return handle
        std::thread::spawn(move || self.issue_txs())
    }

    fn issue_txs(&mut self) -> TxIssuerSummary {
        //issuer will stop once it gets all transactions executed
        let mut num_txs = self.transactions.len();

        let start_time = Instant::now();

        loop {
            //multiplex between channels and check first that sends something
            let mut recv_selector = Select::new();
            for r in self.completed_work_receiver.as_slice() {
                recv_selector.recv(r);
            }

            let selected_worker = recv_selector.try_select();
            match selected_worker {
                Err(_) => { /*No confirmation recived form any worker, moving on...*/ }
                Ok(operation) => {
                    let worker_index = operation.index();
                    //info!("Received work from worker {:?}", worker_index);
                    //try non-blocking receive to see if there were any blocked txs
                    //but only receive one at a time so that we issue it to the scheduler immediately
                    match operation.recv(&self.completed_work_receiver[worker_index]) {
                        Ok(finished_work) => {
                            if finished_work.completed_entry.is_some() {
                                match finished_work.completed_entry.unwrap() {
                                    WorkEntry::SingleTx(_) => {
                                        num_txs -= 1;
                                        self.summary.num_txs_executed += 1;
                                    }
                                    WorkEntry::MultipleTxs(txs) => {
                                        num_txs -= txs.len();
                                        self.summary.num_txs_executed += txs.len();
                                    }
                                };
                                tracing::debug!(
                                    "Successfully executed {}% of txs",
                                    (self.summary.num_initial_txs - num_txs) * 100
                                    / self.summary.num_initial_txs
                                );
                            }

                            if finished_work.failed_entry.is_some() {
                                tracing::debug!(
                                    "Found failed txs..pushing them back to queue for rescheduling"
                                );
                                let failed_work = finished_work.failed_entry.unwrap();
                                match failed_work {
                                    WorkEntry::SingleTx(mut tx) => {
                                        tx.retry = true;
                                        self.transactions.push_back(tx);
                                        self.summary.num_txs_retried += 1;
                                    }
                                    WorkEntry::MultipleTxs(mut txs) => {
                                        self.summary.num_txs_retried += txs.len();
                                        while let Some(mut tx) = txs.pop() {
                                            
                                            tx.retry = true;
                                            self.transactions.push_back(tx);
                                        }
                                    }
                                };
                            }
                        }
                        Err(_) => { /*"No completed work received yet"*/ }
                    };
                }
            }

            if num_txs == 0 {
                //we received all txs back so we can exit
                break;
            }

            if self.transactions.is_empty() {
                //no more txs to issue, so just wait for workers to send results back
                continue;
            }

            //send available txs
            //channel must bounded, so that once filled, the Tx issuer is free to do other work
            while !self.work_sender.is_full() {
                //get front tx but do not consume it as we might not be able to send it
                let maybe_tx = self.transactions.pop_front();
                if maybe_tx.is_none() {
                    //nothing to send
                    //maybe we will receive something from the workers
                    break;
                }
                let tx = maybe_tx.unwrap();
                match self.work_sender.try_send(Work {
                    entry: WorkEntry::SingleTx(tx),
                }) {
                    Ok(_) => tracing::debug!("Successfully issued another tx to the scheduler"),
                    Err(e) => {
                        match e {
                            //when full, we should push back our txs
                            TrySendError::Full(tx) => {
                                match tx.entry {
                                    WorkEntry::SingleTx(tx) => self.transactions.push_front(tx),
                                    WorkEntry::MultipleTxs(mut txs) => {
                                        while let Some(element) = txs.pop() {
                                            self.transactions.push_front(element);
                                        }
                                    }
                                };
                                info!("Scheduler channel is full, trying again later...");
                                break;
                            }
                            TrySendError::Disconnected(tx) => {
                                match tx.entry {
                                    WorkEntry::SingleTx(tx) => self.transactions.push_front(tx),
                                    WorkEntry::MultipleTxs(mut txs) => {
                                        while let Some(element) = txs.pop() {
                                            self.transactions.push_front(element);
                                        }
                                    }
                                };
                                info!("Scheduler channel got disconnected, ending issuer as well");
                                break;
                            }
                        }
                    }
                }
            }
        }

        let end_time = start_time.elapsed().as_secs_f64();
        self.summary.total_exec_time = end_time;
        self.summary.num_txs_executed += self.summary.num_txs_retried;
        self.summary.useful_tx_throughput = self.summary.num_initial_txs as f64 / end_time;
        self.summary.raw_tx_throughput = self.summary.num_txs_executed as f64 / end_time;
        self.summary.clone()
    }
}
