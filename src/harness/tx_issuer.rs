use crossbeam_channel::TrySendError;
use crossbeam_channel::{Receiver, Select, Sender};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use std::collections::VecDeque;
use tracing::info;
use crate::harness::scheduler::scheduler::Work;
use crate::harness::scheduler::scheduler::WorkEntry;
use crate::harness::tx_executor::FinishedWork;


#[derive(Debug)]
pub struct TxIssuer<Tx> {
    transactions: VecDeque<Tx>,
    completed_work_receiver: Vec<Receiver<FinishedWork<Tx>>>,
    work_sender: Sender<Work<Tx>>,
}

impl<Tx> TxIssuer<Tx> 
where Tx: TransactionWithMeta + Send + Sync + 'static {
    pub fn new(
        completed_work_receiver: Vec<Receiver<FinishedWork<Tx>>>,
        work_sender: Sender<Work<Tx>>,
        transactions: VecDeque<Tx>,
    ) -> Self {
        Self {
            transactions,
            completed_work_receiver,
            work_sender,
        }
    }

    pub fn run(mut self) -> std::thread::JoinHandle<()> {
        let handle = std::thread::spawn(move || {
            self.issue_txs();
        });
        //return handle
        handle
    }

    fn issue_txs(&mut self) {
        loop {
            //multiplex between channels and check first that sends something
            let mut recv_selector = Select::new();
            for r in self.completed_work_receiver.as_slice() {
                recv_selector.recv(r);
            }

            let selected_worker = recv_selector.try_select();
            match selected_worker {
                Err(_) => info!("No confirmation recived form any worker, moving on..."),
                Ok(operation) => {
                    let worker_index = operation.index();
                    info!("Received work from worker {:?}", worker_index);
                    //try non-blocking receive to see if there were any blocked txs
                    //but only receive one at a time so that we issue it to the scheduler immediately
                    match operation.recv(&self.completed_work_receiver[worker_index]) {
                        Ok(finished_work) => {
                            if finished_work.completed_entry.is_some() {
                                info!(
                                    "Received completed txs: {:?}",
                                    finished_work.completed_entry.unwrap()
                                );
                            }

                            if finished_work.failed_entry.is_some() {
                                info!(
                                    "Found failed txs..pushing them back to queue for rescheduling"
                                );
                                let failed_work = finished_work.failed_entry.unwrap();
                                match failed_work {
                                    WorkEntry::SingleTx(tx) => self.transactions.push_front(tx),
                                    WorkEntry::MultipleTxs(mut txs) => {
                                        while !txs.is_empty() {
                                            self.transactions.push_front(txs.pop().unwrap());
                                        }
                                    }
                                };
                            }
                        }
                        Err(_) => info!("No completed work received yet"),
                    };
                }
            }

            if self.transactions.is_empty() {
                //no more txs to issue
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
                    Ok(_) => info!("Successfully issued another tx to the scheduler"), //consume tx from queue
                    Err(e) => {
                        match e {
                            //regardless of dailer we should push back our txs
                            TrySendError::Full(tx) =>{
                                match tx.entry {
                                    WorkEntry::SingleTx(tx) => self.transactions.push_front(tx),
                                    WorkEntry::MultipleTxs(mut txs) => {
                                        while !txs.is_empty() {
                                            self.transactions.push_front(txs.pop().unwrap());
                                        }
                                    }
                                };
                                info!("Scheduler channel is full, trying again later...");
                                break;
                            },
                            TrySendError::Disconnected(tx) => {
                                match tx.entry {
                                    WorkEntry::SingleTx(tx) => self.transactions.push_front(tx),
                                    WorkEntry::MultipleTxs(mut txs) => {
                                        while !txs.is_empty() {
                                            self.transactions.push_front(txs.pop().unwrap());
                                        }
                                    }
                                };
                                info!("Scheduler channel got disconnected, ending issuer as well");
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}
