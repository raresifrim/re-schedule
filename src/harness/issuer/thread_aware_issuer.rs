use std::sync::Arc;
use std::sync::Mutex;
use crate::harness::issuer::tx_issuer::TxIssuerSummary;
use crate::harness::issuer::issuer::Issuer;
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use std::collections::VecDeque;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use std::time::Instant;
use crossbeam_channel::Select;
use itertools::Itertools;
use crate::harness::scheduler::scheduler::Work;
use crossbeam_channel::TrySendError;
use tracing::info;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;

pub struct ThreadAwareIssuer {
    account_locks: Arc<Mutex<ThreadAwareAccountLocks>>,
    unlock_accounts: bool,
    summary: TxIssuerSummary,
}

impl ThreadAwareIssuer {
    pub fn new(account_locks: Arc<Mutex<ThreadAwareAccountLocks>>, unlock_accounts: bool,) -> Self {
        Self{
            account_locks,
            unlock_accounts,
            summary: TxIssuerSummary {
                ..Default::default()
            },
        }
    }
}

impl<Tx> Issuer<Tx> for ThreadAwareIssuer 
where Tx: TransactionWithMeta + Send + Sync + 'static
{
    fn issue_txs(
            &mut self,
            mut transactions: VecDeque<HarnessTransaction<Tx>>,
            work_sender: crossbeam_channel::Sender<crate::harness::scheduler::scheduler::Work<Tx>>,
            completed_work_receiver: Vec<crossbeam_channel::Receiver<crate::harness::executor::tx_executor::FinishedWork<Tx>>>,
        ) -> super::tx_issuer::TxIssuerSummary {
        //issuer will stop once it gets all transactions executed
        let mut num_txs = transactions.len();
        self.summary.num_initial_txs = num_txs;
        let start_time = Instant::now();

        'main: loop {
            //multiplex between channels and check first that sends something
            let mut recv_selector = Select::new();
            for r in completed_work_receiver.as_slice() {
                recv_selector.recv(r);
            }

            let selected_worker = recv_selector.try_select();
            match selected_worker {
                Err(_) => { /*No confirmation recived form any worker, moving on...*/ }
                Ok(operation) => {
                    let worker_index = operation.index();
                    tracing::debug!("Received work from worker {:?}", worker_index);

                    //lock the mutex now and unlock accounts for all txs
                    let mut mutex = self.account_locks.lock().unwrap();

                    //try non-blocking receive to see if there were any blocked txs
                    //but only receive one at a time so that we issue it to the scheduler immediately
                    match operation.recv(&completed_work_receiver[worker_index]) {
                        Ok(finished_work) => {
                            if finished_work.completed_entry.is_some() {
                                let completed_work = finished_work.completed_entry.unwrap();
                                num_txs -= completed_work.len();
                                self.summary.num_txs_executed += completed_work.len();

                                if self.unlock_accounts {
                                    for harness_tx in completed_work {
                                        let account_keys = harness_tx.transaction.account_keys();

                                        let write_account_locks = account_keys
                                            .iter()
                                            .enumerate()
                                            .filter_map(|(index, key)| {
                                                harness_tx
                                                    .transaction
                                                    .is_writable(index)
                                                    .then_some(key)
                                            })
                                            .collect_vec();

                                        let read_account_locks = account_keys
                                            .iter()
                                            .enumerate()
                                            .filter_map(|(index, key)| {
                                                (!harness_tx.transaction.is_writable(index))
                                                    .then_some(key)
                                            })
                                            .collect_vec();

                                        mutex.unlock_accounts(
                                            &write_account_locks,
                                            &read_account_locks,
                                            worker_index,
                                        );
                                    }
                                }

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
                                let mut failed_work = finished_work.failed_entry.unwrap();
                                self.summary.num_txs_retried += failed_work.len();
                                while let Some(mut harness_tx) = failed_work.pop() {
                                    harness_tx.retry = true;

                                    if self.unlock_accounts {
                                        let account_keys = harness_tx.transaction.account_keys();

                                        let write_account_locks = account_keys
                                            .iter()
                                            .enumerate()
                                            .filter_map(|(index, key)| {
                                                harness_tx
                                                    .transaction
                                                    .is_writable(index)
                                                    .then_some(key)
                                            })
                                            .collect_vec();

                                        let read_account_locks = account_keys
                                            .iter()
                                            .enumerate()
                                            .filter_map(|(index, key)| {
                                                (!harness_tx.transaction.is_writable(index))
                                                    .then_some(key)
                                            })
                                            .collect_vec();

                                        mutex.unlock_accounts(
                                            &write_account_locks,
                                            &read_account_locks,
                                            worker_index,
                                        );
                                    }

                                    transactions.push_back(harness_tx);
                                }
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

            if transactions.is_empty() {
                //no more txs to issue, so just wait for workers to send results back
                continue;
            }

            //send available txs
            //channel must bounded, so that once filled, the Tx issuer is free to do other work
            while !work_sender.is_full() {
                //get front tx but do not consume it as we might not be able to send it
                let maybe_tx = transactions.pop_front();
                if maybe_tx.is_none() {
                    //nothing to send
                    //maybe we will receive something from the workers
                    break;
                }
                let tx = maybe_tx.unwrap();
                match work_sender.try_send(Work {
                    total_cus: tx.cu_cost,
                    entry: vec![tx],
                }) {
                    Ok(_) => tracing::debug!("Successfully issued another tx to the scheduler"),
                    Err(e) => {
                        match e {
                            //when full, we should push back our txs
                            TrySendError::Full(mut txs) => {
                                while let Some(element) = txs.entry.pop() {
                                    transactions.push_front(element);
                                }
                                info!("Scheduler channel is full, trying again later...");
                                break;
                            }
                            TrySendError::Disconnected(_) => {
                                info!("Scheduler channel got disconnected, ending issuer as well");
                                break 'main;
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