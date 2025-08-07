use crossbeam_channel::{Receiver,Sender};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_runtime::bank::LoadAndExecuteTransactionsOutput;
use solana_svm::transaction_processing_result::ProcessedTransaction;
use tracing::info;
use crate::harness::scheduler::scheduler::WorkEntry;
use crate::harness::scheduler::scheduler::Work;
use solana_runtime::bank::Bank;
use std::sync::Arc;
use solana_runtime::transaction_batch::TransactionBatch;
use solana_runtime::transaction_batch::OwnedOrBorrowed;
use solana_sdk::transaction::*;
use solana_svm::transaction_error_metrics::TransactionErrorMetrics;
use solana_svm::transaction_processor::TransactionProcessingConfig;
use solana_svm::transaction_processor::ExecutionRecordingConfig;
use solana_timings::ExecuteTimings;
use solana_svm::transaction_processing_result::TransactionProcessingResultExtensions;

/// Message: [Worker -> Issuer]
/// Processed transactions.
pub struct FinishedWork<Tx> {
    pub completed_entry: Option<WorkEntry<Tx>>,
    pub failed_entry: Option<WorkEntry<Tx>>,
}

#[derive(Debug)]
pub struct TxExecutor<Tx> {
    work_receiver: Receiver<Work<Tx>>,
    completed_work_sender: Sender<FinishedWork<Tx>>,
    bank: Arc<Bank>
}

impl<Tx> TxExecutor<Tx>
where Tx: TransactionWithMeta + Send + Sync + 'static {
    pub fn new(
        work_receiver: Receiver<Work<Tx>>,
        completed_work_sender: Sender<FinishedWork<Tx>>,
        bank: Arc<Bank>,
    ) -> Self {
        Self {work_receiver, completed_work_sender, bank}
    }

    pub fn run(self) -> std::thread::JoinHandle<()>{
        let handle = std::thread::spawn(move || {
            info!("Startng worker thread");
            self.execute_txs();
        });
        //return handle
        handle
    }
    fn execute_txs(&self) {
        while let Ok(work) = self.work_receiver.recv() {
            info!("Received new batch of work...");

            let mut transactions = vec![];
            match work.entry {
                WorkEntry::SingleTx(tx) => transactions.push(tx),
                WorkEntry::MultipleTxs(txs) => transactions = txs,
            }
            
            let processed_output = self.process_transactions(&self.bank, &transactions);
            info!("Processed a total of {:?} from the given {} txs batch", processed_output.processed_counts, transactions.len());
            
            let mut completed_txs = vec![];
            let mut failed_txs = vec![];
            for (processing_result, tx) in processed_output.processing_results.iter().zip(transactions)
            {   
                if processing_result.was_processed_with_successful_result() {
                    completed_txs.push(tx);
                } else {
                    failed_txs.push(tx);
                }
            }

            let completed_entries = match completed_txs.len() {
                0 => None,
                _ => Some(WorkEntry::MultipleTxs(completed_txs))
            };
            
            let failed_entries = match failed_txs.len() {
                0 => None,
                _ => Some(WorkEntry::MultipleTxs(failed_txs))
            };

            if self.completed_work_sender
                .send(FinishedWork {
                    completed_entry: completed_entries,
                    failed_entry: failed_entries,
                })
                .is_err()
            {
                // kill this worker if finished_work channel is broken
                break;
            } 
            info!("Tx Executed successfuly");
        }
    }

    fn process_transactions(&self,  bank: &Arc<Bank>, transactions: &[Tx]) -> LoadAndExecuteTransactionsOutput {
        
        //lock accounts of all txs in batch
        let batch  = TransactionBatch::new(
            bank.try_lock_accounts_with_results(transactions, transactions.iter().map(|_| Ok(()))),
            bank,
            OwnedOrBorrowed::Borrowed(transactions),
        );

        //check which txs throwed an error
        let mut error_counters = TransactionErrorMetrics::default();
        let mut retryable_transaction_indexes: Vec<_> = batch
            .lock_results()
            .iter()
            .enumerate()
            .filter_map(|(index, res)| match res {
                // following are retryable errors
                Err(TransactionError::AccountInUse) => {
                    error_counters.account_in_use += 1;
                    Some(index)
                }
                // following are non-retryable errors
                Err(TransactionError::TooManyAccountLocks) => {
                    error_counters.too_many_account_locks += 1;
                    None
                }
                //other types of errors are not of interest in. this harness environment yet
                Err(_) => None,
                Ok(_) => None,
            })
            .collect();
        info!("Found following tx indexes inside batch that must be retried:{:?}", retryable_transaction_indexes);
        
        //execute and record execution time
        let mut execute_timings = ExecuteTimings::default();
        let load_and_execute_transactions_output= bank
            .load_and_execute_transactions(
                &batch,
                150,
                &mut execute_timings,
                &mut error_counters,
                TransactionProcessingConfig {
                    account_overrides: None,
                    check_program_modification_slot: bank.check_program_modification_slot(),
                    log_messages_bytes_limit: None,
                    limit_to_load_programs: true,
                    recording_config: ExecutionRecordingConfig::new_single_setting(true),
                }
            );
        //compute total execution time
        let actual_execute_time = execute_timings
            .execute_accessories
            .process_instructions
            .total_us
            .0;
        info!("Executed batch of {} in {} us", transactions.len(), actual_execute_time);
        
        //return execution result
        load_and_execute_transactions_output
    }
}