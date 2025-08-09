use anyhow::Context;
use crossbeam_channel::{Receiver,Sender};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_runtime::bank::LoadAndExecuteTransactionsOutput;
use solana_sdk::clock::MAX_PROCESSING_AGE;
use solana_svm::account_overrides;
use solana_svm::account_overrides::AccountOverrides;
use tracing::info;
use crate::harness::scheduler::scheduler::HarnessTransaction;
use crate::harness::scheduler::scheduler::WorkEntry;
use crate::harness::scheduler::scheduler::Work;
use solana_runtime::bank::Bank;
use std::collections::HashMap;
use std::sync::Arc;
use solana_runtime::transaction_batch::TransactionBatch;
use solana_runtime::transaction_batch::OwnedOrBorrowed;
use solana_sdk::transaction::*;
use solana_svm::transaction_error_metrics::TransactionErrorMetrics;
use solana_svm::transaction_processor::TransactionProcessingConfig;
use solana_svm::transaction_processor::ExecutionRecordingConfig;
use solana_timings::ExecuteTimings;
use solana_svm::transaction_processing_result::TransactionProcessingResultExtensions;
use solana_account::AccountSharedData;
use solana_pubkey::Pubkey;
use solana_accounts_db::account_locks::validate_account_locks;
use std::slice;
use solana_svm::transaction_processing_result::ProcessedTransaction;


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

            let mut harness_transactions = vec![];
            match work.entry {
                WorkEntry::SingleTx(tx) => harness_transactions.push(tx),
                WorkEntry::MultipleTxs(txs) => harness_transactions = txs
            }
            
            let processed_output = self.process_transactions_one_by_one(&self.bank, &harness_transactions);
            
            let mut completed_txs = vec![];
            let mut failed_txs = vec![];
            for (processed_result, tx) in  processed_output.iter().zip(harness_transactions)
            {   
                let result = processed_result.as_ref().unwrap();
                match result.status() {
                    Ok(_) => {
                        info!("Successfuly executed transaction identified by message hash and signature: {:?}, {:?}", 
                                tx.transaction.message_hash(),
                                tx.transaction.signature());
                        completed_txs.push(tx);
                    },
                    Err(e) => {
                        info!("Execution of transaction identified by message hash and signature: {:?}, {:?} failed with following details:{:?}",
                                tx.transaction.message_hash(),
                                tx.transaction.signature()
                                ,e);
                        failed_txs.push(tx);
                    }
                };
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
                info!("Tx issuer not present anymore, exiting as well...");
                break;
            } 
            info!("Transaction executed and result sent back for recording");
        }
    }

    fn process_transactions_one_by_one(&self,  bank: &Arc<Bank>,  harness_transactions: &[HarnessTransaction<Tx>]) -> Vec<Result<ProcessedTransaction>> {
        
        let mut actual_execute_time: u64 = 0;
        let mut transaction_results = vec![];

        for tx in harness_transactions {
            let (tx_result, tx_time) = self.process_single_transaction(bank, &tx.transaction, &tx.account_overrides);
            transaction_results.extend(tx_result);
            actual_execute_time += tx_time;
        }

        info!("Executed {} transactions in {} us", harness_transactions.len(), actual_execute_time);
        
        //return execution result
        transaction_results
    }

   fn process_single_transaction(&self,  bank: &Arc<Bank>, transaction: &Tx, account_override: &AccountOverrides) -> (Vec<Result<ProcessedTransaction>>, u64) {

        let batch = TransactionBatch::new(
            bank.try_lock_accounts_with_results(slice::from_ref(transaction), slice::from_ref(transaction).into_iter().map(|_| Ok(()))),
            bank,
            OwnedOrBorrowed::Borrowed(slice::from_ref(transaction)),
        );
        
        info!("Processing tx with signature and message hash: {:?}, {:?}", transaction.signature(), transaction.message_hash());

        //prepare bank for current tx as it might be older than the current snapshot
        bank.load_addresses_from_ref(transaction.message_address_table_lookups()).context("Failed to load addresses from ALT").unwrap();
        bank.register_recent_blockhash_for_test(transaction.recent_blockhash(), None);
        
        let mut timings = ExecuteTimings::default();
        let LoadAndExecuteTransactionsOutput {
            processing_results,
            ..
        } = bank.load_and_execute_transactions(
            &batch,
            MAX_PROCESSING_AGE,
            &mut timings,
            &mut TransactionErrorMetrics::default(),
            TransactionProcessingConfig {
                account_overrides: Some(account_override),
                check_program_modification_slot: bank.check_program_modification_slot(),
                log_messages_bytes_limit: None,
                limit_to_load_programs: true,
                recording_config: ExecutionRecordingConfig {
                    enable_cpi_recording: true,
                    enable_log_recording: true,
                    enable_return_data_recording: true,
                    enable_transaction_balance_recording: false,
                },
            },
        );

        info!("Processed transaction timings: {:?}", timings);
        let actual_execute_time = timings
            .execute_accessories
            .process_instructions
            .total_us
            .0;
        (processing_results, actual_execute_time)

   }

}
