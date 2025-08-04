use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use crossbeam_channel::{Receiver,Sender};

//Placeholder structures that just echo back the txs as if they consumed them

/// A unique identifier for a transaction batch.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TransactionBatchId(u64);

impl TransactionBatchId {
    pub fn new(index: u64) -> Self {
        Self(index)
    }
}

impl std::fmt::Display for TransactionBatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type TransactionId = usize;



pub struct ConsumeWork<Tx> {
    pub batch_id: usize,
    pub ids: Vec<TransactionId>,
    pub transactions: Vec<Tx>,
}

/// Message: [Worker -> Scheduler]
/// Processed transactions.
pub struct FinishedConsumeWork<Tx> {
    pub work: ConsumeWork<Tx>,
    pub retryable_indexes: Vec<usize>,
}

struct ConsumeWorkers {
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl ConsumeWorkers {
    fn new<Tx: TransactionWithMeta + Send + Sync + 'static>(
        work_receivers: Vec<Receiver<ConsumeWork<Tx>>>,
        completed_work_sender: Sender<FinishedConsumeWork<Tx>>,
    ) -> Self {
        let mut threads = Vec::with_capacity(work_receivers.len());

        for receiver in work_receivers {
            let completed_work_sender_clone = completed_work_sender.clone();

            let handle = std::thread::spawn(move || {
                Self::service_loop(std::thread::current().id(),receiver, completed_work_sender_clone);
            });
            threads.push(handle);
        }

        Self { threads }
    }

    fn service_loop<Tx: TransactionWithMeta + Send + Sync + 'static>(
        _id: std::thread::ThreadId,
        work_receiver: Receiver<ConsumeWork<Tx>>,
        completed_work_sender: Sender<FinishedConsumeWork<Tx>>,
    ) {
        while let Ok(work) = work_receiver.recv() {
            //println!("Thread {:?} received and completed following txs:{:?}", id, work.ids);
            if completed_work_sender
                .send(FinishedConsumeWork {
                    work,
                    retryable_indexes: vec![],
                })
                .is_err()
            {
                // kill this worker if finished_work channel is broken
                break;
            } 
        }
    }
}