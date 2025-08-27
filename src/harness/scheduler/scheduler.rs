use ahash::HashMap;
use crossbeam_channel::{Receiver, Sender};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_svm::account_overrides::AccountOverrides;
use thiserror::Error;
use std::sync::Arc;
use crate::harness::{executor::execution_tracker::ExecutionTracker};
use crate::harness::scheduler::thread_aware_account_locks::ThreadAwareAccountLocks;
use std::sync::Mutex;
pub type WorkerId = usize;

pub struct HarnessTransaction<Tx> {
    pub transaction: Tx,
    pub account_overrides: AccountOverrides,
    //a compressed u64 values of the blockhash
    pub blockhash: u64,
    /// if we run in simulation mode we randomly generate an execution time in us
    pub simulated_ex_us: Option<u64>,
    ///flag to mark if this tx failed and is retried
    pub retry: bool,
    /// Compute Unit cost of tx
    pub cu_cost: u64,
}


/// Message: [Issuer -> Scheduler]
/// Message: [Scheduler -> Executor]
pub struct Work<Tx> {
    pub entry: Vec<HarnessTransaction<Tx>>,
    pub total_cus: u64,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("Sending channel disconnected: {0}")]
    DisconnectedSendChannel(String),
    #[error("Recv channel disconnected: {0}")]
    DisconnectedRecvChannel(String),
}

#[derive(Default, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkerSummary {
    pub unique: u64,
    pub total: u64,
    pub retried: u64,
    pub work_share: f64,
}

#[derive(Default, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[must_use]
pub struct SchedulingSummary {
    /// 4 types of txs that can be reported in the vector reported:
    /// 1. unique txs scheduled to each worker
    /// 2. total txs scheduled to each worker (includes failed txs that are retried)
    /// 3. duplicate txs that were scheduled to each worker
    /// 4. saturation per worker
    pub txs_per_worker: HashMap<WorkerId, WorkerSummary>,
    /// total unique txs scheduled among all workers
    pub useful_txs: u64,
    /// total txs scheduled among all workers
    pub total_txs: u64,
}

pub trait Scheduler {
    type Tx: TransactionWithMeta + Send + Sync + 'static;
    /// basic scheduler function that should:
    /// 1. pull data from the work issuer channel
    /// 2. schedule it as single or list of txs
    /// 3. send it to the appropriate worker channels available
    /// we also pass a reference to the current account locks held by each thread
    /// this can be used for scheduling srategies
    /// Scheduler should lock the accounts of the scheduled txs
    /// and TxIssuer will release the accounts of the completed txs received from the workers
    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>]
    ) -> Result<(), SchedulerError>;

    /// retrieve scheduling summary
    fn get_summary(&self) -> SchedulingSummary;

    /// All schedulers should have a reference of the execution trackers of each thread workers
    fn add_thread_trackers(&mut self, execution_trackers: Vec<Arc<ExecutionTracker>>);

}
