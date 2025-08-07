use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use solana_svm::account_overrides::AccountOverrides;
use thiserror::Error;
use crossbeam_channel::{Receiver,Sender};
use tracing::info;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;

pub struct HarnessTransaction<Tx>
{
    pub transaction: Tx,
    pub account_overrides: AccountOverrides
}

// we should be able to send and receive either one or more txs at once
pub enum WorkEntry<Tx>
{
    SingleTx(HarnessTransaction<Tx>),
    MultipleTxs(Vec<HarnessTransaction<Tx>>)
}

/// Message: [Issuer -> Scheduler]
/// Message: [Scheduler -> Executor]
pub struct Work<Tx> 
{
    pub entry: WorkEntry<Tx>,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("Sending channel disconnected: {0}")]
    DisconnectedSendChannel(String),
    #[error("Recv channel disconnected: {0}")]
    DisconnectedRecvChannel(String),
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct SchedulingSummary {
    /// Number of transactions scheduled.
    pub num_scheduled: usize,
    /// Number of transactions that were not scheduled due to conflicts.
    pub num_unschedulable_conflicts: usize,
    /// Number of transactions that were skipped due to thread capacity.
    pub num_unschedulable_threads: usize,
}


pub trait Scheduler {
    type Tx: TransactionWithMeta + Send + Sync + 'static;
    /// basic scheduler function that should:
    /// 1. pull data from the work issuer channel
    /// 2. schedule it as single or list of txs
    /// 3. send it to the appropriate worker channels available
    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Self::Tx>>,
        execution_channels: &[Sender<Work<Self::Tx>>]
    ) -> Result<SchedulingSummary, SchedulerError>{
        
        //this implements a dummy sequential scheduler for a single worker
        match issue_channel.recv() {
            Ok(tx) => {
                match execution_channels[0].send(tx) {
                    Ok(_) => {
                        info!("Scheduled transaction to worker");
                        return Ok(SchedulingSummary{
                            num_scheduled:1,
                            num_unschedulable_conflicts:0,
                            num_unschedulable_threads:0
                        });
                    }
                    Err(e) => return Err(SchedulerError::DisconnectedSendChannel(format!("{:?}", e))) 
                }
            },
            Err(e) => return Err(SchedulerError::DisconnectedRecvChannel(format!("{:?}", e)))
        }
    }

}

pub struct SequentialScheduler;
impl Scheduler for SequentialScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;
}