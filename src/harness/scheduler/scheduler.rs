use thiserror::Error;
use crossbeam_channel::{Receiver,Sender};

//TODO move these structures in a common place

//we should be able to send and receive either one or more txs at once
#[derive(Debug, Clone)]
pub enum WorkEntry<Tx> {
    SingleTx(Tx),
    MultipleTxs(Vec<Tx>)
}

/// Message: [Issuer -> Scheduler]
/// Message: [Scheduler -> Executor]
#[derive(Debug, Clone)]
pub struct Work<Tx> {
    pub entry: WorkEntry<Tx>,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("Sending channel disconnected: {0}")]
    DisconnectedSendChannel(&'static str),
    #[error("Recv channel disconnected: {0}")]
    DisconnectedRecvChannel(&'static str),
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

pub trait Scheduler<Tx: Send + Sync + 'static> {

    /// basic scheduler function that should:
    /// 1. pull data from the work issuer channel
    /// 2. schedule it as single or list of txs
    /// 3. send it to the appropriate worker channels available
    fn schedule(
        &mut self,
        issue_channel: &Receiver<Work<Tx>>,
        execution_channels: &Vec<Sender<Work<Tx>>>
    ) -> Result<SchedulingSummary, SchedulerError>{
        
        //for now just echo the txs to first worker
        let tx = issue_channel.recv().unwrap();
        execution_channels[0].send(tx);
       
        Ok(SchedulingSummary{
            num_scheduled:0,
            num_unschedulable_conflicts:0,
            num_unschedulable_threads:0
        })
    }

}