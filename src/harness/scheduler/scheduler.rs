use crate::harness::scheduler::transaction_container::Container;
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use crate::harness::scheduler::scheduling_common::SchedulingCommon;
use thiserror::Error;
use std::sync::{Arc,Mutex};

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

pub trait Scheduler<Tx: TransactionWithMeta> {
    /// Schedule transactions from `container`.
    /// pre-graph and pre-lock filters may be passed to be applied
    /// before specific actions internally.
    fn schedule<C:Container<Tx>>(
        &mut self,
        container: &Arc<Mutex<C>>,
        //TODO
        //pre_graph_filter: impl Fn(&[&Tx], &mut [bool]),
        //pre_lock_filter: impl Fn(&TransactionState<Tx>) -> PreLockFilterAction,
    ) -> Result<SchedulingSummary, SchedulerError>;

    /// Receive completed batches of transactions without blocking.
    /// Returns (num_transactions, num_retryable_transactions) on success.
    fn receive_completed<C:Container<Tx>>(
        &mut self,
        container: &mut Arc<Mutex<C>>,
    ) -> anyhow::Result<()> {
        loop {
            let num_transactions = self
                .scheduling_common_mut()
                .try_receive_completed(container)?;
            if num_transactions == 0 {
                break;
            }
        }
        
        Ok(())
    }

    /// All schedulers should have access to the common context for shared
    /// implementation.
    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx>;
}