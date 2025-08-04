use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;

use crate::harness::scheduler::scheduler::Scheduler;

#[derive(Debug)]
pub struct BloomScheduler;

impl<Tx: TransactionWithMeta + Send + Sync + 'static> Scheduler<Tx> for BloomScheduler {}