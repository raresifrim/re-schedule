use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;

use crate::harness::scheduler::scheduler::Scheduler;

#[derive(Debug)]
pub struct BloomScheduler;

impl Scheduler for BloomScheduler {
    type Tx = RuntimeTransaction<SanitizedTransaction>;
}