use crate::harness::scheduler::scheduler::Scheduler;

#[derive(Debug)]
pub struct BloomScheduler;

impl<Tx: Send + Sync + 'static> Scheduler<Tx> for BloomScheduler {}