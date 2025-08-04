use crate::harness::scheduler::scheduler::Scheduler;
use solana_runtime::bank::Bank;
use std::sync::{Arc, Mutex};
use crate::harness::scheduler::transaction_container::Container;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::transaction::SanitizedTransaction;

const TOTAL_BUFFERED_PACKETS: usize = 64;

pub type Transaction = RuntimeTransaction<SanitizedTransaction>;

pub struct SchedulerController<C, S>
where
    C: Container<Transaction>,
    S: Scheduler<Transaction>,
{
    bank: Arc<Bank>,
    /// Container for transactions.
    /// Shared resource between `packet_receiver` and `scheduler`.
    pub container: Arc<Mutex<C>>,
    /// State for scheduling and communicating with worker threads.
    pub scheduler: S
}

impl<C, S> SchedulerController<C, S>
where
    C: Container<Transaction>,
    S: Scheduler<Transaction>,
{
    pub fn new(
        bank: Arc<Bank>,
        scheduler: S
    ) -> Self {
        Self {
            bank,
            container: Arc::new(Mutex::new(Container::with_capacity(TOTAL_BUFFERED_PACKETS))),
            scheduler,
        }
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        loop {
            self.receive_completed()?;
            
            self.process_transactions()?;
            
            //if self.receive_and_buffer_packets().is_err() {
                //our thread that fills our container finished work
            //    break;
            //}
        }
        Ok(())
    }

    /// Process packets based on decision.
    pub fn process_transactions(
        &mut self,
    ) -> anyhow::Result<()> {
       
        self.scheduler.schedule(&self.container)?;
        Ok(())
    }

    fn pre_graph_filter(
    ) {
        todo!();
    }

    /// Clears the transaction state container.
    /// This only clears pending transactions, and does **not** clear in-flight transactions.
    fn clear_container(&mut self) {
        todo!()
    }

    /// Clean unprocessable transactions from the queue. These will be transactions that are
    /// expired, already processed, or are no longer sanitizable.
    /// This only clears pending transactions, and does **not** clear in-flight transactions.
    fn clean_queue(&mut self) {
       todo!()
    }

    /// Receives completed transactions from the workers and updates metrics.
    pub fn receive_completed(&mut self) -> anyhow::Result<()> {
        self.scheduler.receive_completed(&mut self.container)?;
        Ok(())
    }

}