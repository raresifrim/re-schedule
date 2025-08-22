use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Default)]
pub struct ExecutionTracker (
    /// txs processed until now
    AtomicUsize,
    /// compute time spent until now
    AtomicU64,
    /// total CUs
    AtomicU64
);


impl ExecutionTracker {
    pub fn new() -> Self {
        Self (
            AtomicUsize::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
        )
    }

    pub fn num_txs_executed(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn total_time_executed(&self) -> u64 {
        self.1.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn total_cus(&self) -> u64 {
        self.2.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn update(
        & self,
        num_transactions: usize,
        total_time: u64,
        total_cus: u64
    ) {
       self.0.fetch_add(num_transactions, std::sync::atomic::Ordering::SeqCst);
       self.1.fetch_add(total_time, std::sync::atomic::Ordering::SeqCst);
       self.2.fetch_add(total_cus, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn reset(&mut self) {
        self.0.store(0, std::sync::atomic::Ordering::SeqCst);
        self.1.store(0, std::sync::atomic::Ordering::SeqCst);
        self.2.store(0, std::sync::atomic::Ordering::SeqCst);
    }
}
