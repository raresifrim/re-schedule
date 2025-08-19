use crate::harness::scheduler::scheduler::{
    SATURATION, Scheduler, SchedulingSummary, TOTAL_TXS, Work,
};
use crossbeam_channel::{Receiver, Sender};
use tracing::info;

#[derive(Debug)]
pub struct TxScheduler<S>
where
    S: Scheduler + Send + Sync + 'static,
{
    /// scheduler strategy used for the tx scheduler
    pub scheduler: S,
    /// channel connected to the tx issuer
    pub work_issuer: Receiver<Work<S::Tx>>,
    /// channel connected to the tx executors
    pub work_executors: Vec<Sender<Work<S::Tx>>>,
}

impl<S> TxScheduler<S>
where
    S: Scheduler + Send + Sync + 'static,
{
    pub fn run(mut self) -> std::thread::JoinHandle<SchedulingSummary> {
        std::thread::spawn(move || {
            // the schedulers' schedule function should implement the loop
            // that receives txs until the channel becomes empty or disconnected
            let schedule_resp = self
                .scheduler
                .schedule(&self.work_issuer, &self.work_executors);
            if let Err(e) = schedule_resp {
                //scheduler should return errors such as channels disconnected
                //in which case we should end its execution
                info!("Channel disconnected");
            }

            //compute saturation per worker in regard of total txs scheduled
            let mut summary = self.scheduler.get_summary();
            for worker_index in 0..self.work_executors.len() {
                let report = summary.txs_per_worker.get_mut(&worker_index).unwrap();
                let txs_per_worker = report[TOTAL_TXS] as f64;
                let total_txs = summary.total_txs as f64;
                report[SATURATION] = ((txs_per_worker / total_txs) * 100.0) as u64;
            }

            summary
        })
    }
}
