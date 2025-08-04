use crossbeam_channel::{Sender,Receiver};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use crate::harness::scheduler::scheduler::{Scheduler, Work};
use tracing::info;

#[derive(Debug)]
pub struct TxScheduler<S, Tx> {
    /// scheduler strategy used for the tx scheduler
    scheduler: S,
    /// channel connected to the tx issuer 
    work_issuer: Receiver<Work<Tx>>,
    /// channel connected to the tx executors
    work_executors: Vec<Sender<Work<Tx>>>,
}

impl<S, Tx> TxScheduler<S, Tx> where 
    S: Scheduler<Tx> + Send + Sync + 'static,
    Tx: TransactionWithMeta + Send + Sync + 'static
{
     pub fn new(
        scheduler: S,
        work_issuer: Receiver<Work<Tx>>,
        work_executors: Vec<Sender<Work<Tx>>>,
    ) -> Self {
        Self { scheduler, work_issuer, work_executors}
    }


    pub fn run(mut self) -> std::thread::JoinHandle<()>{
        let handle = std::thread::spawn(move || {
            loop {
                let schedule_resp = self.scheduler.schedule(
                    &self.work_issuer,
                    &self.work_executors
                );
                match schedule_resp {
                    Err(e) => {
                        info!("Received following error from scheduler {:?}", e);
                        break;
                    }
                    Ok(summary) => info!("Current scheduling summary: {:?}", summary)
                }    
            }
            
        });
        //return handle
        handle
    }

}

