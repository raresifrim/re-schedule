use crossbeam_channel::{Sender,Receiver};
use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;
use crate::harness::scheduler::scheduler::{Scheduler, Work};
use tracing::info;

#[derive(Debug)]
pub struct TxScheduler<S> where S: Scheduler {
    /// scheduler strategy used for the tx scheduler
    scheduler: S,
    /// channel connected to the tx issuer 
    work_issuer: Receiver<Work<S::Tx>>,
    /// channel connected to the tx executors
    work_executors: Vec<Sender<Work<S::Tx>>>,
}

impl<S> TxScheduler<S> where 
    S: Scheduler + Send + Sync + 'static,
{
     pub fn new(
        scheduler: S,
        work_issuer: Receiver<Work<S::Tx>>,
        work_executors: Vec<Sender<Work<S::Tx>>>,
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
                        //scheduler should return errors such as channels disconnected
                        //in which case we should end its execution
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

