use crossbeam_channel::{Receiver,Sender};
use tracing::info;
use crate::harness::scheduler::scheduler::WorkEntry;
use crate::harness::scheduler::scheduler::Work;

/// Message: [Worker -> Issuer]
/// Processed transactions.
pub struct FinishedWork<Tx> {
    pub completed_entry: Option<WorkEntry<Tx>>,
    pub failed_entry: Option<WorkEntry<Tx>>,
}

#[derive(Debug)]
pub struct TxExecutor<Tx> {
    //TODO: add actual Consumer that will execute the txs properly
    work_receiver: Receiver<Work<Tx>>,
    completed_work_sender: Sender<FinishedWork<Tx>>
}

impl<Tx> TxExecutor<Tx>
where Tx: Send + Sync + 'static {
    pub fn new(
        work_receiver: Receiver<Work<Tx>>,
        completed_work_sender: Sender<FinishedWork<Tx>>,
    ) -> Self {
        Self {work_receiver, completed_work_sender}
    }

    pub fn run(self) -> std::thread::JoinHandle<()>{
        let handle = std::thread::spawn(move || {
            self.execute_txs();
        });
        //return handle
        handle
    }
    fn execute_txs(&self) {
        while let Ok(work) = self.work_receiver.recv() {
            //for now just echo back the work as if we actually executed it successfuly
            if self.completed_work_sender
                .send(FinishedWork {
                    completed_entry: Some(work.entry),
                    failed_entry: None,
                })
                .is_err()
            {
                // kill this worker if finished_work channel is broken
                break;
            } 
            info!("Tx Executed successfuly");
        }
    }
}