use std::sync::Mutex;
use crate::harness::scheduler::{scheduler_controller::Transaction, transaction_container::{Container, TransactionContainer}};
use std::sync::Arc;

const WINDOW_SIZE: usize = 10;

#[derive(Debug)]
pub struct TransactionBuffer {
    container_mutex: Arc<Mutex<TransactionContainer<Transaction>>>,
    txs: Vec <Transaction>
}

impl TransactionBuffer {
    pub fn read_and_buffer_packets(&mut self) {
        let local_container =self.container_mutex.clone();
        loop {
           { //create scope for mutex
                let mut lock = local_container.try_lock();
                if let Ok(ref mut container) = lock {
                    //once we got the lock, push another window of txs
                    for _ in 0..WINDOW_SIZE {
                        if !self.txs.is_empty() {
                            container.try_push(self.txs.pop().unwrap());
                        }
                    } 
                } 
            }
            if self.txs.is_empty() {
                break;
            }
        }
    }
}

