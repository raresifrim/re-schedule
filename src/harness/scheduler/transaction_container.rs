use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;

pub type TransactionId = usize;

#[derive(Debug)]
pub struct TransactionContainer<Tx: TransactionWithMeta> {
    capacity: usize,
    queue: Vec<Tx>,
}

//#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub trait Container<Tx: TransactionWithMeta> {
    /// Create a new `TransactionStateContainer` with the given capacity.
    fn with_capacity(capacity: usize) -> Self;

    fn queue_size(&self) -> usize;

    /// Returns true if the queue is empty.
    fn is_empty(&self) -> bool;

    /// Returns true if the queue is empty.
    fn is_full(&self) -> bool;

    /// Get the top transaction id in the priority queue.
    fn pop(&mut self) -> Option<Tx>;

    /// Pushes a new tx into the queue unless queue is at full capacity
    fn try_push(&mut self, tx: Tx) -> bool;

    /// Get reference to `SanitizedTransactionTTL` by id.
    /// Panics if the transaction does not exist.
    fn get_transaction(&self, id: TransactionId) -> Option<&Tx>;

    /// Remove transaction by id.
    fn remove_by_id(&mut self, id: TransactionId);

    fn clear(&mut self);
}

impl<Tx: TransactionWithMeta> Container<Tx> for TransactionContainer<Tx> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            queue: Vec::with_capacity(capacity),
        }
    }

    fn is_full(&self) -> bool {
        self.queue.len() == self.capacity
    }

    fn queue_size(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn pop(&mut self) -> Option<Tx> {
        self.queue.pop()
    }

    fn try_push(&mut self, tx: Tx) -> bool {
        if self.queue.len() <= self.capacity {
            self.queue.push(tx);
            return true;
        }
        return false;
    }

    fn get_transaction(&self, id: TransactionId) -> Option<&Tx> {
        self.queue.get(id)
    }

    fn remove_by_id(&mut self, id: TransactionId) {
        self.queue.remove(id);
    }

    fn clear(&mut self) {
        self.queue.clear();
    }
}