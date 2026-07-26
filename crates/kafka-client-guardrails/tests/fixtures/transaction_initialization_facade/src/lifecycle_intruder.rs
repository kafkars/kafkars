//! Forbidden public transaction lifecycle before its owners land.

struct TransactionalProducer;

impl TransactionalProducer {
    pub fn begin(&mut self) {}

    pub fn send(&mut self) {}

    pub fn commit(&mut self) {}

    pub fn abort(&mut self) {}
}
