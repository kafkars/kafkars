//! Forbidden transaction lifecycle hidden outside the transaction facade.

struct TransactionalProducer;

impl TransactionalProducer {
    pub fn begin(&mut self) {}

    pub fn send(&mut self) {}

    pub fn commit(&mut self) {}

    pub fn abort(&mut self) {}
}

pub trait TransactionalProducerExt {
    fn begin(&mut self);

    fn send(&mut self);

    fn commit(&mut self);

    fn abort(&mut self);
}

impl TransactionalProducerExt for TransactionalProducer {
    fn begin(&mut self) {}

    fn send(&mut self) {}

    fn commit(&mut self) {}

    fn abort(&mut self) {}
}
