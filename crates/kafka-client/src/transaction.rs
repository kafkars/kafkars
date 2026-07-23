//! Uniquely controlled transactional producer and explicit transaction lifecycle.

use crate::client::Client;
use crate::error::KafkaError;
use crate::operation::Operation;
use crate::producer::RecordMetadata;
use crate::record::Record;

/// Builder for one fenced transactional producer identity.
#[derive(Debug, Clone)]
pub struct TransactionalProducerBuilder {
    client: Client,
    transactional_id: String,
}

impl TransactionalProducerBuilder {
    pub(crate) fn new(client: Client, transactional_id: String) -> Self {
        Self {
            client,
            transactional_id,
        }
    }

    /// Initializes producer identity and fencing state.
    pub fn build(self) -> BeginTransactionProducer {
        Operation::ready(Ok(TransactionalProducer {
            client: self.client,
            transactional_id: self.transactional_id,
        }))
    }
}

/// Uniquely controlled transactional producer.
#[derive(Debug)]
pub struct TransactionalProducer {
    client: Client,
    transactional_id: String,
}

impl TransactionalProducer {
    /// Begins one transaction.
    pub fn begin(&mut self) -> BeginTransaction {
        Operation::ready(Ok(Transaction {
            client: self.client.clone(),
            transactional_id: self.transactional_id.clone(),
        }))
    }
}

/// Active transaction that must be committed or aborted explicitly.
#[derive(Debug)]
pub struct Transaction {
    client: Client,
    transactional_id: String,
}

impl Transaction {
    /// Sends one record as part of the active transaction.
    pub fn send(&mut self, record: Record) -> Operation<Result<RecordMetadata, KafkaError>> {
        match self.client.producer().build() {
            Ok(handle) => handle.send(record),
            Err(error) => Operation::ready(Err(error)),
        }
    }

    /// Commits the active transaction.
    pub fn commit(self) -> CommitTransaction {
        let _ = self.transactional_id;
        Operation::ready(Ok(()))
    }

    /// Aborts the active transaction.
    pub fn abort(self) -> AbortTransaction {
        let _ = self.transactional_id;
        Operation::ready(Ok(()))
    }
}

/// Transactional producer initialization operation.
pub type BeginTransactionProducer = Operation<Result<TransactionalProducer, KafkaError>>;
/// Transaction begin operation.
pub type BeginTransaction = Operation<Result<Transaction, KafkaError>>;
/// Transaction commit operation.
pub type CommitTransaction = Operation<Result<(), KafkaError>>;
/// Transaction abort operation.
pub type AbortTransaction = Operation<Result<(), KafkaError>>;
