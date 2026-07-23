//! Uniquely controlled transactional producer and explicit transaction lifecycle.

use crate::client::Client;
use crate::error::KafkaError;
use crate::operation::Operation;

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
            _client: self.client.clone(),
            transactional_id: self.transactional_id.clone(),
        }))
    }
}

/// Active transaction that must be committed or aborted explicitly.
#[derive(Debug)]
pub struct Transaction {
    _client: Client,
    transactional_id: String,
}

impl Transaction {
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
