//! Declarative public transactional-owner, lifecycle, and send surface.

mod builder;
mod end;
mod end_error;
mod identity;
mod initialization;
mod producer;
mod send;
mod send_error;
#[expect(
    clippy::module_inception,
    reason = "transaction.rs owns the public Transaction type while mod.rs remains a declarative facade"
)]
mod transaction;

pub use builder::TransactionalProducerBuilder;
pub use end::{AbortTransaction, CommitTransaction};
pub use end_error::TransactionEndAdmissionError;
pub use identity::TransactionalProducerIdentity;
pub use initialization::InitializeTransactionalProducer;
pub use producer::TransactionalProducer;
pub use send::SendTransactionRecord;
pub use send_error::TransactionSendAdmissionError;
pub use transaction::Transaction;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod end_error_test;
#[cfg(test)]
mod end_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod initialization_test;
#[cfg(test)]
mod producer_test;
#[cfg(test)]
mod send_error_test;
#[cfg(test)]
mod send_test;
#[cfg(test)]
mod transaction_test;
