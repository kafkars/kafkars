//! Declarative public transactional-owner, lifecycle, and send surface.

mod batch_metadata;
mod builder;
mod end;
mod end_error;
mod end_intent;
mod identity;
mod initialization;
mod offsets;
mod offsets_error;
mod producer;
mod send;
mod send_batch;
mod send_batch_error;
mod send_error;
#[expect(
    clippy::module_inception,
    reason = "transaction.rs owns the public Transaction type while mod.rs remains a declarative facade"
)]
mod transaction;

pub use batch_metadata::TransactionBatchMetadata;
pub use builder::TransactionalProducerBuilder;
pub use end::{AbortTransaction, CommitTransaction};
pub use end_error::TransactionEndAdmissionError;
pub use end_intent::TransactionEndIntent;
pub use identity::TransactionalProducerIdentity;
pub use initialization::InitializeTransactionalProducer;
pub use offsets::SendTransactionOffsets;
pub use offsets_error::TransactionOffsetsAdmissionError;
pub use producer::TransactionalProducer;
pub use send::SendTransactionRecord;
pub use send_batch::SendTransactionBatch;
pub use send_batch_error::TransactionBatchSendAdmissionError;
pub use send_error::TransactionSendAdmissionError;
pub use transaction::Transaction;

#[cfg(test)]
mod batch_metadata_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod end_error_test;
#[cfg(test)]
mod end_intent_test;
#[cfg(test)]
mod end_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod initialization_test;
#[cfg(test)]
mod offsets_error_test;
#[cfg(test)]
mod offsets_test;
#[cfg(test)]
mod producer_test;
#[cfg(test)]
mod send_batch_error_test;
#[cfg(test)]
mod send_batch_test;
#[cfg(test)]
mod send_error_test;
#[cfg(test)]
mod send_test;
#[cfg(test)]
mod transaction_test;
