//! Declarative private bridge for transaction initialization, lifecycle, and send.

mod handle;
mod identity;
mod lifecycle;
mod offsets;
mod offsets_result;
mod operation;
mod owner;
mod result;
mod send;
mod send_batch;
mod send_result;
mod validation;

pub(crate) use handle::TransactionalProducerInitializer;
pub(crate) use lifecycle::{TransactionEndEngine, TransactionEngine};
pub(crate) use offsets::TransactionOffsetsEngine;
pub(crate) use operation::TransactionInitialization;
pub(crate) use owner::TransactionalProducerEngine;
pub(crate) use send::TransactionSendEngine;
pub(crate) use send_batch::TransactionBatchSendEngine;
pub(crate) use validation::TransactionValidationEngine;

#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod lifecycle_test;
#[cfg(test)]
mod offsets_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod result_test;
#[cfg(test)]
mod send_batch_test;
#[cfg(test)]
mod send_result_admission_test;
#[cfg(test)]
mod send_result_test;
#[cfg(test)]
mod send_test;
#[cfg(test)]
mod validation_test;
