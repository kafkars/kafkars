//! Declarative immediate group checkpoint-acknowledgment boundary.

mod error;
mod operation;

#[cfg(test)]
mod operation_test;

pub use error::{GroupConsumerAcknowledgeError, GroupConsumerAcknowledgeErrorKind};
