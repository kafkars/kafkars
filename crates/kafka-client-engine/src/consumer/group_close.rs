//! Declarative explicit-close boundary for one hosted classic-group consumer.

mod admission;
#[cfg(test)]
mod admission_test;
mod error;
#[cfg(test)]
mod error_test;
mod operation;
#[cfg(test)]
mod operation_test;

pub use admission::{GroupConsumerCloseAdmissionError, GroupConsumerCloseAdmissionErrorKind};
pub use error::{GroupConsumerCloseError, GroupConsumerCloseErrorKind};
pub use operation::GroupConsumerClose;
