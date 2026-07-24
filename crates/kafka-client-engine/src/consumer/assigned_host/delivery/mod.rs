//! Declarative public delivery lease and borrowed-record boundary.

mod batch;
mod error;
mod owner;
mod record;

#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod return_test;

pub use batch::AssignedConsumerBatch;
pub use error::{AssignedConsumerTryTakeBatchError, AssignedConsumerTryTakeBatchErrorKind};
pub(crate) use owner::AssignedConsumerDelivery;
pub use record::{AssignedConsumerHeader, AssignedConsumerRecord, AssignedConsumerRecords};
