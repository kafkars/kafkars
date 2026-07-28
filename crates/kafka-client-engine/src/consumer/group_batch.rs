//! Declarative public batch, checkpoint, error, and record-view boundary.

mod batch;
mod checkpoint;
mod error;
mod record;

#[cfg(test)]
mod batch_test;
#[cfg(test)]
pub(crate) mod test_support;

pub use batch::GroupConsumerBatch;
pub use checkpoint::GroupConsumerCheckpoint;
pub use error::{GroupConsumerTryTakeBatchError, GroupConsumerTryTakeBatchErrorKind};
pub use record::{GroupConsumerHeader, GroupConsumerRecord, GroupConsumerRecords};
