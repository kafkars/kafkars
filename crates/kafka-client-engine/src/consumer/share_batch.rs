//! Declarative public share batch, observation error, and record-view boundary.

mod batch;
mod error;
mod record;

#[cfg(test)]
mod batch_test;

pub use batch::ShareConsumerBatch;
pub use error::{ShareConsumerTryTakeBatchError, ShareConsumerTryTakeBatchErrorKind};
pub use record::{ShareConsumerHeader, ShareConsumerRecord, ShareConsumerRecords};
