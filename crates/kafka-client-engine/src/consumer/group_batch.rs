//! Declarative public batch, checkpoint, error, and record-view boundary.

mod batch;
mod checkpoint;
mod checkpoint_builder;
mod error;
mod record;

#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod checkpoint_builder_test;
#[cfg(test)]
pub(crate) mod test_support;

pub use batch::GroupConsumerBatch;
pub use checkpoint::GroupConsumerCheckpoint;
pub(in crate::consumer) use checkpoint::GroupConsumerCheckpointObservation;
pub use checkpoint_builder::{
    GroupConsumerCheckpointBuilder, GroupConsumerCheckpointMarkError,
    GroupConsumerCheckpointMarkErrorKind,
};
pub use error::{
    GroupConsumerFetchFailureKind, GroupConsumerPositionFailureKind,
    GroupConsumerTryTakeBatchError, GroupConsumerTryTakeBatchErrorKind,
};
pub use record::{GroupConsumerHeader, GroupConsumerRecord, GroupConsumerRecords};
