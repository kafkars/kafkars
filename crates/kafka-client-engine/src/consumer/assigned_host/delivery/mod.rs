//! Declarative public delivery lease and borrowed-record boundary.

mod batch;
mod error;
mod owned_batch;
mod owned_record;
mod owner;
mod record;

#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod owned_batch_test;
#[cfg(test)]
mod owned_record_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod return_test;

pub use batch::AssignedConsumerBatch;
pub use error::{AssignedConsumerTryTakeBatchError, AssignedConsumerTryTakeBatchErrorKind};
pub use owned_batch::AssignedConsumerOwnedBatch;
pub use owned_record::{
    AssignedConsumerOwnedHeader, AssignedConsumerOwnedRecord, AssignedConsumerOwnedRecords,
};
pub(crate) use owner::AssignedConsumerDelivery;
pub use record::{AssignedConsumerHeader, AssignedConsumerRecord, AssignedConsumerRecords};
