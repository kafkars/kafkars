//! Declarative public share batch, observation error, and record-view boundary.

mod acknowledgement;
mod batch;
mod error;
mod record;

#[cfg(test)]
mod acknowledgement_test;
#[cfg(test)]
mod batch_test;

pub use acknowledgement::{ShareAcknowledgement, ShareAcknowledgementBuildError};
pub use batch::ShareConsumerBatch;
pub use error::{ShareConsumerTryTakeBatchError, ShareConsumerTryTakeBatchErrorKind};
pub use kafka_client_core::{
    ShareAcknowledgementBuildErrorKind, ShareDisposition, ShareRecordDecision,
};
pub use record::{ShareConsumerHeader, ShareConsumerRecord, ShareConsumerRecords};
