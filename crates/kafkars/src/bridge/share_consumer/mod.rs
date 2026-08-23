//! Declarative bridge for unique share-member ownership and observation.

mod acknowledgement;
mod batch;
mod close;
mod recv;
mod recv_result;
mod registration;
mod state;

pub(crate) use batch::{
    ShareConsumerBatch, ShareConsumerHeader, ShareConsumerRecord, ShareConsumerRecords,
};
pub(crate) use close::ShareConsumerClose;
pub(crate) use recv::ShareConsumerRecv;
pub(crate) use registration::ShareConsumerEngine;
pub(crate) use registration::translate_registration_kind;

#[cfg(test)]
mod acknowledgement_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod recv_result_test;
#[cfg(test)]
mod registration_test;
#[cfg(test)]
mod state_test;
pub(crate) use acknowledgement::{
    ShareAcknowledgement, ShareAcknowledgementBuildError, ShareAcknowledgementBuildErrorKind,
    ShareDisposition, ShareRecordDecision,
};
