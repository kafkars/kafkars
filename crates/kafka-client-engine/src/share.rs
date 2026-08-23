//! Curated low-level Rust API for hosted share-member ownership.

pub use crate::config::EngineShareConsumerFetchConfig;
pub use crate::consumer::{
    ShareAcknowledgeBrokerError, ShareAcknowledgeDeliveryStatus, ShareAcknowledgeFailure,
    ShareAcknowledgeFailureKind, ShareAcknowledgeOutcome, ShareAcknowledgePartitionOutcome,
    ShareAcknowledgeResponse, ShareAcknowledgement, ShareAcknowledgementAccepted,
    ShareAcknowledgementAdmissionError, ShareAcknowledgementAdmissionErrorKind,
    ShareAcknowledgementBuildError, ShareAcknowledgementBuildErrorKind,
    ShareAcknowledgementObserver, ShareAcknowledgementObserverError,
    ShareConsumerAssignmentPartition, ShareConsumerBatch, ShareConsumerClose,
    ShareConsumerCloseAdmissionError, ShareConsumerCloseAdmissionErrorKind,
    ShareConsumerCloseError, ShareConsumerCloseErrorKind, ShareConsumerHandle, ShareConsumerHeader,
    ShareConsumerRecord, ShareConsumerRecords, ShareConsumerRecv, ShareConsumerRecvError,
    ShareConsumerRecvErrorKind, ShareConsumerRegistration, ShareConsumerRegistrationError,
    ShareConsumerRegistrationErrorKind, ShareConsumerStartCapture, ShareConsumerStartupFailureKind,
    ShareConsumerState, ShareConsumerStateError, ShareConsumerStateErrorKind,
    ShareConsumerTryTakeBatchError, ShareConsumerTryTakeBatchErrorKind, ShareDisposition,
    ShareRecordDecision,
};
