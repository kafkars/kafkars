//! Curated public re-exports for engine execution and observation.

pub use crate::admin::{
    AdminHandle, CreateTopic, CreateTopicConfig, CreateTopicError, CreateTopicResult,
    CreateTopicsAccepted, CreateTopicsAcceptedFaultKind, CreateTopicsAdmissionError,
    CreateTopicsAdmissionErrorKind, CreateTopicsDeliveryStatus, CreateTopicsFailure,
    CreateTopicsFailureKind, CreateTopicsObserver, CreateTopicsObserverError, CreateTopicsOutcome,
    CreateTopicsRequest, DeleteTopicError, DeleteTopicResult, DeleteTopicsAccepted,
    DeleteTopicsAcceptedFaultKind, DeleteTopicsAdmissionError, DeleteTopicsAdmissionErrorKind,
    DeleteTopicsDeliveryStatus, DeleteTopicsFailure, DeleteTopicsFailureKind, DeleteTopicsObserver,
    DeleteTopicsObserverError, DeleteTopicsOutcome, DeleteTopicsRequest,
};
pub use crate::config::{EngineConfig, EngineProducerLimits};
pub use crate::delivery::{
    ProducerDeliveryFailure, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    ProducerRecordMetadata,
};
pub use crate::delivery_error::{ProducerDeliveryError, ProducerObserverError};
pub use crate::delivery_observer::{ProducerDeliveryObserver, ProducerDeliveryResult};
pub use crate::engine::Engine;
pub use crate::engine_host::{
    EngineShutdownError, EngineShutdownErrorKind, EngineStartError, EngineStartErrorKind,
};
pub use crate::flush_error::ProducerFlushError;
pub use crate::flush_observer::{ProducerFlushObserver, ProducerFlushResult};
pub use crate::producer::{
    ProducerAcceptedFault, ProducerAcceptedFaultKind, ProducerCancelAccepted, ProducerCancelError,
    ProducerCancelErrorKind, ProducerCancelFault, ProducerCancelFaultKind,
    ProducerCancellationOutcome, ProducerHandle, ProducerSendCapture, ProducerSendCaptureError,
    ProducerSendCaptureErrorKind, ProducerSendOptions, ProducerTryCloseAccepted,
    ProducerTryCloseError, ProducerTryCloseErrorKind, ProducerTryFlushAccepted,
    ProducerTryFlushError, ProducerTryFlushErrorKind, ProducerTrySendAccepted,
    ProducerTrySendError, ProducerTrySendErrorKind, PublicProducerHeader as ProducerHeader,
    PublicProducerRecord as ProducerRecord,
};
