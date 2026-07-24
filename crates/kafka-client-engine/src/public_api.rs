//! Curated public re-exports for engine execution and observation.

pub use crate::admin::{
    AdminHandle, ClusterBroker, ClusterDescription, CreatePartitionsAccepted,
    CreatePartitionsAcceptedFaultKind, CreatePartitionsAdmissionError,
    CreatePartitionsAdmissionErrorKind, CreatePartitionsDeliveryStatus, CreatePartitionsFailure,
    CreatePartitionsFailureKind, CreatePartitionsObserver, CreatePartitionsObserverError,
    CreatePartitionsOutcome, CreatePartitionsRequest, CreateTopic, CreateTopicConfig,
    CreateTopicError, CreateTopicResult, CreateTopicsAccepted, CreateTopicsAcceptedFaultKind,
    CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind, CreateTopicsDeliveryStatus,
    CreateTopicsFailure, CreateTopicsFailureKind, CreateTopicsObserver, CreateTopicsObserverError,
    CreateTopicsOutcome, CreateTopicsRequest, DeleteTopicError, DeleteTopicResult,
    DeleteTopicsAccepted, DeleteTopicsAcceptedFaultKind, DeleteTopicsAdmissionError,
    DeleteTopicsAdmissionErrorKind, DeleteTopicsDeliveryStatus, DeleteTopicsFailure,
    DeleteTopicsFailureKind, DeleteTopicsObserver, DeleteTopicsObserverError, DeleteTopicsOutcome,
    DeleteTopicsRequest, DescribeClusterAccepted, DescribeClusterAcceptedFaultKind,
    DescribeClusterAdmissionError, DescribeClusterAdmissionErrorKind, DescribeClusterBrokerError,
    DescribeClusterDeliveryStatus, DescribeClusterFailure, DescribeClusterFailureKind,
    DescribeClusterObserver, DescribeClusterObserverError, DescribeClusterOutcome,
    DescribeConfigEntry, DescribeConfigResourceError, DescribeConfigResourceResult,
    DescribeConfigSynonym, DescribeConfigsAccepted, DescribeConfigsAcceptedFaultKind,
    DescribeConfigsAdmissionError, DescribeConfigsAdmissionErrorKind, DescribeConfigsBatch,
    DescribeConfigsDeliveryStatus, DescribeConfigsFailure, DescribeConfigsFailureKind,
    DescribeConfigsObserver, DescribeConfigsObserverError, DescribeConfigsOutcome,
    DescribeConfigsRequest, DescribeConfigsResourceQuery, DescribeTopicError, DescribeTopicResult,
    DescribeTopicsAccepted, DescribeTopicsAcceptedFaultKind, DescribeTopicsAdmissionError,
    DescribeTopicsAdmissionErrorKind, DescribeTopicsDeliveryStatus, DescribeTopicsFailure,
    DescribeTopicsFailureKind, DescribeTopicsObserver, DescribeTopicsObserverError,
    DescribeTopicsOutcome, DescribeTopicsRequest, PartitionIncrease, PartitionIncreaseError,
    PartitionIncreaseResult, TopicDescription, TopicPartitionDescription,
};
pub use crate::config::{EngineConfig, EngineProducerLimits, ProducerCompression};
pub use crate::consumer::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerAssignment, AssignedConsumerAssignmentEpoch,
    AssignedConsumerAssignmentInputError, AssignedConsumerAssignmentInputErrorKind,
    AssignedConsumerClaimError, AssignedConsumerCloseObserver, AssignedConsumerCloseObserverError,
    AssignedConsumerControlAccepted, AssignedConsumerControlError,
    AssignedConsumerControlErrorKind, AssignedConsumerHandle, AssignedConsumerPartition,
    AssignedConsumerPartitionInputError, AssignedConsumerPartitionInputErrorKind,
    AssignedConsumerStartPosition, AssignedConsumerTryCloseAccepted, AssignedConsumerTryCloseError,
    AssignedConsumerTryCloseErrorKind, AssignedConsumerTryReplaceAssignmentAccepted,
    AssignedConsumerTryReplaceAssignmentError, AssignedConsumerTryReplaceAssignmentErrorKind,
};
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
