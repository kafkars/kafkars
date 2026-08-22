//! Closed classification of normalized Fetch outcome failures for interpreters.

use super::{
    FetchDecodeFailure, FetchOutcomeFailure, FetchResponseFailure,
    record_failure::classify_record_error,
};

/// Protocol-local failure categories consumed by policy interpreters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchOutcomeFailureClass {
    DriverRejected,
    Compatibility,
    InvalidResponse,
    ResponseTooLarge,
}

/// Classifies bounded protocol normalization without exposing wire vocabulary.
pub(crate) fn classify_fetch_outcome_failure(
    failure: &FetchOutcomeFailure,
) -> FetchOutcomeFailureClass {
    match failure {
        FetchOutcomeFailure::Response(failure) => classify_response_failure(failure),
        FetchOutcomeFailure::Retention(_failure) => FetchOutcomeFailureClass::ResponseTooLarge,
        FetchOutcomeFailure::InvalidRequestedOffset { .. } => {
            FetchOutcomeFailureClass::DriverRejected
        }
        FetchOutcomeFailure::NegativeThrottleTime { .. }
        | FetchOutcomeFailure::UnexpectedSessionId { .. }
        | FetchOutcomeFailure::ThrottleTickOverflow { .. }
        | FetchOutcomeFailure::CorrelatedShapeLost => FetchOutcomeFailureClass::InvalidResponse,
    }
}

fn classify_response_failure(failure: &FetchResponseFailure) -> FetchOutcomeFailureClass {
    match failure {
        FetchResponseFailure::UnsupportedApiVersion { .. } => {
            FetchOutcomeFailureClass::Compatibility
        }
        FetchResponseFailure::RequestedPartitionOutOfRange { .. } => {
            FetchOutcomeFailureClass::DriverRejected
        }
        FetchResponseFailure::Decode(failure) => classify_decode_failure(failure),
        FetchResponseFailure::TopicCount { .. }
        | FetchResponseFailure::TopicNameMismatch
        | FetchResponseFailure::TopicIdMismatch
        | FetchResponseFailure::PartitionCount { .. }
        | FetchResponseFailure::PartitionIndexMismatch { .. } => {
            FetchOutcomeFailureClass::InvalidResponse
        }
    }
}

fn classify_decode_failure(failure: &FetchDecodeFailure) -> FetchOutcomeFailureClass {
    match failure {
        FetchDecodeFailure::ResponseRetainedBytes { .. }
        | FetchDecodeFailure::ResponseAllocations { .. }
        | FetchDecodeFailure::TopicCount { .. }
        | FetchDecodeFailure::PartitionCount { .. }
        | FetchDecodeFailure::EndpointCount { .. }
        | FetchDecodeFailure::BatchCount { .. }
        | FetchDecodeFailure::RecordCount { .. }
        | FetchDecodeFailure::HeaderCount { .. }
        | FetchDecodeFailure::AbortedTransactionCount { .. }
        | FetchDecodeFailure::ReadCommittedScratch { .. }
        | FetchDecodeFailure::LogicalRecordBytes { .. }
        | FetchDecodeFailure::AdditionalRetainedPayloadBytes { .. }
        | FetchDecodeFailure::AccountingOverflow => FetchOutcomeFailureClass::ResponseTooLarge,
        FetchDecodeFailure::RecordBatch { source, .. } => classify_record_error(source),
        FetchDecodeFailure::NegativeThrottleTime { .. }
        | FetchDecodeFailure::NegativeSessionId { .. }
        | FetchDecodeFailure::NegativePartitionIndex { .. }
        | FetchDecodeFailure::InvalidCurrentLeader { .. }
        | FetchDecodeFailure::InvalidPreferredReplica { .. }
        | FetchDecodeFailure::InvalidPartitionOffset { .. }
        | FetchDecodeFailure::InvalidEpochEndOffset { .. }
        | FetchDecodeFailure::InvalidEndpointNodeId { .. }
        | FetchDecodeFailure::InvalidEndpointPort { .. }
        | FetchDecodeFailure::NegativeLastOffsetDelta { .. }
        | FetchDecodeFailure::NegativeBaseOffset { .. }
        | FetchDecodeFailure::NextOffsetOverflow { .. }
        | FetchDecodeFailure::BatchOffsetOverlap { .. }
        | FetchDecodeFailure::InvalidPartitionLeaderEpoch { .. }
        | FetchDecodeFailure::InvalidBatchTimestamps { .. }
        | FetchDecodeFailure::OffsetOverflow
        | FetchDecodeFailure::TimestampOverflow
        | FetchDecodeFailure::NegativeRecordTimestamp { .. }
        | FetchDecodeFailure::RecordTimestampAfterBatchMax { .. }
        | FetchDecodeFailure::TimestampDeltaWithoutTimestamp { .. }
        | FetchDecodeFailure::InvalidProducerIdentity { .. }
        | FetchDecodeFailure::TransactionalIdentityMissing
        | FetchDecodeFailure::NonTransactionalControlIdentity
        | FetchDecodeFailure::InvalidAbortedTransaction { .. }
        | FetchDecodeFailure::MissingLastStableOffset
        | FetchDecodeFailure::AbortedTransactionAtOrAfterLastStableOffset { .. }
        | FetchDecodeFailure::BatchAtOrAfterLastStableOffset { .. }
        | FetchDecodeFailure::ControlRecord(_)
        | FetchDecodeFailure::UnsupportedControlRecordType { .. }
        | FetchDecodeFailure::RecordOffsetOutsideBatch { .. }
        | FetchDecodeFailure::RecordOffsetsNotIncreasing { .. } => {
            FetchOutcomeFailureClass::InvalidResponse
        }
        FetchDecodeFailure::UnsupportedRecordBatchDecode { .. } => {
            FetchOutcomeFailureClass::Compatibility
        }
    }
}
