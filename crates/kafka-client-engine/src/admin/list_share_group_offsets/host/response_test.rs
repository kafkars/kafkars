//! Exhaustive host mapping for exact API-90 protocol failures.

use kafka_client_core::{DeliveryStatus, ListShareGroupOffsetsInput};

use crate::protocol::admin::list_share_group_offsets::ListShareGroupOffsetsProtocolFailure;

use super::response::protocol_failure;

#[test]
fn version_failures_preserve_possibly_sent_compatibility() {
    for failure in [
        ListShareGroupOffsetsProtocolFailure::MissingSelectedVersion,
        ListShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual: 2 },
    ] {
        assert_eq!(
            protocol_failure(failure),
            ListShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        );
    }
}

#[test]
fn every_capacity_failure_maps_to_response_too_large() {
    for failure in [
        ListShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
            required: 2,
            max: 1,
        },
        ListShareGroupOffsetsProtocolFailure::RetainedBytes {
            required: 2,
            limit: 1,
        },
        ListShareGroupOffsetsProtocolFailure::Allocation {
            field: "partition results",
            requested: 2,
        },
        ListShareGroupOffsetsProtocolFailure::TooManyTopics { actual: 2, max: 1 },
        ListShareGroupOffsetsProtocolFailure::TooManyPartitions { actual: 2, max: 1 },
        ListShareGroupOffsetsProtocolFailure::TopicNameTooLong { actual: 2, max: 1 },
        ListShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded {
            required: 2,
            max: 1,
        },
    ] {
        assert_eq!(
            protocol_failure(failure),
            ListShareGroupOffsetsInput::ResponseTooLarge
        );
    }
}

#[test]
fn every_malformed_or_uncorrelatable_shape_maps_to_invalid_response() {
    for failure in [
        ListShareGroupOffsetsProtocolFailure::NegativeThrottleTime { actual: -1 },
        ListShareGroupOffsetsProtocolFailure::GroupCount { actual: 0 },
        ListShareGroupOffsetsProtocolFailure::UnexpectedGroup,
        ListShareGroupOffsetsProtocolFailure::EmptyTopicName,
        ListShareGroupOffsetsProtocolFailure::EmptyTopicPartitions,
        ListShareGroupOffsetsProtocolFailure::DuplicateTopic,
        ListShareGroupOffsetsProtocolFailure::NegativePartition { actual: -1 },
        ListShareGroupOffsetsProtocolFailure::DuplicatePartition { actual: 1 },
        ListShareGroupOffsetsProtocolFailure::MissingPartition,
        ListShareGroupOffsetsProtocolFailure::UnexpectedPartition,
        ListShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess,
        ListShareGroupOffsetsProtocolFailure::ZeroTopicId,
        ListShareGroupOffsetsProtocolFailure::InvalidStartOffset { actual: -2 },
        ListShareGroupOffsetsProtocolFailure::InvalidLeaderEpoch { actual: -2 },
        ListShareGroupOffsetsProtocolFailure::InvalidV0Lag { actual: 0 },
        ListShareGroupOffsetsProtocolFailure::InvalidLag { actual: -2 },
    ] {
        assert_eq!(
            protocol_failure(failure),
            ListShareGroupOffsetsInput::InvalidResponse
        );
    }
}
