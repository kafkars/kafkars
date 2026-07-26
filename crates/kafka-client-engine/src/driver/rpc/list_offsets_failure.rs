//! Closed translation from existing `ListOffsets` driver facts into core semantics.

use kafka_client_core::PositionResolutionAttemptFailure;
use kafka_driver::{
    CallFailure, ConnectionCloseReason, NegotiationFailure, RequestError, ResponseCloseReason,
};
use kafka_wire_core::{DecodeError, EncodeError};

use crate::protocol::consumer::ListOffsetsResponseFailure;

pub(super) fn classify_request_error(failure: &RequestError) -> PositionResolutionAttemptFailure {
    match failure {
        RequestError::Encode(failure) => classify_encode_error(failure),
        RequestError::Decode(failure) => classify_decode_error(failure),
        RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            PositionResolutionAttemptFailure::Compatibility
        }
        RequestError::ResponseCapacityReached { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. } => {
            PositionResolutionAttemptFailure::DriverRejected
        }
        RequestError::RouteUnavailable | RequestError::NameResolutionFailed { .. } => {
            PositionResolutionAttemptFailure::Transport
        }
        RequestError::Rejected { failure, .. } => classify_call_failure(*failure),
        RequestError::ConnectionClosed(reason) => classify_response_close(*reason),
    }
}

pub(super) const fn classify_response_failure(
    failure: ListOffsetsResponseFailure,
) -> PositionResolutionAttemptFailure {
    match failure {
        ListOffsetsResponseFailure::UnsupportedApiVersion { .. } => {
            PositionResolutionAttemptFailure::Compatibility
        }
        ListOffsetsResponseFailure::NegativeThrottleTime { .. }
        | ListOffsetsResponseFailure::MissingTopic
        | ListOffsetsResponseFailure::DuplicateTopic
        | ListOffsetsResponseFailure::UnexpectedTopic
        | ListOffsetsResponseFailure::MissingPartition
        | ListOffsetsResponseFailure::DuplicatePartition
        | ListOffsetsResponseFailure::InvalidPartitionIndex { .. }
        | ListOffsetsResponseFailure::UnexpectedPartition { .. }
        | ListOffsetsResponseFailure::RequestedPartitionOutOfRange { .. }
        | ListOffsetsResponseFailure::InvalidOffset { .. }
        | ListOffsetsResponseFailure::InvalidTimestamp { .. }
        | ListOffsetsResponseFailure::InvalidLeaderEpoch { .. } => {
            PositionResolutionAttemptFailure::InvalidResponse
        }
    }
}

fn classify_encode_error(failure: &EncodeError) -> PositionResolutionAttemptFailure {
    #[allow(
        clippy::match_same_arms,
        reason = "named current variants remain audited separately from the non-exhaustive fallback"
    )]
    match failure {
        EncodeError::UnsupportedVersion { .. }
        | EncodeError::FieldNotRepresentable { .. }
        | EncodeError::NullNotAllowed { .. }
        | EncodeError::TaggedFieldsNotRepresentable { .. } => {
            PositionResolutionAttemptFailure::Compatibility
        }
        EncodeError::LengthOverflow { .. }
        | EncodeError::KnownTagConflict { .. }
        | EncodeError::UnclaimedKnownTag { .. }
        | EncodeError::KnownTagCapacityExceeded { .. }
        | EncodeError::TaggedFieldsInvalid(_)
        | EncodeError::SizeMismatch { .. }
        | EncodeError::FrameTooLarge { .. }
        | EncodeError::FrameLimitExceeded { .. } => {
            PositionResolutionAttemptFailure::DriverRejected
        }
        _ => PositionResolutionAttemptFailure::DriverRejected,
    }
}

fn classify_decode_error(failure: &DecodeError) -> PositionResolutionAttemptFailure {
    #[allow(
        clippy::match_same_arms,
        reason = "named current variants remain audited separately from the non-exhaustive fallback"
    )]
    match failure {
        DecodeError::UnsupportedVersion { .. } => PositionResolutionAttemptFailure::Compatibility,
        DecodeError::LimitExceeded { .. } | DecodeError::LengthOverflow { .. } => {
            PositionResolutionAttemptFailure::ResponseTooLarge
        }
        DecodeError::UnexpectedEnd { .. }
        | DecodeError::InvalidBoolean { .. }
        | DecodeError::NegativeLength { .. }
        | DecodeError::NullNotAllowed { .. }
        | DecodeError::CountExceedsFrame { .. }
        | DecodeError::InvalidUtf8 { .. }
        | DecodeError::MalformedVarint { .. }
        | DecodeError::TaggedFieldOrder { .. }
        | DecodeError::TaggedFieldSize { .. }
        | DecodeError::TrailingBytes { .. } => PositionResolutionAttemptFailure::InvalidResponse,
        _ => PositionResolutionAttemptFailure::InvalidResponse,
    }
}

const fn classify_call_failure(failure: CallFailure) -> PositionResolutionAttemptFailure {
    match failure {
        CallFailure::DeadlineExceeded => PositionResolutionAttemptFailure::DeadlineElapsed,
        CallFailure::CapacityReached { .. }
        | CallFailure::CorrelationSpaceExhausted
        | CallFailure::LocallyRejected => PositionResolutionAttemptFailure::DriverRejected,
        CallFailure::CorrelationMismatch { .. } => {
            PositionResolutionAttemptFailure::InvalidResponse
        }
        CallFailure::ConnectionClosed { reason } => classify_connection_close(reason),
        CallFailure::NotReady | CallFailure::Draining | CallFailure::Closed => {
            PositionResolutionAttemptFailure::Transport
        }
    }
}

const fn classify_connection_close(
    reason: ConnectionCloseReason,
) -> PositionResolutionAttemptFailure {
    match reason {
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed)
        | ConnectionCloseReason::CorrelationMismatch { .. }
        | ConnectionCloseReason::UnexpectedResponse
        | ConnectionCloseReason::MalformedResponse => {
            PositionResolutionAttemptFailure::InvalidResponse
        }
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Capacity) => {
            PositionResolutionAttemptFailure::ResponseTooLarge
        }
        ConnectionCloseReason::Drained
        | ConnectionCloseReason::Requested
        | ConnectionCloseReason::OpenFailed(_)
        | ConnectionCloseReason::TransportLost(_)
        | ConnectionCloseReason::NegotiationFailed(
            NegotiationFailure::Broker | NegotiationFailure::Timeout,
        )
        | ConnectionCloseReason::AuthenticationFailed(_)
        | ConnectionCloseReason::DeadlineExceeded { .. } => {
            PositionResolutionAttemptFailure::Transport
        }
    }
}

const fn classify_response_close(reason: ResponseCloseReason) -> PositionResolutionAttemptFailure {
    match reason {
        ResponseCloseReason::ProtocolFault => PositionResolutionAttemptFailure::InvalidResponse,
        ResponseCloseReason::TransportClosed | ResponseCloseReason::Shutdown => {
            PositionResolutionAttemptFailure::Transport
        }
    }
}
