//! Stable classification of driver failures for one KIP-848 heartbeat call.

use kafka_driver::{
    CallFailure, ConnectionCloseReason, NegotiationFailure, RequestError, ResponseCloseReason,
};
use kafka_wire_core::{DecodeError, EncodeError};

/// Driver and protocol failure before deterministic membership policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupHeartbeatDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    DriverRejected,
    Transport,
    InvalidResponse,
    ResponseTooLarge,
}

pub(super) fn classify_consumer_group_heartbeat_request_error(
    error: &RequestError,
) -> ConsumerGroupHeartbeatDriverFailureKind {
    match error {
        RequestError::Encode(error) => classify_encode(error),
        RequestError::Decode(error) => classify_decode(error),
        RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ConsumerGroupHeartbeatDriverFailureKind::Compatibility
        }
        RequestError::ResponseCapacityReached { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. } => {
            ConsumerGroupHeartbeatDriverFailureKind::DriverRejected
        }
        RequestError::RouteUnavailable | RequestError::NameResolutionFailed { .. } => {
            ConsumerGroupHeartbeatDriverFailureKind::Transport
        }
        RequestError::Rejected { failure, .. } => classify_call(*failure),
        RequestError::ConnectionClosed(reason) => classify_response_close(*reason),
    }
}

fn classify_encode(error: &EncodeError) -> ConsumerGroupHeartbeatDriverFailureKind {
    match error {
        EncodeError::UnsupportedVersion { .. }
        | EncodeError::FieldNotRepresentable { .. }
        | EncodeError::NullNotAllowed { .. }
        | EncodeError::TaggedFieldsNotRepresentable { .. } => {
            ConsumerGroupHeartbeatDriverFailureKind::Compatibility
        }
        _ => ConsumerGroupHeartbeatDriverFailureKind::DriverRejected,
    }
}

fn classify_decode(error: &DecodeError) -> ConsumerGroupHeartbeatDriverFailureKind {
    match error {
        DecodeError::UnsupportedVersion { .. } => {
            ConsumerGroupHeartbeatDriverFailureKind::Compatibility
        }
        DecodeError::LimitExceeded { .. } | DecodeError::LengthOverflow { .. } => {
            ConsumerGroupHeartbeatDriverFailureKind::ResponseTooLarge
        }
        _ => ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse,
    }
}

const fn classify_call(failure: CallFailure) -> ConsumerGroupHeartbeatDriverFailureKind {
    match failure {
        CallFailure::DeadlineExceeded => ConsumerGroupHeartbeatDriverFailureKind::DeadlineElapsed,
        CallFailure::CapacityReached { .. }
        | CallFailure::CorrelationSpaceExhausted
        | CallFailure::LocallyRejected => ConsumerGroupHeartbeatDriverFailureKind::DriverRejected,
        CallFailure::CorrelationMismatch { .. } => {
            ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse
        }
        CallFailure::ConnectionClosed { reason } => classify_connection_close(reason),
        CallFailure::NotReady | CallFailure::Draining | CallFailure::Closed => {
            ConsumerGroupHeartbeatDriverFailureKind::Transport
        }
    }
}

const fn classify_connection_close(
    reason: ConnectionCloseReason,
) -> ConsumerGroupHeartbeatDriverFailureKind {
    match reason {
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed)
        | ConnectionCloseReason::CorrelationMismatch { .. }
        | ConnectionCloseReason::UnexpectedResponse
        | ConnectionCloseReason::MalformedResponse => {
            ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse
        }
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Capacity) => {
            ConsumerGroupHeartbeatDriverFailureKind::ResponseTooLarge
        }
        _ => ConsumerGroupHeartbeatDriverFailureKind::Transport,
    }
}

const fn classify_response_close(
    reason: ResponseCloseReason,
) -> ConsumerGroupHeartbeatDriverFailureKind {
    match reason {
        ResponseCloseReason::ProtocolFault => {
            ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse
        }
        ResponseCloseReason::TransportClosed | ResponseCloseReason::Shutdown => {
            ConsumerGroupHeartbeatDriverFailureKind::Transport
        }
    }
}
