//! Closed driver-failure classification for classic-group `LeaveGroup`.

use kafka_driver::{
    CallFailure, ConnectionCloseReason, NegotiationFailure, RequestError, ResponseCloseReason,
};
use kafka_wire_core::{DecodeError, EncodeError};

/// Driver and protocol failure category before consumer close policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicGroupLeaveDriverFailureKind {
    DeadlineElapsed,
    Authentication,
    Compatibility,
    DriverRejected,
    Transport,
    InvalidResponse,
    ResponseTooLarge,
}

#[allow(
    clippy::match_same_arms,
    unreachable_patterns,
    reason = "the published driver RC exposes non-exhaustive failure vocabularies while the reviewed path dependency is exhaustive"
)]
pub(super) fn classify_request_error(error: &RequestError) -> ClassicGroupLeaveDriverFailureKind {
    match error {
        RequestError::Encode(error) => classify_encode(error),
        RequestError::Decode(error) => classify_decode(error),
        RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ClassicGroupLeaveDriverFailureKind::Compatibility
        }
        RequestError::ResponseCapacityReached { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. } => {
            ClassicGroupLeaveDriverFailureKind::DriverRejected
        }
        RequestError::RouteUnavailable | RequestError::NameResolutionFailed { .. } => {
            ClassicGroupLeaveDriverFailureKind::Transport
        }
        RequestError::Rejected { failure, .. } => classify_call(*failure),
        RequestError::ConnectionClosed(reason) => classify_response_close(*reason),
        _ => ClassicGroupLeaveDriverFailureKind::DriverRejected,
    }
}

fn classify_encode(error: &EncodeError) -> ClassicGroupLeaveDriverFailureKind {
    match error {
        EncodeError::UnsupportedVersion { .. }
        | EncodeError::FieldNotRepresentable { .. }
        | EncodeError::NullNotAllowed { .. }
        | EncodeError::TaggedFieldsNotRepresentable { .. } => {
            ClassicGroupLeaveDriverFailureKind::Compatibility
        }
        _ => ClassicGroupLeaveDriverFailureKind::DriverRejected,
    }
}

fn classify_decode(error: &DecodeError) -> ClassicGroupLeaveDriverFailureKind {
    match error {
        DecodeError::UnsupportedVersion { .. } => ClassicGroupLeaveDriverFailureKind::Compatibility,
        DecodeError::LimitExceeded { .. } | DecodeError::LengthOverflow { .. } => {
            ClassicGroupLeaveDriverFailureKind::ResponseTooLarge
        }
        _ => ClassicGroupLeaveDriverFailureKind::InvalidResponse,
    }
}

const fn classify_call(failure: CallFailure) -> ClassicGroupLeaveDriverFailureKind {
    match failure {
        CallFailure::DeadlineExceeded => ClassicGroupLeaveDriverFailureKind::DeadlineElapsed,
        CallFailure::CapacityReached { .. }
        | CallFailure::CorrelationSpaceExhausted
        | CallFailure::LocallyRejected => ClassicGroupLeaveDriverFailureKind::DriverRejected,
        CallFailure::CorrelationMismatch { .. } => {
            ClassicGroupLeaveDriverFailureKind::InvalidResponse
        }
        CallFailure::ConnectionClosed { reason } => classify_connection_close(reason),
        CallFailure::NotReady | CallFailure::Draining | CallFailure::Closed => {
            ClassicGroupLeaveDriverFailureKind::Transport
        }
    }
}

const fn classify_connection_close(
    reason: ConnectionCloseReason,
) -> ClassicGroupLeaveDriverFailureKind {
    match reason {
        ConnectionCloseReason::AuthenticationFailed(_) => {
            ClassicGroupLeaveDriverFailureKind::Authentication
        }
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed)
        | ConnectionCloseReason::CorrelationMismatch { .. }
        | ConnectionCloseReason::UnexpectedResponse
        | ConnectionCloseReason::MalformedResponse => {
            ClassicGroupLeaveDriverFailureKind::InvalidResponse
        }
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Capacity) => {
            ClassicGroupLeaveDriverFailureKind::ResponseTooLarge
        }
        _ => ClassicGroupLeaveDriverFailureKind::Transport,
    }
}

#[allow(
    clippy::match_same_arms,
    unreachable_patterns,
    reason = "an unknown close reason cannot safely acquire transport-retry semantics"
)]
const fn classify_response_close(
    reason: ResponseCloseReason,
) -> ClassicGroupLeaveDriverFailureKind {
    match reason {
        ResponseCloseReason::ProtocolFault => ClassicGroupLeaveDriverFailureKind::InvalidResponse,
        ResponseCloseReason::TransportClosed | ResponseCloseReason::Shutdown => {
            ClassicGroupLeaveDriverFailureKind::Transport
        }
        _ => ClassicGroupLeaveDriverFailureKind::InvalidResponse,
    }
}
