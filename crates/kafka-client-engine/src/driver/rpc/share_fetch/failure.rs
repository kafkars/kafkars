//! Stable driver-failure facts for broker-local `ShareFetch` sessions.

use kafka_driver::{
    CallFailure, ConnectionCloseReason, NegotiationFailure, RequestError, ResponseCloseReason,
};
use kafka_wire_core::{DecodeError, EncodeError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    DriverRejected,
    Transport,
    InvalidResponse,
    ResponseTooLarge,
}

pub(super) fn classify_share_fetch_request_error(
    error: &RequestError,
) -> ShareFetchDriverFailureKind {
    #[allow(
        clippy::match_same_arms,
        unreachable_patterns,
        reason = "the published driver request error is non-exhaustive"
    )]
    match error {
        RequestError::Encode(error) => classify_encode(error),
        RequestError::Decode(error) => classify_decode(error),
        RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => ShareFetchDriverFailureKind::Compatibility,
        RequestError::ResponseCapacityReached { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. } => {
            ShareFetchDriverFailureKind::DriverRejected
        }
        RequestError::RouteUnavailable | RequestError::NameResolutionFailed { .. } => {
            ShareFetchDriverFailureKind::Transport
        }
        RequestError::Rejected { failure, .. } => classify_call(*failure),
        RequestError::ConnectionClosed(reason) => classify_response_close(*reason),
        _ => ShareFetchDriverFailureKind::DriverRejected,
    }
}

fn classify_encode(error: &EncodeError) -> ShareFetchDriverFailureKind {
    match error {
        EncodeError::UnsupportedVersion { .. }
        | EncodeError::FieldNotRepresentable { .. }
        | EncodeError::NullNotAllowed { .. }
        | EncodeError::TaggedFieldsNotRepresentable { .. } => {
            ShareFetchDriverFailureKind::Compatibility
        }
        _ => ShareFetchDriverFailureKind::DriverRejected,
    }
}

fn classify_decode(error: &DecodeError) -> ShareFetchDriverFailureKind {
    match error {
        DecodeError::UnsupportedVersion { .. } => ShareFetchDriverFailureKind::Compatibility,
        DecodeError::LimitExceeded { .. } | DecodeError::LengthOverflow { .. } => {
            ShareFetchDriverFailureKind::ResponseTooLarge
        }
        _ => ShareFetchDriverFailureKind::InvalidResponse,
    }
}

const fn classify_call(failure: CallFailure) -> ShareFetchDriverFailureKind {
    match failure {
        CallFailure::DeadlineExceeded => ShareFetchDriverFailureKind::DeadlineElapsed,
        CallFailure::CapacityReached { .. }
        | CallFailure::CorrelationSpaceExhausted
        | CallFailure::LocallyRejected => ShareFetchDriverFailureKind::DriverRejected,
        CallFailure::CorrelationMismatch { .. } => ShareFetchDriverFailureKind::InvalidResponse,
        CallFailure::ConnectionClosed { reason } => classify_connection_close(reason),
        CallFailure::NotReady | CallFailure::Draining | CallFailure::Closed => {
            ShareFetchDriverFailureKind::Transport
        }
    }
}

const fn classify_connection_close(reason: ConnectionCloseReason) -> ShareFetchDriverFailureKind {
    match reason {
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed)
        | ConnectionCloseReason::CorrelationMismatch { .. }
        | ConnectionCloseReason::UnexpectedResponse
        | ConnectionCloseReason::MalformedResponse => ShareFetchDriverFailureKind::InvalidResponse,
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Capacity) => {
            ShareFetchDriverFailureKind::ResponseTooLarge
        }
        _ => ShareFetchDriverFailureKind::Transport,
    }
}

#[allow(
    clippy::match_same_arms,
    unreachable_patterns,
    reason = "an unknown close reason cannot gain transport-retry semantics"
)]
const fn classify_response_close(reason: ResponseCloseReason) -> ShareFetchDriverFailureKind {
    match reason {
        ResponseCloseReason::ProtocolFault => ShareFetchDriverFailureKind::InvalidResponse,
        ResponseCloseReason::TransportClosed | ResponseCloseReason::Shutdown => {
            ShareFetchDriverFailureKind::Transport
        }
        _ => ShareFetchDriverFailureKind::InvalidResponse,
    }
}
