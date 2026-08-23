//! Stable driver-failure facts for broker-local acknowledgement requests.

use kafka_driver::{
    CallFailure, ConnectionCloseReason, NegotiationFailure, RequestError, ResponseCloseReason,
};
use kafka_wire_core::{DecodeError, EncodeError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgeDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    DriverRejected,
    Transport,
    InvalidResponse,
    ResponseTooLarge,
}

pub(super) fn classify_share_acknowledge_request_error(
    error: &RequestError,
) -> ShareAcknowledgeDriverFailureKind {
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
        | RequestError::VersionBoundsInvalid { .. } => {
            ShareAcknowledgeDriverFailureKind::Compatibility
        }
        RequestError::ResponseCapacityReached { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. } => {
            ShareAcknowledgeDriverFailureKind::DriverRejected
        }
        RequestError::RouteUnavailable | RequestError::NameResolutionFailed { .. } => {
            ShareAcknowledgeDriverFailureKind::Transport
        }
        RequestError::Rejected { failure, .. } => classify_call(*failure),
        RequestError::ConnectionClosed(reason) => classify_response_close(*reason),
        _ => ShareAcknowledgeDriverFailureKind::DriverRejected,
    }
}

fn classify_encode(error: &EncodeError) -> ShareAcknowledgeDriverFailureKind {
    match error {
        EncodeError::UnsupportedVersion { .. }
        | EncodeError::FieldNotRepresentable { .. }
        | EncodeError::NullNotAllowed { .. }
        | EncodeError::TaggedFieldsNotRepresentable { .. } => {
            ShareAcknowledgeDriverFailureKind::Compatibility
        }
        _ => ShareAcknowledgeDriverFailureKind::DriverRejected,
    }
}

fn classify_decode(error: &DecodeError) -> ShareAcknowledgeDriverFailureKind {
    match error {
        DecodeError::UnsupportedVersion { .. } => ShareAcknowledgeDriverFailureKind::Compatibility,
        DecodeError::LimitExceeded { .. } | DecodeError::LengthOverflow { .. } => {
            ShareAcknowledgeDriverFailureKind::ResponseTooLarge
        }
        _ => ShareAcknowledgeDriverFailureKind::InvalidResponse,
    }
}

const fn classify_call(failure: CallFailure) -> ShareAcknowledgeDriverFailureKind {
    match failure {
        CallFailure::DeadlineExceeded => ShareAcknowledgeDriverFailureKind::DeadlineElapsed,
        CallFailure::CapacityReached { .. }
        | CallFailure::CorrelationSpaceExhausted
        | CallFailure::LocallyRejected => ShareAcknowledgeDriverFailureKind::DriverRejected,
        CallFailure::CorrelationMismatch { .. } => {
            ShareAcknowledgeDriverFailureKind::InvalidResponse
        }
        CallFailure::ConnectionClosed { reason } => classify_connection_close(reason),
        CallFailure::NotReady | CallFailure::Draining | CallFailure::Closed => {
            ShareAcknowledgeDriverFailureKind::Transport
        }
    }
}

const fn classify_connection_close(
    reason: ConnectionCloseReason,
) -> ShareAcknowledgeDriverFailureKind {
    match reason {
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed)
        | ConnectionCloseReason::CorrelationMismatch { .. }
        | ConnectionCloseReason::UnexpectedResponse
        | ConnectionCloseReason::MalformedResponse => {
            ShareAcknowledgeDriverFailureKind::InvalidResponse
        }
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Capacity) => {
            ShareAcknowledgeDriverFailureKind::ResponseTooLarge
        }
        _ => ShareAcknowledgeDriverFailureKind::Transport,
    }
}

#[allow(
    clippy::match_same_arms,
    unreachable_patterns,
    reason = "an unknown close reason cannot gain transport-retry semantics"
)]
const fn classify_response_close(reason: ResponseCloseReason) -> ShareAcknowledgeDriverFailureKind {
    match reason {
        ResponseCloseReason::ProtocolFault => ShareAcknowledgeDriverFailureKind::InvalidResponse,
        ResponseCloseReason::TransportClosed | ResponseCloseReason::Shutdown => {
            ShareAcknowledgeDriverFailureKind::Transport
        }
        _ => ShareAcknowledgeDriverFailureKind::InvalidResponse,
    }
}
