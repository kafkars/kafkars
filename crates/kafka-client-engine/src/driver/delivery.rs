//! Authoritative translation of driver-owned terminal delivery certainty.

use kafka_client_core::{DeliveryStatus, ProducerAttemptFailureKind};
use kafka_driver::{
    CallFailure, ConnectionCloseReason, Delivery, NegotiationFailure, RequestError,
};

/// Translates one terminal driver failure without reclassifying its variant.
pub(crate) const fn request_failure_delivery(error: &RequestError) -> DeliveryStatus {
    delivery_status(error.delivery())
}

/// Normalizes driver structure without changing authoritative delivery certainty.
#[allow(
    clippy::match_same_arms,
    unreachable_patterns,
    reason = "the published driver RC exposes a non-exhaustive request error while the reviewed path dependency is exhaustive"
)]
pub(crate) const fn request_failure_kind(error: &RequestError) -> ProducerAttemptFailureKind {
    match error {
        RequestError::ResponseCapacityReached { .. }
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. } => {
            ProducerAttemptFailureKind::LocalCapacity
        }
        RequestError::RouteUnavailable => ProducerAttemptFailureKind::RouteUnavailable,
        RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. } => ProducerAttemptFailureKind::Compatibility,
        RequestError::Rejected { failure, .. } => rejected_failure_kind(*failure),
        // The public driver surface does not yet expose the sanitized DNS
        // variant, so the engine cannot safely identify temporary failures.
        RequestError::NameResolutionFailed { .. }
        | RequestError::Encode(_)
        | RequestError::Decode(_)
        | RequestError::VersionBoundsInvalid { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::ConnectionClosed(_) => ProducerAttemptFailureKind::Permanent,
        _ => ProducerAttemptFailureKind::Permanent,
    }
}

const fn rejected_failure_kind(failure: CallFailure) -> ProducerAttemptFailureKind {
    match failure {
        CallFailure::CapacityReached { .. }
        | CallFailure::CorrelationSpaceExhausted
        | CallFailure::LocallyRejected => ProducerAttemptFailureKind::LocalCapacity,
        CallFailure::NotReady | CallFailure::Draining | CallFailure::Closed => {
            ProducerAttemptFailureKind::ConnectionUnavailable
        }
        CallFailure::ConnectionClosed {
            reason:
                ConnectionCloseReason::OpenFailed(_)
                | ConnectionCloseReason::TransportLost(_)
                | ConnectionCloseReason::NegotiationFailed(
                    NegotiationFailure::Broker
                    | NegotiationFailure::Capacity
                    | NegotiationFailure::Timeout,
                ),
        } => ProducerAttemptFailureKind::ConnectionUnavailable,
        CallFailure::DeadlineExceeded
        | CallFailure::ConnectionClosed { .. }
        | CallFailure::CorrelationMismatch { .. } => ProducerAttemptFailureKind::Permanent,
    }
}

const fn delivery_status(delivery: Delivery) -> DeliveryStatus {
    match delivery {
        Delivery::NotSent => DeliveryStatus::NotSent,
        Delivery::PossiblySent => DeliveryStatus::PossiblySent,
    }
}
