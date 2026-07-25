//! Delivery-certainty translation scenarios at the client-driver boundary.

use kafka_client_core::{DeliveryStatus, ProducerAttemptFailureKind};
use kafka_driver::{
    ApiKey, ApiVersion, CallFailure, ConnectionCloseReason, Delivery, NegotiationFailure,
    RequestError, TransportFailure,
};

use super::delivery::{request_failure_delivery, request_failure_kind};

#[test]
fn local_driver_failures_remain_definitely_not_sent() {
    assert_eq!(
        request_failure_delivery(&RequestError::RouteUnavailable),
        DeliveryStatus::NotSent
    );
}

#[test]
fn version_floor_and_bounds_failures_are_definitely_unsent_and_permanent() {
    let api_key = ApiKey::new(3);
    for error in [
        RequestError::VersionFloorUnavailable {
            api_key,
            minimum: ApiVersion::new(4),
            negotiated_maximum: ApiVersion::new(3),
        },
        RequestError::VersionBoundsInvalid {
            api_key,
            minimum: ApiVersion::new(4),
            maximum: ApiVersion::new(3),
        },
    ] {
        assert_eq!(request_failure_delivery(&error), DeliveryStatus::NotSent);
        assert_eq!(
            request_failure_kind(&error),
            ProducerAttemptFailureKind::Permanent
        );
    }
}

#[test]
fn driver_owned_possibly_sent_evidence_is_preserved() {
    let error = RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: kafka_driver::ConnectionCloseReason::Requested,
        },
        delivery: Delivery::PossiblySent,
    };

    assert_eq!(
        request_failure_delivery(&error),
        DeliveryStatus::PossiblySent
    );
}

#[test]
fn bounded_local_and_route_failures_keep_distinct_structure() {
    assert_eq!(
        request_failure_kind(&RequestError::ResponseCapacityReached { limit: 3 }),
        ProducerAttemptFailureKind::LocalCapacity
    );
    assert_eq!(
        request_failure_kind(&RequestError::RouteUnavailable),
        ProducerAttemptFailureKind::RouteUnavailable
    );
    assert_eq!(
        request_failure_kind(&RequestError::CoordinatorCapacityReached { limit: 2 }),
        ProducerAttemptFailureKind::Permanent
    );
}

#[test]
fn only_recoverable_connection_shapes_are_transient() {
    for failure in [
        CallFailure::NotReady,
        CallFailure::Draining,
        CallFailure::Closed,
        CallFailure::ConnectionClosed {
            reason: ConnectionCloseReason::OpenFailed(TransportFailure::Refused),
        },
        CallFailure::ConnectionClosed {
            reason: ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Timeout),
        },
    ] {
        let error = RequestError::Rejected {
            failure,
            delivery: Delivery::NotSent,
        };
        assert_eq!(
            request_failure_kind(&error),
            ProducerAttemptFailureKind::ConnectionUnavailable
        );
    }
}

#[test]
fn possibly_sent_transport_loss_is_not_structurally_retryable() {
    let error = RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: ConnectionCloseReason::TransportLost(TransportFailure::Reset),
        },
        delivery: Delivery::PossiblySent,
    };

    assert_eq!(
        request_failure_kind(&error),
        ProducerAttemptFailureKind::Permanent
    );
    assert_eq!(
        request_failure_delivery(&error),
        DeliveryStatus::PossiblySent
    );
}
