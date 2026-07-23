//! Delivery-certainty translation scenarios at the client-driver boundary.

use kafka_client_core::{DeliveryStatus, ProducerAttemptFailureKind};
use kafka_driver::{
    CallFailure, ConnectionCloseReason, Delivery, NegotiationFailure, RequestError,
    TransportFailure,
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
