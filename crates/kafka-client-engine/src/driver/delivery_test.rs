//! Delivery-certainty translation scenarios at the client-driver boundary.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{CallFailure, Delivery, RequestError};

use super::delivery::request_failure_delivery;

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
