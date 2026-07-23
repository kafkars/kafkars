//! Tests for the facade-owned stable error vocabulary.

use crate::{DeliveryStatus, ErrorKind, KafkaError};

#[test]
fn producer_delivery_certainty_round_trips_through_public_error() {
    for status in [DeliveryStatus::NotSent, DeliveryStatus::PossiblySent] {
        let error =
            KafkaError::new(ErrorKind::Timeout, "delivery timed out").with_delivery_status(status);

        assert_eq!(error.delivery_status(), Some(status));
    }
}

#[test]
fn non_producer_error_has_no_delivery_certainty() {
    let error = KafkaError::new(ErrorKind::Configuration, "invalid configuration");

    assert_eq!(error.delivery_status(), None);
    assert_eq!(error.broker_code(), None);
}

#[test]
fn broker_code_preserves_the_signed_protocol_domain() {
    let error =
        KafkaError::new(ErrorKind::Broker, "unknown broker error").with_broker_code(Some(-123));

    assert_eq!(error.broker_code(), Some(-123));
}
