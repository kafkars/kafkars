//! Evidence for core-owned Produce error classification and unknown preservation.

use crate::{DeliveryStatus, ProducerFailure, ProducerFailureKind};

#[test]
fn produce_error_codes_are_classified_in_core() {
    assert_eq!(
        ProducerFailure::broker(6, DeliveryStatus::PossiblySent).kind(),
        ProducerFailureKind::Routing
    );
    assert_eq!(
        ProducerFailure::broker(19, DeliveryStatus::PossiblySent).kind(),
        ProducerFailureKind::BrokerRetriable
    );
    assert_eq!(
        ProducerFailure::broker(29, DeliveryStatus::PossiblySent).kind(),
        ProducerFailureKind::AccessRejected
    );
    assert_eq!(
        ProducerFailure::broker(10, DeliveryStatus::PossiblySent).kind(),
        ProducerFailureKind::InvalidRecord
    );
    assert_eq!(
        ProducerFailure::broker(47, DeliveryStatus::PossiblySent).kind(),
        ProducerFailureKind::ProducerIdentity
    );
    assert_eq!(
        ProducerFailure::broker(2, DeliveryStatus::PossiblySent).kind(),
        ProducerFailureKind::BrokerRetriable
    );
    assert_eq!(
        ProducerFailure::broker(35, DeliveryStatus::PossiblySent).kind(),
        ProducerFailureKind::Compatibility
    );
}

#[test]
fn unknown_broker_code_is_preserved_exactly() {
    let failure = ProducerFailure::broker(32_000, DeliveryStatus::PossiblySent);
    assert_eq!(failure.kind(), ProducerFailureKind::UnknownBroker);
    assert_eq!(failure.broker_code(), Some(32_000));
}
