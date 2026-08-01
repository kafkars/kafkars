//! Evidence for core-owned Produce failure policy and unknown preservation.

use core::num::NonZeroI16;

use crate::{
    DeliveryStatus, ProducerBrokerFailure, ProducerBrokerFailureKind, ProducerFailure,
    ProducerFailureKind,
};

#[test]
fn semantic_broker_facts_map_to_core_owned_terminal_policy() {
    for (broker, terminal) in [
        (
            ProducerBrokerFailureKind::Routing,
            ProducerFailureKind::Routing,
        ),
        (
            ProducerBrokerFailureKind::Retriable,
            ProducerFailureKind::BrokerRetriable,
        ),
        (
            ProducerBrokerFailureKind::AccessRejected,
            ProducerFailureKind::AccessRejected,
        ),
        (
            ProducerBrokerFailureKind::InvalidRecord,
            ProducerFailureKind::InvalidRecord,
        ),
        (
            ProducerBrokerFailureKind::Compatibility,
            ProducerFailureKind::Compatibility,
        ),
        (
            ProducerBrokerFailureKind::ProducerIdentity,
            ProducerFailureKind::ProducerIdentity,
        ),
        (
            ProducerBrokerFailureKind::ProducerFenced,
            ProducerFailureKind::ProducerFenced,
        ),
        (
            ProducerBrokerFailureKind::Unknown,
            ProducerFailureKind::UnknownBroker,
        ),
    ] {
        assert_eq!(
            ProducerFailure::broker(fact(broker, -123), DeliveryStatus::PossiblySent).kind(),
            terminal
        );
    }
}

#[test]
fn unknown_broker_code_is_preserved_exactly() {
    let failure = ProducerFailure::broker(
        fact(ProducerBrokerFailureKind::Unknown, -123),
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(failure.kind(), ProducerFailureKind::UnknownBroker);
    assert_eq!(failure.broker_code(), Some(-123));
}

#[test]
fn compatibility_attempt_preserves_semantics_and_authoritative_certainty() {
    for delivery in [DeliveryStatus::NotSent, DeliveryStatus::PossiblySent] {
        let failure =
            ProducerFailure::attempt(crate::ProducerAttemptFailureKind::Compatibility, delivery);
        assert_eq!(failure.kind(), ProducerFailureKind::Compatibility);
        assert_eq!(failure.delivery(), delivery);
        assert_eq!(failure.broker_code(), None);
    }
}

#[test]
fn invalid_response_attempt_preserves_semantics_and_certainty() {
    let failure = ProducerFailure::attempt(
        crate::ProducerAttemptFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(failure.kind(), ProducerFailureKind::InvalidResponse);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(failure.broker_code(), None);
}

#[test]
fn execution_loss_preserves_the_reported_conservative_certainty() {
    for status in [DeliveryStatus::NotSent, DeliveryStatus::PossiblySent] {
        let failure = ProducerFailure::execution_unavailable(status);
        assert_eq!(failure.kind(), ProducerFailureKind::ExecutionUnavailable);
        assert_eq!(failure.delivery(), status);
        assert_eq!(failure.broker_code(), None);
    }
}

fn fact(kind: ProducerBrokerFailureKind, code: i16) -> ProducerBrokerFailure {
    let code =
        NonZeroI16::new(code).unwrap_or_else(|| panic!("the test broker code must be non-zero"));
    ProducerBrokerFailure::new(kind, code)
}
