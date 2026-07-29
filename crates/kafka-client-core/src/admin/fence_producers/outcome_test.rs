//! Scenarios for exact per-ID producer-fencing outcomes.

use core::num::NonZeroI16;

use super::{
    AdminFenceProducerBrokerError, AdminFenceProducerOutcome, AdminFenceProducerResult,
    AdminFenceProducersBatch, AdminFencedProducerIdentity,
};

#[test]
fn identity_requires_nonnegative_id_and_epoch_and_preserves_exact_scalars() {
    assert_eq!(AdminFencedProducerIdentity::try_new(-1, 0), None);
    assert_eq!(AdminFencedProducerIdentity::try_new(0, -1), None);

    let identity = AdminFencedProducerIdentity::try_new(i64::MAX, i16::MAX)
        .unwrap_or_else(|| panic!("nonnegative producer identity"));
    assert_eq!(identity.producer_id(), i64::MAX);
    assert_eq!(identity.producer_epoch(), i16::MAX);
    assert_eq!(identity.into_parts(), (i64::MAX, i16::MAX));
}

#[test]
fn success_and_broker_failure_preserve_exact_per_id_facts() {
    let identity = AdminFencedProducerIdentity::try_new(91, 7)
        .unwrap_or_else(|| panic!("valid producer identity"));
    let fenced = AdminFenceProducerOutcome::fenced("invoice-worker".to_owned(), identity);
    let AdminFenceProducerResult::Fenced(actual) = fenced.result() else {
        panic!("ID must be fenced");
    };
    assert_eq!(*actual, identity);

    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero code"));
    let failed = AdminFenceProducerOutcome::broker_failed(
        "audit-writer".to_owned(),
        AdminFenceProducerBrokerError::new(code),
    );
    let AdminFenceProducerResult::BrokerFailed(error) = failed.result() else {
        panic!("ID must retain broker rejection");
    };
    assert_eq!(error.code(), -31_777);
}

#[test]
fn batch_preserves_caller_order_and_maximum_throttle() {
    let identity = AdminFencedProducerIdentity::try_new(91, 7)
        .unwrap_or_else(|| panic!("valid producer identity"));
    let batch = AdminFenceProducersBatch::new(
        73,
        vec![
            AdminFenceProducerOutcome::fenced("invoice-worker".to_owned(), identity),
            AdminFenceProducerOutcome::fenced("audit-writer".to_owned(), identity),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].transactional_id(), "invoice-worker");
    assert_eq!(batch.outcomes()[1].transactional_id(), "audit-writer");
}
