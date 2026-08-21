//! Tests for the facade-owned stable error vocabulary.

use crate::{DeliveryStatus, ErrorKind, KafkaError, RetryAdvice};

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
    assert_eq!(error.is_internal_topic(), None);
    assert!(!error.diagnostic_truncated());
    assert!(!error.requires_transaction_abort());
    assert!(!error.is_retriable());
    assert!(!error.is_fatal());
    assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
}

#[test]
fn closed_retry_advice_distinguishes_safe_duplicate_risk_and_fatal() {
    let safe = KafkaError::new(ErrorKind::Backpressure, "capacity")
        .with_delivery_status(DeliveryStatus::NotSent)
        .with_safe_retry();
    assert!(safe.is_retriable());
    assert!(!safe.is_fatal());
    assert_eq!(safe.retry_advice(), RetryAdvice::RetrySafe);

    let duplicate_risk = KafkaError::new(ErrorKind::Broker, "transient broker response")
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_duplicate_risk();
    assert!(duplicate_risk.is_retriable());
    assert!(!duplicate_risk.is_fatal());
    assert_eq!(
        duplicate_risk.retry_advice(),
        RetryAdvice::RetryMayDuplicate
    );

    let fatal = KafkaError::new(ErrorKind::Fenced, "producer fenced").with_fatal_disposition();
    assert!(!fatal.is_retriable());
    assert!(fatal.is_fatal());
    assert_eq!(fatal.retry_advice(), RetryAdvice::DoNotRetry);
}

#[test]
fn possibly_sent_certainty_cannot_be_reclassified_as_retry_safe() {
    let error = KafkaError::new(ErrorKind::Transport, "uncertain transport")
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_safe_retry();

    assert!(!error.is_retriable());
    assert!(!error.is_fatal());
    assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
}

#[test]
fn broker_code_preserves_the_signed_protocol_domain() {
    let error =
        KafkaError::new(ErrorKind::Broker, "unknown broker error").with_broker_code(Some(-123));

    assert_eq!(error.broker_code(), Some(-123));
}

#[test]
fn bounded_broker_diagnostics_preserve_truncation() {
    let error = KafkaError::new(ErrorKind::Broker, "bounded broker diagnostic")
        .with_diagnostic_truncated(true);

    assert!(error.diagnostic_truncated());
}

#[test]
fn topic_scoped_error_preserves_internal_marker() {
    let error =
        KafkaError::new(ErrorKind::Broker, "internal topic error").with_internal_topic(true);

    assert_eq!(error.is_internal_topic(), Some(true));
}

#[test]
fn transaction_abort_requirement_is_explicit_and_opt_in() {
    let error = KafkaError::new(ErrorKind::Broker, "transaction must abort")
        .with_transaction_abort_required();

    assert!(error.requires_transaction_abort());
}
