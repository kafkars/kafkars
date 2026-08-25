//! Exhaustive stable transaction-end result translation.

use kafka_client_engine::{
    TransactionEndDeliveryStatus, TransactionEndFailureKind,
    TransactionEndIntent as EngineEndIntent, TransactionEndObserverError, TransactionEndOutcome,
};

use super::{translate_end_failure_parts, translate_end_observation};
use crate::{DeliveryStatus, ErrorKind, TransactionEndIntent};

#[test]
fn end_observation_preserves_disposition_and_observer_failures() {
    for (intent, outcome) in [
        (
            TransactionEndIntent::Commit,
            TransactionEndOutcome::Committed,
        ),
        (TransactionEndIntent::Abort, TransactionEndOutcome::Aborted),
    ] {
        assert_eq!(translate_end_observation(intent, Ok(outcome)), Ok(()));
    }
    for (intent, outcome) in [
        (TransactionEndIntent::Commit, TransactionEndOutcome::Aborted),
        (
            TransactionEndIntent::Abort,
            TransactionEndOutcome::Committed,
        ),
    ] {
        let Err(error) = translate_end_observation(intent, Ok(outcome)) else {
            panic!("mismatched transaction disposition must fail")
        };
        assert_eq!(error.kind(), ErrorKind::Internal);
        assert_eq!(error.transaction_end_intent(), Some(intent));
        assert!(error.is_fatal());
    }
    for (input, expected) in [
        (
            TransactionEndObserverError::AlreadyObserved,
            ErrorKind::State,
        ),
        (TransactionEndObserverError::Stale, ErrorKind::Internal),
    ] {
        let Err(error) = translate_end_observation(TransactionEndIntent::Commit, Err(input)) else {
            panic!("observer failure must remain an error")
        };
        assert_eq!(error.kind(), expected);
        assert_eq!(
            error.transaction_end_intent(),
            Some(TransactionEndIntent::Commit)
        );
    }
}

#[test]
fn end_failures_preserve_intent_cause_certainty_code_and_fatality() {
    use TransactionEndFailureKind as Kind;
    for (kind, code, expected) in [
        (Kind::DeadlineElapsed, None, ErrorKind::Timeout),
        (Kind::DriverRejected, None, ErrorKind::Backpressure),
        (Kind::Transport, None, ErrorKind::Transport),
        (Kind::Compatibility, None, ErrorKind::Compatibility),
        (Kind::InvalidResponse, None, ErrorKind::Broker),
        (Kind::DriverClosed, None, ErrorKind::Internal),
        (Kind::Correlation, None, ErrorKind::Internal),
        (Kind::Access, Some(53), ErrorKind::Access),
        (Kind::Coordinator, Some(16), ErrorKind::Routing),
        (Kind::Broker, Some(-731), ErrorKind::Broker),
        (Kind::Lifecycle, None, ErrorKind::Internal),
        (Kind::Fenced, Some(47), ErrorKind::Fenced),
        (Kind::Fenced, Some(90), ErrorKind::Fenced),
        (Kind::Fenced, Some(91), ErrorKind::Broker),
        (Kind::Fenced, None, ErrorKind::Broker),
    ] {
        let error = translate_end_failure_parts(
            TransactionEndIntent::Abort,
            EngineEndIntent::Abort,
            kind,
            TransactionEndDeliveryStatus::PossiblySent,
            code,
        );
        assert_eq!(error.kind(), expected, "{kind:?}");
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
        assert_eq!(error.broker_code(), code);
        assert_eq!(
            error.transaction_end_intent(),
            Some(TransactionEndIntent::Abort)
        );
        assert!(error.is_fatal());
    }
}

#[test]
fn mismatched_engine_end_intent_fails_internal_without_claiming_fencing() {
    let error = translate_end_failure_parts(
        TransactionEndIntent::Commit,
        EngineEndIntent::Abort,
        TransactionEndFailureKind::Fenced,
        TransactionEndDeliveryStatus::PossiblySent,
        Some(90),
    );

    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(
        error.transaction_end_intent(),
        Some(TransactionEndIntent::Commit)
    );
    assert_eq!(error.broker_code(), None);
    assert!(error.is_fatal());
}
