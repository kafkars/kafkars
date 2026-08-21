//! Scenarios for exhaustive and lossless terminal producer translation.

use kafka_client_engine::{
    ProducerDeliveryError as EngineDeliveryError, ProducerDeliveryFailureKind as EngineFailureKind,
    ProducerDeliveryResult as EngineDeliveryResult, ProducerDeliveryStatus as EngineDeliveryStatus,
    ProducerObserverError as EngineObserverError,
};

use super::delivery::{
    delivery_status, failure_error, failure_kind, metadata_parts, translate_delivery_error,
    translate_delivery_result,
};
use crate::{DeliveryStatus, ErrorKind, KafkaError, RecordMetadata, RetryAdvice};

#[test]
fn future_delivery_bridge_surface_remains_type_checked() {
    let _ = translate_delivery_result
        as fn(
            std::sync::Arc<str>,
            i64,
            Option<usize>,
            Option<usize>,
            EngineDeliveryResult,
        ) -> Result<RecordMetadata, KafkaError>;
}

#[test]
fn every_engine_delivery_failure_has_one_stable_facade_category() {
    let cases = [
        (EngineFailureKind::Cancelled, ErrorKind::Cancelled),
        (EngineFailureKind::DriverRejected, ErrorKind::Backpressure),
        (
            EngineFailureKind::MaterializationFailed,
            ErrorKind::Internal,
        ),
        (EngineFailureKind::Routing, ErrorKind::Routing),
        (EngineFailureKind::BrokerRetriable, ErrorKind::Broker),
        (EngineFailureKind::AccessRejected, ErrorKind::Access),
        (EngineFailureKind::InvalidRecord, ErrorKind::InvalidRecord),
        (EngineFailureKind::Compatibility, ErrorKind::Compatibility),
        (EngineFailureKind::ProducerFenced, ErrorKind::Fenced),
        (EngineFailureKind::ProducerIdentity, ErrorKind::State),
        (EngineFailureKind::Transport, ErrorKind::Transport),
        (EngineFailureKind::InvalidResponse, ErrorKind::Broker),
        (EngineFailureKind::ExecutionUnavailable, ErrorKind::Internal),
        (EngineFailureKind::DeadlineElapsed, ErrorKind::Timeout),
        (EngineFailureKind::UnknownBroker, ErrorKind::Broker),
    ];

    for (engine, facade) in cases {
        assert_eq!(failure_kind(engine), facade);
    }
}

#[test]
fn terminal_type_mismatch_is_an_internal_contract_failure() {
    let error = translate_delivery_error(EngineDeliveryError::Observer(
        EngineObserverError::TerminalTypeMismatch,
    ));

    assert_eq!(error.kind(), ErrorKind::Internal);
}

#[test]
fn engine_delivery_certainty_maps_without_weakening() {
    assert_eq!(
        delivery_status(EngineDeliveryStatus::NotSent),
        DeliveryStatus::NotSent
    );
    assert_eq!(
        delivery_status(EngineDeliveryStatus::PossiblySent),
        DeliveryStatus::PossiblySent
    );
}

#[test]
fn terminal_failure_preserves_exact_delivery_status_and_broker_code() {
    let error = failure_error(
        EngineFailureKind::UnknownBroker,
        EngineDeliveryStatus::PossiblySent,
        Some(-123),
    );

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(error.broker_code(), Some(-123));
}

#[test]
fn terminal_retry_advice_distinguishes_transience_from_duplicate_safety() {
    for kind in [
        EngineFailureKind::Routing,
        EngineFailureKind::BrokerRetriable,
    ] {
        let safe = failure_error(kind, EngineDeliveryStatus::NotSent, Some(20));
        assert!(safe.is_retriable(), "{kind:?}");
        assert!(!safe.is_fatal(), "{kind:?}");
        assert_eq!(safe.retry_advice(), RetryAdvice::RetrySafe, "{kind:?}");

        let uncertain = failure_error(kind, EngineDeliveryStatus::PossiblySent, Some(20));
        assert!(uncertain.is_retriable(), "{kind:?}");
        assert!(!uncertain.is_fatal(), "{kind:?}");
        assert_eq!(
            uncertain.retry_advice(),
            RetryAdvice::RetryMayDuplicate,
            "{kind:?}"
        );
    }

    let driver_rejected = failure_error(
        EngineFailureKind::DriverRejected,
        EngineDeliveryStatus::NotSent,
        None,
    );
    assert!(driver_rejected.is_retriable());
    assert!(!driver_rejected.is_fatal());
    assert_eq!(driver_rejected.retry_advice(), RetryAdvice::RetrySafe);

    let impossible_uncertain_rejection = failure_error(
        EngineFailureKind::DriverRejected,
        EngineDeliveryStatus::PossiblySent,
        None,
    );
    assert!(!impossible_uncertain_rejection.is_retriable());
    assert!(!impossible_uncertain_rejection.is_fatal());
    assert_eq!(
        impossible_uncertain_rejection.retry_advice(),
        RetryAdvice::DoNotRetry
    );
}

#[test]
fn terminal_advice_is_fatal_or_permanent_only_from_exact_engine_facts() {
    for kind in [
        EngineFailureKind::ProducerFenced,
        EngineFailureKind::ProducerIdentity,
        EngineFailureKind::InvalidResponse,
        EngineFailureKind::ExecutionUnavailable,
    ] {
        let error = failure_error(kind, EngineDeliveryStatus::NotSent, None);
        assert!(!error.is_retriable(), "{kind:?}");
        assert!(error.is_fatal(), "{kind:?}");
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry, "{kind:?}");
    }

    for kind in [
        EngineFailureKind::Cancelled,
        EngineFailureKind::MaterializationFailed,
        EngineFailureKind::AccessRejected,
        EngineFailureKind::InvalidRecord,
        EngineFailureKind::Compatibility,
        EngineFailureKind::Transport,
        EngineFailureKind::DeadlineElapsed,
        EngineFailureKind::UnknownBroker,
    ] {
        let error = failure_error(kind, EngineDeliveryStatus::NotSent, None);
        assert!(!error.is_retriable(), "{kind:?}");
        assert!(!error.is_fatal(), "{kind:?}");
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry, "{kind:?}");
    }
}

#[test]
fn engine_metadata_preserves_facade_topic_and_acknowledgement_fields() {
    let result = metadata_parts(
        "orders".to_owned(),
        7,
        4_294_967_296,
        Some(1_725_000_000_000),
        Some(19),
        Some(8),
        Some(7),
    );
    let Ok(metadata) = result else {
        panic!("valid engine metadata should translate")
    };

    assert_eq!(metadata.topic(), "orders");
    assert_eq!(metadata.partition(), 7);
    assert_eq!(metadata.offset(), 4_294_967_296);
    assert_eq!(metadata.timestamp_milliseconds(), Some(1_725_000_000_000));
    assert_eq!(metadata.leader_epoch(), Some(19));
    assert_eq!(metadata.serialized_key_size(), Some(8));
    assert_eq!(metadata.serialized_value_size(), Some(7));
}

#[test]
fn out_of_range_engine_partition_is_an_internal_translation_failure() {
    let result = metadata_parts("orders".to_owned(), u32::MAX, 0, None, None, None, Some(0));
    let Err(error) = result else {
        panic!("unsigned partition outside Kafka's domain must fail")
    };

    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.delivery_status(), None);
}

#[test]
fn engine_observer_lifecycle_errors_translate_exhaustively() {
    for observer in [
        EngineObserverError::AlreadyObserved,
        EngineObserverError::Stale,
    ] {
        let expected = observer.to_string();
        let error = translate_delivery_error(EngineDeliveryError::Observer(observer));

        assert_eq!(error.kind(), ErrorKind::State);
        assert_eq!(error.to_string(), expected);
        assert_eq!(error.delivery_status(), None);
        assert_eq!(error.broker_code(), None);
    }
}
