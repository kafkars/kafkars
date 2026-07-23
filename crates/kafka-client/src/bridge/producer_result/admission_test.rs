//! Scenarios for exhaustive producer admission and accepted-fault translation.

use kafka_client_engine::{
    ProducerAcceptedFault as EngineAcceptedFault,
    ProducerAcceptedFaultKind as EngineAcceptedFaultKind,
    ProducerTrySendError as EngineTrySendError, ProducerTrySendErrorKind as EngineTrySendErrorKind,
};

use super::admission::{
    ProducerAdmissionRejection, accepted_fault_kind, admission_error, admission_kind,
    translate_accepted_fault, translate_admission_error,
};
use crate::{DeliveryStatus, ErrorKind, KafkaError, Record};

#[test]
fn future_admission_bridge_surface_remains_type_checked() {
    let _ = translate_admission_error as fn(EngineTrySendError) -> ProducerAdmissionRejection;
    let _ = ProducerAdmissionRejection::into_parts
        as fn(ProducerAdmissionRejection) -> (Option<Record>, KafkaError);
    let _ = translate_accepted_fault as fn(&EngineAcceptedFault) -> KafkaError;
}

#[test]
fn every_engine_admission_kind_has_one_stable_facade_category() {
    let cases = [
        (EngineTrySendErrorKind::EmptyTopic, ErrorKind::InvalidRecord),
        (
            EngineTrySendErrorKind::MissingExplicitPartition,
            ErrorKind::InvalidRecord,
        ),
        (
            EngineTrySendErrorKind::NegativeExplicitPartition,
            ErrorKind::InvalidRecord,
        ),
        (
            EngineTrySendErrorKind::DeadlineUnrepresentable,
            ErrorKind::Timeout,
        ),
        (
            EngineTrySendErrorKind::TimestampUnrepresentable,
            ErrorKind::Internal,
        ),
        (EngineTrySendErrorKind::Contended, ErrorKind::Backpressure),
        (
            EngineTrySendErrorKind::CompletionCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::RecordCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::ByteCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::RecordSizeUnrepresentable,
            ErrorKind::InvalidRecord,
        ),
        (
            EngineTrySendErrorKind::BatchCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::AccumulatorPending,
            ErrorKind::Backpressure,
        ),
        (EngineTrySendErrorKind::DeadlineElapsed, ErrorKind::Timeout),
        (EngineTrySendErrorKind::Closed, ErrorKind::State),
        (
            EngineTrySendErrorKind::LocalIdentityExhausted,
            ErrorKind::Internal,
        ),
        (EngineTrySendErrorKind::HostPoisoned, ErrorKind::Internal),
        (
            EngineTrySendErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ];

    for (engine, facade) in cases {
        assert_eq!(admission_kind(engine), facade);
    }
}

#[test]
fn pre_ownership_admission_failure_is_exactly_not_sent() {
    let error = admission_error(
        EngineTrySendErrorKind::InternalInvariant,
        Some("completion generation disagreed"),
    );

    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    assert_eq!(error.to_string(), "completion generation disagreed");
    assert_eq!(error.broker_code(), None);
}

#[test]
fn accepted_execution_faults_remain_internal_without_invented_delivery_status() {
    for kind in [
        EngineAcceptedFaultKind::HostInvariant,
        EngineAcceptedFaultKind::Wake,
    ] {
        assert_eq!(accepted_fault_kind(kind), ErrorKind::Internal);
    }
}
