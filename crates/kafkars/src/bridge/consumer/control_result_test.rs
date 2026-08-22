//! Exhaustive category tests for assigned-consumer control translation.

use kafka_client_engine::AssignedConsumerControlErrorKind as Kind;

use super::control_result::translate_assigned_control_admission_kind;
use crate::{ErrorKind, RetryAdvice};

#[test]
fn every_control_admission_kind_is_translated() {
    for (kind, expected) in [
        (Kind::Contended, ErrorKind::Backpressure),
        (Kind::Closed, ErrorKind::State),
        (Kind::Pending, ErrorKind::Backpressure),
        (Kind::NoAssignment, ErrorKind::State),
        (Kind::StaleAssignment, ErrorKind::State),
        (Kind::UnknownPartition, ErrorKind::State),
        (Kind::NegativeOffset, ErrorKind::Configuration),
        (Kind::DeadlineOverflow, ErrorKind::Timeout),
        (Kind::ResourceExhausted, ErrorKind::Internal),
        (Kind::HostUnavailable, ErrorKind::Internal),
        (Kind::InternalInvariant, ErrorKind::Internal),
    ] {
        assert_eq!(
            translate_assigned_control_admission_kind(kind).kind(),
            expected
        );
    }
}

#[test]
fn only_pre_admission_control_backpressure_is_retry_safe() {
    for kind in [Kind::Contended, Kind::Pending] {
        assert_eq!(
            translate_assigned_control_admission_kind(kind).retry_advice(),
            RetryAdvice::RetrySafe
        );
    }
    for kind in [
        Kind::Closed,
        Kind::NoAssignment,
        Kind::StaleAssignment,
        Kind::UnknownPartition,
        Kind::NegativeOffset,
        Kind::DeadlineOverflow,
        Kind::ResourceExhausted,
        Kind::HostUnavailable,
        Kind::InternalInvariant,
    ] {
        assert_eq!(
            translate_assigned_control_admission_kind(kind).retry_advice(),
            RetryAdvice::DoNotRetry
        );
    }
}
