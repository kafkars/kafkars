//! Exhaustive category tests for assignment-result translation.

use kafka_client_engine::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerTryReplaceAssignmentErrorKind as Kind,
};

use super::assignment_result::{
    translate_assigned_assignment_admission_kind, translate_assigned_assignment_fault,
};
use crate::{ErrorKind, RetryAdvice};

#[test]
fn every_assignment_admission_kind_is_translated() {
    for (kind, expected) in [
        (Kind::Contended, ErrorKind::Backpressure),
        (Kind::Closed, ErrorKind::State),
        (Kind::Pending, ErrorKind::Backpressure),
        (Kind::AssignmentCapacity, ErrorKind::Backpressure),
        (Kind::TopicCapacity, ErrorKind::Backpressure),
        (Kind::RetainedNameCapacity, ErrorKind::Backpressure),
        (Kind::EventCapacity, ErrorKind::Backpressure),
        (Kind::EmptyAssignment, ErrorKind::Configuration),
        (Kind::DuplicatePartition, ErrorKind::Configuration),
        (Kind::DeadlineOverflow, ErrorKind::Timeout),
        (Kind::ResourceExhausted, ErrorKind::Internal),
        (Kind::HostUnavailable, ErrorKind::Internal),
        (Kind::InternalInvariant, ErrorKind::Internal),
    ] {
        assert_eq!(
            translate_assigned_assignment_admission_kind(kind).kind(),
            expected
        );
    }
}

#[test]
fn only_transient_pre_admission_assignment_rejections_are_retry_safe() {
    for kind in [Kind::Contended, Kind::Pending] {
        assert_eq!(
            translate_assigned_assignment_admission_kind(kind).retry_advice(),
            RetryAdvice::RetrySafe
        );
    }
    for kind in [
        Kind::Closed,
        Kind::AssignmentCapacity,
        Kind::TopicCapacity,
        Kind::RetainedNameCapacity,
        Kind::EventCapacity,
        Kind::EmptyAssignment,
        Kind::DuplicatePartition,
        Kind::DeadlineOverflow,
        Kind::ResourceExhausted,
        Kind::HostUnavailable,
        Kind::InternalInvariant,
    ] {
        assert_eq!(
            translate_assigned_assignment_admission_kind(kind).retry_advice(),
            RetryAdvice::DoNotRetry
        );
    }
}

#[test]
fn advisory_wake_failure_remains_diagnostic_only() {
    assert_eq!(
        translate_assigned_assignment_fault(AssignedConsumerAcceptedFaultKind::Wake).kind(),
        ErrorKind::Internal
    );
}
