//! Exhaustive category tests for assignment-result translation.

use kafka_client_engine::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerTryReplaceAssignmentErrorKind as Kind,
};

use super::assignment_result::{
    translate_assigned_assignment_admission_kind, translate_assigned_assignment_fault,
};
use crate::ErrorKind;

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
fn advisory_wake_failure_remains_diagnostic_only() {
    assert_eq!(
        translate_assigned_assignment_fault(AssignedConsumerAcceptedFaultKind::Wake).kind(),
        ErrorKind::Internal
    );
}
