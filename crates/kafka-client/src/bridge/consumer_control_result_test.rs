//! Exhaustive category tests for assigned-consumer control translation.

use kafka_client_engine::AssignedConsumerControlErrorKind as Kind;

use super::consumer_control_result::translate_assigned_control_admission_kind;
use crate::ErrorKind;

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
