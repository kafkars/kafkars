//! Public assignment scalar validation and core-free accessor scenarios.

use super::assignment::{
    AssignedConsumerAssignment, AssignedConsumerAssignmentInputErrorKind,
    AssignedConsumerStartPosition,
};

#[test]
fn valid_assignment_inputs_round_trip_without_core_types() {
    let cases = [
        AssignedConsumerStartPosition::Beginning,
        AssignedConsumerStartPosition::End,
        AssignedConsumerStartPosition::Offset(42),
    ];

    for start in cases {
        let assignment = AssignedConsumerAssignment::try_new("orders", 7, start)
            .unwrap_or_else(|error| panic!("valid assignment input: {error}"));
        assert_eq!(assignment.topic(), "orders");
        assert_eq!(assignment.partition(), 7);
        assert_eq!(assignment.start(), start);
    }
}

#[test]
fn invalid_scalar_domains_are_rejected_before_operation_admission() {
    let long_topic = "x".repeat(250);
    let cases = [
        (
            AssignedConsumerAssignment::try_new("", 0, AssignedConsumerStartPosition::Beginning),
            AssignedConsumerAssignmentInputErrorKind::EmptyTopic,
        ),
        (
            AssignedConsumerAssignment::try_new(
                long_topic,
                0,
                AssignedConsumerStartPosition::Beginning,
            ),
            AssignedConsumerAssignmentInputErrorKind::TopicTooLong,
        ),
        (
            AssignedConsumerAssignment::try_new(
                "orders",
                -1,
                AssignedConsumerStartPosition::Beginning,
            ),
            AssignedConsumerAssignmentInputErrorKind::NegativePartition,
        ),
        (
            AssignedConsumerAssignment::try_new(
                "orders",
                0,
                AssignedConsumerStartPosition::Offset(-1),
            ),
            AssignedConsumerAssignmentInputErrorKind::NegativeOffset,
        ),
    ];

    for (result, expected) in cases {
        let error = result.err().unwrap_or_else(|| panic!("input must fail"));
        assert_eq!(error.kind(), expected);
    }
}
