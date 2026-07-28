//! Lossless reassignment failure and ordered outcome values.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::{
    AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentOutcome,
    AlterPartitionReassignmentResult, AlterPartitionReassignmentsFailure,
    AlterPartitionReassignmentsFailureKind,
};

#[test]
fn signed_broker_error_and_delivery_certainty_round_trip_exactly() {
    let broker = AlterPartitionReassignmentBrokerError::with_bounded_message(
        NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("nonzero")),
        Some("controller".to_owned()),
        true,
    );
    let outcome = AlterPartitionReassignmentOutcome::failed("orders".to_owned(), 2, broker);
    let (topic, partition, result) = outcome.into_parts();
    assert_eq!((topic.as_str(), partition), ("orders", 2));
    let AlterPartitionReassignmentResult::Failed(error) = result else {
        panic!("broker failure expected");
    };
    assert_eq!(
        error.into_parts(),
        (-31_999, Some("controller".to_owned()), true)
    );

    let failure = AlterPartitionReassignmentsFailure::new(
        AlterPartitionReassignmentsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(
        failure.into_parts(),
        (
            AlterPartitionReassignmentsFailureKind::Transport,
            DeliveryStatus::PossiblySent,
        )
    );
}
