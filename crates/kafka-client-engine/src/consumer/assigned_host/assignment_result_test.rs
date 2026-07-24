//! Public assignment-result trait and stable rejection-shape contracts.

use super::{
    assignment_result::{
        AssignedConsumerAssignmentEpoch, AssignedConsumerTryReplaceAssignmentAccepted,
        AssignedConsumerTryReplaceAssignmentError, AssignedConsumerTryReplaceAssignmentErrorKind,
    },
    result::AssignedConsumerPortError,
};

#[test]
fn accepted_assignment_and_epoch_are_sendable_scalar_results() {
    fn require_send<T: Send>() {}
    fn require_copy<T: Copy>() {}

    require_send::<AssignedConsumerTryReplaceAssignmentAccepted>();
    require_copy::<AssignedConsumerTryReplaceAssignmentAccepted>();
    require_copy::<AssignedConsumerAssignmentEpoch>();
}

#[test]
fn public_rejection_does_not_expose_private_port_types() {
    let error =
        AssignedConsumerTryReplaceAssignmentError::from_port(&AssignedConsumerPortError::Closed);

    assert_eq!(
        error.kind(),
        AssignedConsumerTryReplaceAssignmentErrorKind::Closed
    );
    assert_eq!(
        error.to_string(),
        "assigned-consumer assignment replacement failed: Closed"
    );
}
