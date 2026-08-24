//! Public incremental-assignment result shape contracts.

use super::{
    assignment_change_result::{
        AssignedConsumerTryChangeAssignmentAccepted, AssignedConsumerTryChangeAssignmentError,
        AssignedConsumerTryChangeAssignmentErrorKind,
    },
    result::{AssignedConsumerAccepted, AssignedConsumerPortError},
};

#[test]
fn accepted_empty_change_retains_truthful_optional_epoch() {
    fn require_send<T: Send>() {}
    fn require_copy<T: Copy>() {}

    require_send::<AssignedConsumerTryChangeAssignmentAccepted>();
    require_copy::<AssignedConsumerTryChangeAssignmentAccepted>();
    let accepted = AssignedConsumerTryChangeAssignmentAccepted::from_port(
        AssignedConsumerAccepted::new(None, Ok(())),
    );
    assert_eq!(accepted.epoch(), None);
    assert_eq!(accepted.fault(), None);
}

#[test]
fn public_change_rejection_hides_private_port_types() {
    let error =
        AssignedConsumerTryChangeAssignmentError::from_port(&AssignedConsumerPortError::Closed);

    assert_eq!(
        error.kind(),
        AssignedConsumerTryChangeAssignmentErrorKind::Closed
    );
    assert_eq!(
        error.to_string(),
        "assigned-consumer assignment change failed: Closed"
    );
}
