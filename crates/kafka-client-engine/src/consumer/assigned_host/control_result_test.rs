//! Public position-control result trait and stable rejection-shape contracts.

use super::{
    control_result::{
        AssignedConsumerControlAccepted, AssignedConsumerControlError,
        AssignedConsumerControlErrorKind,
    },
    result::AssignedConsumerPortError,
};

#[test]
fn accepted_control_is_an_ordinary_sendable_scalar_result() {
    fn require_send<T: Send>() {}
    fn require_copy<T: Copy>() {}

    require_send::<AssignedConsumerControlAccepted>();
    require_copy::<AssignedConsumerControlAccepted>();
}

#[test]
fn public_rejection_does_not_expose_private_port_types() {
    let error = AssignedConsumerControlError::from_port(&AssignedConsumerPortError::Closed);

    assert_eq!(error.kind(), AssignedConsumerControlErrorKind::Closed);
    assert_eq!(
        error.to_string(),
        "assigned-consumer position control failed: Closed"
    );
}
