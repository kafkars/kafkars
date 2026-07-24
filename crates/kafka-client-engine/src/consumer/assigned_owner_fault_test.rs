//! Stable assigned-owner fault classification without releasing retained owners.

use super::{
    assigned_owner_fault::{AssignedConsumerFaultKind, AssignedConsumerOwnerFault},
    assigned_owner_test::owner,
};

#[test]
fn scalar_fault_kind_does_not_release_the_retained_fault() {
    let mut owner = owner(1);
    owner.fault = Some(AssignedConsumerOwnerFault::Clock(
        crate::clock::ClockError::TickOverflow,
    ));

    assert_eq!(owner.fault_kind(), Some(AssignedConsumerFaultKind::Clock));
    assert!(owner.fault.is_some());
}
