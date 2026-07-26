//! Driver-neutral invalidation-admission classification scenarios.

use kafka_client_core::GroupId;
use kafka_driver::SubmitError;

use super::coordinator_invalidation_admission::{
    ClassicCoordinatorInvalidationAdmissionFailure,
    ClassicCoordinatorInvalidationAdmissionFailureKind,
};

#[test]
fn capacity_and_closed_rejections_remain_distinct_without_exposing_tokens() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("test group must be nonzero"));

    let full = ClassicCoordinatorInvalidationAdmissionFailure::new(group_id, SubmitError::Full);
    let closed = ClassicCoordinatorInvalidationAdmissionFailure::new(group_id, SubmitError::Closed);

    assert_eq!(full.group_id(), group_id);
    assert_eq!(
        full.kind(),
        ClassicCoordinatorInvalidationAdmissionFailureKind::Full
    );
    assert_eq!(
        closed.kind(),
        ClassicCoordinatorInvalidationAdmissionFailureKind::Closed
    );
}
