//! Accepted `DeleteTopics` post-commit fault scenarios.

use super::{
    DeleteTopicsAcceptedFaultKind, DeleteTopicsHostError, delete_handle::accepted_fault_kind,
};

#[test]
fn accepted_wake_failure_never_revokes_operation_ownership() {
    assert_eq!(
        accepted_fault_kind(DeleteTopicsHostError::Wake),
        DeleteTopicsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(DeleteTopicsHostError::MissingTerminal),
        DeleteTopicsAcceptedFaultKind::HostInvariant
    );
}
