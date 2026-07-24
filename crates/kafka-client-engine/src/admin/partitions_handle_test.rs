//! Accepted `CreatePartitions` post-commit fault scenarios.

use super::{
    CreatePartitionsAcceptedFaultKind, CreatePartitionsHostError,
    partitions_handle::accepted_fault_kind,
};

#[test]
fn accepted_wake_failure_never_revokes_operation_ownership() {
    assert_eq!(
        accepted_fault_kind(CreatePartitionsHostError::Wake),
        CreatePartitionsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(CreatePartitionsHostError::MissingTerminal),
        CreatePartitionsAcceptedFaultKind::HostInvariant
    );
}
