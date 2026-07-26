//! Accepted offset-deletion ownership and post-commit fault scenarios.

use super::{
    DeleteConsumerGroupOffsetsAcceptedFaultKind, DeleteConsumerGroupOffsetsHostError,
    handle::accepted_fault_kind,
};

#[test]
fn accepted_faults_never_revoke_offset_deletion_ownership() {
    assert_eq!(
        accepted_fault_kind(DeleteConsumerGroupOffsetsHostError::Wake),
        DeleteConsumerGroupOffsetsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(DeleteConsumerGroupOffsetsHostError::MissingTerminal),
        DeleteConsumerGroupOffsetsAcceptedFaultKind::HostInvariant
    );
}
