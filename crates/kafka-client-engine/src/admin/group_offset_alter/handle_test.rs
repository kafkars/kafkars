//! Accepted offset-alteration ownership and post-commit fault scenarios.

use super::{
    AlterConsumerGroupOffsetsAcceptedFaultKind, AlterConsumerGroupOffsetsHostError,
    handle::accepted_fault_kind,
};

#[test]
fn accepted_faults_never_revoke_offset_alteration_ownership() {
    assert_eq!(
        accepted_fault_kind(AlterConsumerGroupOffsetsHostError::Wake),
        AlterConsumerGroupOffsetsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(AlterConsumerGroupOffsetsHostError::MissingTerminal),
        AlterConsumerGroupOffsetsAcceptedFaultKind::HostInvariant
    );
}
