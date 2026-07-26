//! Accepted group-offset ownership and post-commit fault scenarios.

use super::{
    ListConsumerGroupOffsetsAcceptedFaultKind, ListConsumerGroupOffsetsHostError,
    handle::accepted_fault_kind,
};

#[test]
fn accepted_faults_never_revoke_group_offset_ownership() {
    assert_eq!(
        accepted_fault_kind(ListConsumerGroupOffsetsHostError::Wake),
        ListConsumerGroupOffsetsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(ListConsumerGroupOffsetsHostError::MissingTerminal),
        ListConsumerGroupOffsetsAcceptedFaultKind::HostInvariant
    );
}
