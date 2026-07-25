//! Accepted `IncrementalAlterConfigs` post-commit fault scenarios.

use super::{
    IncrementalAlterConfigsAcceptedFaultKind, IncrementalAlterConfigsHostError,
    handle::accepted_fault_kind,
};

#[test]
fn accepted_faults_never_revoke_incremental_alter_configs_ownership() {
    assert_eq!(
        accepted_fault_kind(IncrementalAlterConfigsHostError::Wake),
        IncrementalAlterConfigsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(IncrementalAlterConfigsHostError::MissingTerminal),
        IncrementalAlterConfigsAcceptedFaultKind::HostInvariant
    );
}
