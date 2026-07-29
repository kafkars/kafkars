//! Accepted `LegacyAlterConfigs` post-commit fault scenarios.

use super::{
    LegacyAlterConfigsAcceptedFaultKind, LegacyAlterConfigsHostError, handle::accepted_fault_kind,
};

#[test]
fn accepted_faults_never_revoke_legacy_alter_configs_ownership() {
    assert_eq!(
        accepted_fault_kind(LegacyAlterConfigsHostError::Wake),
        LegacyAlterConfigsAcceptedFaultKind::Wake
    );
    assert_eq!(
        accepted_fault_kind(LegacyAlterConfigsHostError::MissingTerminal),
        LegacyAlterConfigsAcceptedFaultKind::HostInvariant
    );
}
