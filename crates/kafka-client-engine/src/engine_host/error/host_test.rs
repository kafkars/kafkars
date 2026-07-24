//! Stable internal host diagnostic scenarios.

use super::EngineHostError;

#[test]
fn concrete_admin_diagnostics_never_collapse_into_a_generic_owner() {
    assert_eq!(
        EngineHostError::CreateTopicsLockPoisoned.to_string(),
        "CreateTopics host ownership lock is poisoned"
    );
    assert_eq!(
        EngineHostError::DeleteTopicsLockPoisoned.to_string(),
        "DeleteTopics host ownership lock is poisoned"
    );
}
