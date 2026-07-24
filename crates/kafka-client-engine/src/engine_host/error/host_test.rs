//! Stable internal host diagnostic scenarios.

use super::EngineHostError;

#[test]
fn create_topics_diagnostic_names_the_concrete_owner() {
    assert_eq!(
        EngineHostError::AdminLockPoisoned.to_string(),
        "CreateTopics host ownership lock is poisoned"
    );
}
