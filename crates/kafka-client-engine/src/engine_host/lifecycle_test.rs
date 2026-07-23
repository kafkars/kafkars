//! Retained lifecycle-report state scenarios.

use super::EngineLifecycle;

#[test]
fn closed_is_published_only_by_terminal_owner() {
    let lifecycle = EngineLifecycle::new();
    assert!(!lifecycle.is_closed());

    lifecycle.publish(None);

    assert!(lifecycle.is_closed());
}
