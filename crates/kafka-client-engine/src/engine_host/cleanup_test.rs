//! Ordered terminal cleanup failure composition scenarios.

use super::{EngineHostError, cleanup::combine_cleanup};

#[test]
fn cleanup_failure_never_replaces_the_primary_failure() {
    let combined = combine_cleanup(
        Some(EngineHostError::ForcedTestFailure),
        Some(EngineHostError::DriverStopped),
    )
    .unwrap_or_else(|| panic!("both failures must remain visible"));
    assert!(combined.to_string().starts_with("forced engine host"));
    assert!(combined.to_string().contains("embedded driver stopped"));
}
