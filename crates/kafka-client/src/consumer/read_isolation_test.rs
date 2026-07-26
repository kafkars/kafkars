//! Public read-isolation vocabulary scenarios.

use super::ReadIsolation;

#[test]
fn default_is_read_uncommitted_and_both_choices_are_closed_values() {
    assert_eq!(ReadIsolation::default(), ReadIsolation::ReadUncommitted);
    assert_ne!(ReadIsolation::ReadUncommitted, ReadIsolation::ReadCommitted);
}
