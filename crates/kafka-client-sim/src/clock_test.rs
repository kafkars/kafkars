//! Tests for deterministic virtual time.

use crate::VirtualClock;

#[test]
fn virtual_time_advances_without_sleeping() {
    let mut clock = VirtualClock::default();
    assert_eq!(clock.advance(17), Ok(()));
    assert_eq!(clock.now().tick(), 17);
}

#[test]
fn virtual_time_overflow_is_rejected_without_saturation() {
    let mut clock = VirtualClock::default();
    assert_eq!(clock.advance(u64::MAX), Ok(()));
    assert!(clock.advance(1).is_err());
    assert_eq!(clock.now().tick(), u64::MAX);
}
