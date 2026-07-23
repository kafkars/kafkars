//! Tests for deterministic virtual time.

use crate::VirtualClock;

#[test]
fn virtual_time_advances_without_sleeping() {
    let mut clock = VirtualClock::default();
    clock.advance(17);
    assert_eq!(clock.now().tick(), 17);
}
