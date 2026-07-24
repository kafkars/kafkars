//! Broker-throttle conversion scenarios.

use super::list_offsets_time::throttle_ticks;

#[test]
fn positive_throttle_milliseconds_become_checked_nanosecond_ticks() {
    assert_eq!(throttle_ticks(0), Some(0));
    assert_eq!(throttle_ticks(7), Some(7_000_000));
    assert_eq!(
        throttle_ticks(u32::MAX),
        Some(u64::from(u32::MAX) * 1_000_000)
    );
}
