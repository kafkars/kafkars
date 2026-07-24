//! Checked conversion of `ListOffsets` throttle milliseconds into core ticks.

const NANOSECONDS_PER_MILLISECOND: u64 = 1_000_000;

/// Converts Kafka's nonnegative millisecond throttle into core nanosecond ticks.
pub(crate) fn throttle_ticks(throttle_time_ms: u32) -> Option<u64> {
    u64::from(throttle_time_ms).checked_mul(NANOSECONDS_PER_MILLISECOND)
}
