//! Checked duration-normalization scenarios for engine configuration.

use std::time::Duration;

use super::validation::{EngineConfigError, duration_ticks};

#[test]
fn duration_ticks_are_exact_or_rejected_before_host_start() {
    assert_eq!(duration_ticks(Duration::from_nanos(7)), Ok(7));
    assert_eq!(
        duration_ticks(Duration::MAX),
        Err(EngineConfigError::DurationOverflow)
    );
}
