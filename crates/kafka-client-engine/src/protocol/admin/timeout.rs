//! Shared derivation of Kafka admin timeouts from one original absolute deadline.

use kafka_client_core::{Deadline, Moment};

/// Request construction failure before any driver ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdminRequestDeadlineError {
    /// A deadline adapter supplied an impossible negative broker timeout.
    NegativeTimeout,
    /// The original absolute deadline has already elapsed.
    DeadlineElapsed,
}

/// Derives Kafka's millisecond timeout from the remaining original deadline.
pub(crate) fn remaining_timeout_ms(
    now: Moment,
    deadline: Deadline,
) -> Result<i32, AdminRequestDeadlineError> {
    let remaining = deadline
        .tick()
        .checked_sub(now.tick())
        .filter(|remaining| *remaining > 0)
        .ok_or(AdminRequestDeadlineError::DeadlineElapsed)?;
    let milliseconds = remaining.saturating_add(999_999) / 1_000_000;
    Ok(i32::try_from(milliseconds).unwrap_or(i32::MAX))
}
