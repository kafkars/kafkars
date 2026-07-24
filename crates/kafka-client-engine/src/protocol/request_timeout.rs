//! Shared derivation of generated request timeouts from one absolute core deadline.

use kafka_client_core::{Deadline, Moment};

/// Failure to derive a generated request timeout before driver ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestDeadlineError {
    /// The original absolute deadline has already elapsed.
    DeadlineElapsed,
}

/// Derives Kafka's millisecond timeout from the remaining original deadline.
pub(crate) fn remaining_timeout_ms(
    now: Moment,
    deadline: Deadline,
) -> Result<i32, RequestDeadlineError> {
    let remaining = deadline
        .tick()
        .checked_sub(now.tick())
        .filter(|remaining| *remaining > 0)
        .ok_or(RequestDeadlineError::DeadlineElapsed)?;
    let milliseconds = remaining.saturating_add(999_999) / 1_000_000;
    Ok(i32::try_from(milliseconds).unwrap_or(i32::MAX))
}
