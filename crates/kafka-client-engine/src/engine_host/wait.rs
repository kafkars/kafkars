//! Cross-domain deadline selection and the final host sleep handshake.

use std::time::Duration;

use kafka_client_core::{Deadline, Moment};

use crate::producer::host_turn::ProducerTurnOutcome;

use super::{
    assigned_consumer::AssignedConsumerProgress, group_consumer::GroupConsumerProgress,
    share_consumer::ShareConsumerProgress,
};

// Wake failure cannot revoke ownership; this cap preserves deadline and
// shutdown liveness after an operating-system failure.
pub(super) const HOST_PARK_LIMIT: Duration = Duration::from_millis(100);
const BLOCKED_RETRY_DELAY: Duration = HOST_PARK_LIMIT;

pub(super) fn producer(
    now: Moment,
    outcome: Option<ProducerTurnOutcome>,
    driver_more_work: bool,
) -> Duration {
    if driver_more_work {
        return Duration::ZERO;
    }
    let Some(outcome) = outcome else {
        return HOST_PARK_LIMIT;
    };
    if outcome.runnable_work {
        return Duration::ZERO;
    }
    let deadline_wait = outcome.next_deadline.map_or(HOST_PARK_LIMIT, |value| {
        deadline(now, value).min(HOST_PARK_LIMIT)
    });
    if outcome.blocked_work {
        deadline_wait.min(BLOCKED_RETRY_DELAY)
    } else {
        deadline_wait
    }
}

pub(super) fn assigned_consumer(
    now: Moment,
    current: Duration,
    progress: &AssignedConsumerProgress,
) -> Duration {
    if progress.progressed {
        return Duration::ZERO;
    }
    let deadline_wait = progress.next_deadline.map_or(HOST_PARK_LIMIT, |value| {
        deadline(now, value).min(HOST_PARK_LIMIT)
    });
    let wait = current.min(deadline_wait);
    if progress.blocked_work {
        wait.min(BLOCKED_RETRY_DELAY)
    } else {
        wait
    }
}

pub(super) fn group_consumer(
    now: Moment,
    current: Duration,
    progress: &GroupConsumerProgress,
) -> Duration {
    if progress.progressed {
        return Duration::ZERO;
    }
    let wait = progress
        .next_deadline
        .map_or(current, |value| current.min(deadline(now, value)));
    if progress.blocked_work {
        wait.min(BLOCKED_RETRY_DELAY)
    } else {
        wait
    }
}

pub(super) fn share_consumer(
    now: Moment,
    current: Duration,
    progress: &ShareConsumerProgress,
) -> Duration {
    if progress.progressed {
        return Duration::ZERO;
    }
    let wait = progress
        .next_deadline
        .map_or(current, |value| current.min(deadline(now, value)));
    if progress.blocked_work {
        wait.min(BLOCKED_RETRY_DELAY)
    } else {
        wait
    }
}

pub(super) const fn host_turn(current: Duration, requested: bool) -> Duration {
    if requested { Duration::ZERO } else { current }
}

pub(super) fn deadline(now: Moment, value: Deadline) -> Duration {
    Duration::from_nanos(value.tick().saturating_sub(now.tick()))
}
