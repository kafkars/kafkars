//! Bounded retry timing for metadata states that can lag a successful creation.

use kafka_client_core::{Deadline, Moment};

use super::super::topic_view::TopicPartitionCountFailure;

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const LEADER_NOT_AVAILABLE: i16 = 5;
const INITIAL_VISIBILITY_RETRY_DELAY_TICKS: u64 = 25_000_000;
const MAX_VISIBILITY_RETRY_DELAY_TICKS: u64 = 100_000_000;

// At the capped delay this spans more than 25 seconds, so the usual 30-second
// Admin deadline remains the effective bound without admitting an unbounded loop.
pub(super) const MAX_VISIBILITY_ATTEMPTS_PER_TOPIC: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VisibilityRetryKind {
    Current,
    NewerThan(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct VisibilityRetry {
    not_before: Deadline,
    kind: VisibilityRetryKind,
}

impl VisibilityRetry {
    pub(super) fn schedule(
        kind: VisibilityRetryKind,
        now: Moment,
        public_deadline: Deadline,
        attempts_completed: usize,
    ) -> Option<Self> {
        let not_before = now.checked_deadline_after(retry_delay_ticks(attempts_completed))?;
        if not_before.tick() >= public_deadline.tick() {
            return None;
        }
        Some(Self { not_before, kind })
    }

    pub(super) const fn not_before(self) -> Deadline {
        self.not_before
    }

    pub(super) const fn kind(self) -> VisibilityRetryKind {
        self.kind
    }

    pub(super) const fn is_due(self, now: Moment) -> bool {
        self.not_before.is_elapsed_at(now)
    }
}

const fn retry_delay_ticks(attempts_completed: usize) -> u64 {
    match attempts_completed {
        0 | 1 => INITIAL_VISIBILITY_RETRY_DELAY_TICKS,
        2 => INITIAL_VISIBILITY_RETRY_DELAY_TICKS * 2,
        _ => MAX_VISIBILITY_RETRY_DELAY_TICKS,
    }
}

pub(in super::super) const fn visibility_failure_is_transient(
    failure: TopicPartitionCountFailure,
) -> bool {
    matches!(
        failure,
        TopicPartitionCountFailure::Unavailable
            | TopicPartitionCountFailure::Refresh
            | TopicPartitionCountFailure::Broker(UNKNOWN_TOPIC_OR_PARTITION | LEADER_NOT_AVAILABLE)
    )
}
