//! Scalar timer identities and lossless assigned-timer outcomes.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedTopicPartition, Deadline, FetchFence,
    Moment, PositionFence,
};

/// Result of arming one partition-owned throttle timer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedTimerDisposition {
    Inserted,
    Replaced,
    Idempotent,
    Fenced,
}

/// Lossless failure to retain one core timer effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedTimerError {
    Capacity {
        capacity: usize,
        effect: AssignedConsumerEffect,
    },
    Allocation {
        effect: AssignedConsumerEffect,
    },
    DeadlineConflict {
        active_deadline: Deadline,
        effect: AssignedConsumerEffect,
    },
    InsertionSequenceExhausted {
        effect: AssignedConsumerEffect,
    },
}

#[derive(Debug)]
pub(super) enum AssignedTimerKind {
    Position(PositionFence),
    Fetch(FetchFence),
}

impl AssignedTimerKind {
    pub(super) const fn position(&self) -> PositionFence {
        match self {
            Self::Position(fence) => *fence,
            Self::Fetch(fence) => fence.position(),
        }
    }

    pub(super) const fn partition(&self) -> AssignedTopicPartition {
        self.position().partition()
    }

    pub(super) const fn input(&self, now: Moment) -> AssignedConsumerInput {
        match self {
            Self::Position(fence) => {
                AssignedConsumerInput::PositionThrottleElapsed { fence: *fence, now }
            }
            Self::Fetch(fence) => {
                AssignedConsumerInput::FetchThrottleElapsed { fence: *fence, now }
            }
        }
    }

    pub(super) const fn effect(&self, deadline: Deadline) -> AssignedConsumerEffect {
        match self {
            Self::Position(fence) => AssignedConsumerEffect::ArmPositionThrottle {
                fence: *fence,
                deadline,
            },
            Self::Fetch(fence) => AssignedConsumerEffect::ArmFetchThrottle {
                fence: *fence,
                deadline,
            },
        }
    }
}
