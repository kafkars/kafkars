//! Ordered direct-consumer actions for a future engine fetch interpreter.

use super::{
    AssignedTopicPartition, AssignmentEpoch, FetchFence, NextFetchOffset, PositionFence,
    StartPosition,
};

/// One ordered action selected by deterministic direct-consumer policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerEffect {
    /// Revokes all interpreter work for an assignment partition.
    Revoke {
        /// Superseded assignment epoch.
        assignment_epoch: AssignmentEpoch,
        /// Superseded partition.
        partition: AssignedTopicPartition,
    },
    /// Suspends work older than the supplied position fence.
    Suspend {
        /// Newly installed fence; older work must not be applied.
        fence: PositionFence,
    },
    /// Resolves an earliest or end-offset start position.
    ResolvePosition {
        /// Exact generation of the resolution request.
        fence: PositionFence,
        /// Beginning or end policy to resolve.
        position: StartPosition,
    },
    /// Fetches from one exact partition position.
    Fetch {
        /// Exact execution identity for the fetch.
        fence: FetchFence,
        /// Offset used by this fetch.
        next_offset: NextFetchOffset,
    },
}

/// Ordered output of one accepted direct-consumer transition.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerTransition {
    assignment_epoch: AssignmentEpoch,
    effects: Vec<AssignedConsumerEffect>,
}

impl AssignedConsumerTransition {
    pub(crate) const fn new(
        assignment_epoch: AssignmentEpoch,
        effects: Vec<AssignedConsumerEffect>,
    ) -> Self {
        Self {
            assignment_epoch,
            effects,
        }
    }

    /// Returns the active assignment generation after the transition.
    pub const fn assignment_epoch(&self) -> AssignmentEpoch {
        self.assignment_epoch
    }

    /// Borrows interpreter actions in deterministic execution order.
    pub fn effects(&self) -> &[AssignedConsumerEffect] {
        &self.effects
    }

    /// Moves the ordered actions into a future interpreter.
    pub fn into_effects(self) -> Vec<AssignedConsumerEffect> {
        self.effects
    }
}
