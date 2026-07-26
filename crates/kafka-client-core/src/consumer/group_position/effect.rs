//! One-shot `OffsetFetch` and terminal effects from position bootstrap.

use crate::Deadline;

use super::super::group_commit::GroupAssignmentPartition;
use super::{GroupPositionBootstrapTerminal, GroupPositionFence};

/// One concrete mechanism instruction from position bootstrap policy.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapEffect {
    /// Submit one assignment-fenced `OffsetFetch` request.
    FetchOffsets {
        /// Exact group, membership, member, and assignment fence.
        fence: GroupPositionFence,
        /// Original absolute bootstrap deadline.
        deadline: Deadline,
        /// Assigned topic-partitions in deterministic request order.
        partitions: Vec<GroupAssignmentPartition>,
    },
    /// Publish the sole core-owned terminal decision.
    Complete {
        /// Exact group, membership, member, and assignment fence.
        fence: GroupPositionFence,
        /// Original absolute bootstrap deadline.
        deadline: Deadline,
        /// Sole terminal decision.
        terminal: GroupPositionBootstrapTerminal,
    },
}

/// Ordered result of one deterministic bootstrap transition.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionBootstrapTransition {
    effect: Option<GroupPositionBootstrapEffect>,
}

impl GroupPositionBootstrapTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: GroupPositionBootstrapEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<GroupPositionBootstrapEffect> {
        self.effect
    }
}
