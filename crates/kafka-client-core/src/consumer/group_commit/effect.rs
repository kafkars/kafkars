//! Linear mechanism requests emitted by group offset-commit policy.

use crate::{Deadline, OperationId};

use super::{GroupCheckpoint, GroupOffsetCommitMachine, GroupOffsetCommitTerminal};

/// One concrete mechanism request from a group offset-commit transition.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitEffect {
    /// Submit the one validated checkpoint with its original deadline.
    Submit {
        /// Stable identity reserved before core admission.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Linear assignment-fenced next offsets.
        checkpoint: GroupCheckpoint,
    },
    /// Publish the one core-owned terminal decision.
    Complete {
        /// Stable operation identity.
        operation_id: OperationId,
        /// Sole terminal decision.
        terminal: GroupOffsetCommitTerminal,
    },
}

/// Ordered result of one deterministic commit transition.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitTransition {
    effect: Option<GroupOffsetCommitEffect>,
}

impl GroupOffsetCommitTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: GroupOffsetCommitEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<GroupOffsetCommitEffect> {
        self.effect
    }
}

/// Atomic machine plus its one linear submit effect.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitAdmission {
    machine: GroupOffsetCommitMachine,
    submit: GroupOffsetCommitEffect,
}

impl GroupOffsetCommitAdmission {
    pub(crate) const fn new(
        machine: GroupOffsetCommitMachine,
        submit: GroupOffsetCommitEffect,
    ) -> Self {
        Self { machine, submit }
    }

    /// Separates the admitted terminal owner from its sole submit effect.
    pub fn into_parts(self) -> (GroupOffsetCommitMachine, GroupOffsetCommitEffect) {
        (self.machine, self.submit)
    }
}
