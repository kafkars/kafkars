//! Exact lease, input, effect, terminal, and rejection facts for graceful revocation.

use crate::{Deadline, Moment};

use super::super::super::AssignmentEpoch;

/// One exact assignment and absolute deadline awaiting application release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicGracefulRevocationLease {
    assignment_epoch: AssignmentEpoch,
    deadline: Deadline,
}

impl ClassicGracefulRevocationLease {
    /// Binds an already-captured absolute deadline to one assignment epoch.
    pub const fn new(assignment_epoch: AssignmentEpoch, deadline: Deadline) -> Self {
        Self {
            assignment_epoch,
            deadline,
        }
    }

    /// Returns the exact assignment delayed by this lease.
    pub const fn assignment_epoch(self) -> AssignmentEpoch {
        self.assignment_epoch
    }

    /// Returns the original absolute revocation deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }
}

/// Why graceful acknowledgment no longer owns revocation completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGracefulRevocationLossReason {
    /// The one captured absolute deadline was observed at or after its boundary.
    DeadlineElapsed,
    /// Membership lost the assignment before application acknowledgment.
    OwnerLost,
}

/// Exact terminal retained until the engine explicitly releases it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGracefulRevocationTerminal {
    /// The application acknowledged revocation before the deadline.
    Acknowledged(ClassicGracefulRevocationLease),
    /// The assignment was lost without successful graceful acknowledgment.
    Lost {
        /// Exact assignment and deadline that lost completion ownership.
        lease: ClassicGracefulRevocationLease,
        /// Stable reason acknowledgment can no longer succeed.
        reason: ClassicGracefulRevocationLossReason,
    },
}

impl ClassicGracefulRevocationTerminal {
    /// Returns the exact lease settled by this terminal.
    pub const fn lease(self) -> ClassicGracefulRevocationLease {
        match self {
            Self::Acknowledged(lease) | Self::Lost { lease, .. } => lease,
        }
    }
}

/// Explicit facts accepted by one graceful-revocation owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGracefulRevocationInput {
    /// Begins waiting only after the bounded lease already exists.
    Begin {
        /// Exact assignment and already-captured absolute deadline.
        lease: ClassicGracefulRevocationLease,
        /// Monotonic observation at admission.
        now: Moment,
    },
    /// Acknowledges that application work for one assignment is released.
    Acknowledge {
        /// Exact assignment epoch observed by the application.
        assignment_epoch: AssignmentEpoch,
        /// Monotonic observation captured at acknowledgment.
        now: Moment,
    },
    /// Proves that the exact scheduled deadline is due.
    DeadlineElapsed {
        /// Exact assignment epoch named by the scheduled deadline.
        assignment_epoch: AssignmentEpoch,
        /// Monotonic observation proving the deadline elapsed.
        now: Moment,
    },
    /// Reports that membership no longer owns the exact assignment.
    OwnerLost {
        /// Exact assignment epoch lost by membership.
        assignment_epoch: AssignmentEpoch,
    },
    /// Releases an observed terminal so another epoch may begin.
    Release {
        /// Exact terminal assignment epoch being released.
        assignment_epoch: AssignmentEpoch,
    },
}

/// One bounded mechanism instruction from graceful-revocation policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGracefulRevocationEffect {
    /// Schedule the exact absolute revocation deadline.
    Arm {
        /// Exact lease whose deadline the engine must schedule.
        lease: ClassicGracefulRevocationLease,
    },
    /// Retain and act on one exact graceful or lost terminal.
    Complete {
        /// Exact terminal that must remain retained through retirement.
        terminal: ClassicGracefulRevocationTerminal,
    },
}

/// Zero or one ordered effect from one allocation-free transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicGracefulRevocationTransition {
    effect: Option<ClassicGracefulRevocationEffect>,
}

impl ClassicGracefulRevocationTransition {
    /// Returns the bounded ordered mechanism instructions.
    pub fn effects(&self) -> impl ExactSizeIterator<Item = &ClassicGracefulRevocationEffect> {
        self.effect.iter()
    }

    pub(in crate::consumer::classic_group) const fn none() -> Self {
        Self { effect: None }
    }

    pub(in crate::consumer::classic_group) const fn one(
        effect: ClassicGracefulRevocationEffect,
    ) -> Self {
        Self {
            effect: Some(effect),
        }
    }
}

/// Rejected fact that leaves the exact prior owner state unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGracefulRevocationError {
    /// A lease is already awaiting acknowledgment.
    AlreadyActive,
    /// No active lease or retained terminal exists for the input.
    NotActive,
    /// The supplied assignment epoch does not name the retained owner.
    AssignmentEpochMismatch,
    /// The supplied observation precedes the absolute deadline.
    DeadlineNotElapsed,
    /// A terminal must be released before another transition can complete.
    TerminalRetained,
    /// Release named a lease that has not reached a terminal.
    NotTerminal,
}

#[derive(Debug, Eq, PartialEq)]
pub(in crate::consumer::classic_group) enum ClassicGracefulRevocationState {
    Dormant,
    Awaiting(ClassicGracefulRevocationLease),
    Terminal(ClassicGracefulRevocationTerminal),
}
