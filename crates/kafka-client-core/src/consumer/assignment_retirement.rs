//! Linear deadline-free input for retiring one exact active Fetch assignment.

use core::fmt;

use super::AssignmentEpoch;

/// Exact optional complete-assignment control revision that may be retired.
#[must_use = "an assignment retirement must be applied or explicitly abandoned"]
#[derive(Debug, Eq, PartialEq)]
pub struct RetireAssignment {
    expected_assignment_epoch: Option<AssignmentEpoch>,
}

impl RetireAssignment {
    /// Binds retirement to the complete-assignment control state observed by its owner.
    pub const fn new(expected_assignment_epoch: Option<AssignmentEpoch>) -> Self {
        Self {
            expected_assignment_epoch,
        }
    }

    /// Returns the exact optional control revision this input may retire.
    pub const fn expected_assignment_epoch(&self) -> Option<AssignmentEpoch> {
        self.expected_assignment_epoch
    }
}

/// Lossless rejection of one exact assignment retirement.
#[must_use = "a rejected assignment retirement must be recovered or explicitly abandoned"]
#[derive(Debug, Eq, PartialEq)]
pub struct RetireAssignmentError {
    kind: RetireAssignmentErrorKind,
    input: RetireAssignment,
}

impl RetireAssignmentError {
    pub(super) const fn new(kind: RetireAssignmentErrorKind, input: RetireAssignment) -> Self {
        Self { kind, input }
    }

    /// Returns the deterministic rejection reason.
    pub const fn kind(&self) -> RetireAssignmentErrorKind {
        self.kind
    }

    /// Borrows the exact rejected retirement input.
    pub const fn input(&self) -> &RetireAssignment {
        &self.input
    }

    /// Recovers the exact rejected retirement input.
    pub fn into_input(self) -> RetireAssignment {
        self.input
    }
}

impl fmt::Display for RetireAssignmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "assignment retirement rejected: {:?}", self.kind)
    }
}

impl std::error::Error for RetireAssignmentError {}

/// Deterministic reason one exact assignment could not be retired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetireAssignmentErrorKind {
    /// Assigned-consumer admission is permanently closed.
    ConsumerClosed,
    /// The input targets a different optional active assignment.
    AssignmentEpochMismatch {
        /// Optional assignment the input was permitted to retire.
        expected: Option<AssignmentEpoch>,
        /// Assignment retained when the input reached core.
        actual: Option<AssignmentEpoch>,
    },
    /// Ordered revoke-effect storage could not be reserved.
    EffectAllocationFailed,
}
