//! Lossless rejection facts for sequential group-position reset.

use core::fmt;

use super::GroupPositionResetInput;

/// Rejected reset fence, partition, or lifecycle fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionResetMachineError {
    /// The supplied fact belongs to another membership or assignment.
    StaleFence,
    /// The supplied fact names a different assigned partition.
    StalePartition,
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// A deadline fact arrived before the original deadline.
    DeadlineNotElapsed,
    /// Core already assigned the sole terminal decision.
    AlreadyCompleted,
}

/// Lossless rejection of one normalized reset fact.
#[must_use = "rejected group position reset fact must be recovered or deliberately settled"]
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionResetApplyError {
    kind: GroupPositionResetMachineError,
    input: GroupPositionResetInput,
}

impl GroupPositionResetApplyError {
    pub(super) const fn new(
        kind: GroupPositionResetMachineError,
        input: GroupPositionResetInput,
    ) -> Self {
        Self { kind, input }
    }

    /// Returns the deterministic rejection category.
    pub const fn kind(&self) -> GroupPositionResetMachineError {
        self.kind
    }

    /// Recovers the exact rejected fact.
    pub const fn into_input(self) -> GroupPositionResetInput {
        self.input
    }
}

impl fmt::Display for GroupPositionResetApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "group position reset rejected fact: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupPositionResetApplyError {}
