//! Deterministic rejection vocabulary for classic membership transitions.

use core::fmt;

use crate::LiveGroupAssignmentError;

use super::ClassicAssignmentError;

/// Why one input could not change classic membership state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGroupErrorKind {
    /// Membership admission was permanently closed.
    Closed,
    /// The input does not belong to the current lifecycle phase.
    InvalidPhase,
    /// The input names a stale or future membership cycle.
    CycleMismatch,
    /// `MEMBER_ID_REQUIRED` omitted its assigned nonempty member identity.
    MissingAssignedMemberId,
    /// One active cycle already consumed its sole assigned member identity.
    DuplicateAssignedMemberId,
    /// Join success did not match the identity assigned to its replacement.
    AssignedMemberIdMismatch,
    /// Begin or a broker fact arrived after the original deadline.
    DeadlineElapsed,
    /// A timer fact arrived before the original deadline.
    DeadlineNotElapsed,
    /// A checked heartbeat cadence or attempt deadline could not be represented.
    DeadlineOverflow,
    /// A heartbeat fact does not own the current assignment and sequence.
    HeartbeatMismatch,
    /// A due fact does not own the currently pending recovery schedule.
    RejoinMismatch,
    /// No later nonreused membership cycle can be represented.
    CycleExhausted,
    /// No later nonreused assignment generation can be represented.
    AssignmentGenerationExhausted,
    /// The leader's member set omitted or mismatched the local member.
    LocalMemberMissing,
    /// The union of subscribed group topics exceeded its reviewed bound.
    TooManyGroupTopics,
    /// Range planning rejected bounded normalized facts.
    Assignment(ClassicAssignmentError),
    /// The decoded local Sync assignment exceeded its reviewed bound.
    LocalAssignmentTooLarge,
    /// The decoded local Sync assignment was not ordered and unique.
    InvalidLiveAssignment(LiveGroupAssignmentError),
    /// Leader Sync returned a local assignment different from the Range plan.
    LeaderAssignmentMismatch,
    /// Required bounded storage could not be reserved.
    AllocationFailed,
    /// Internal phase facts were incomplete despite successful validation.
    InvariantViolation,
}

/// Deterministic rejection of one normalized lifecycle fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicGroupApplyError {
    kind: ClassicGroupErrorKind,
}

impl ClassicGroupApplyError {
    pub(crate) const fn new(kind: ClassicGroupErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the deterministic rejection reason.
    pub const fn kind(&self) -> ClassicGroupErrorKind {
        self.kind
    }
}

impl fmt::Display for ClassicGroupApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "classic membership input rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ClassicGroupApplyError {}
