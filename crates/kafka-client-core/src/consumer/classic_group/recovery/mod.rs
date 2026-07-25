//! Declarative recovery policy, transition, and sibling evidence surface.

mod broker_error;
mod error_disposition;
mod rejection_transition;
mod rejoin;
mod rejoin_transition;

pub use broker_error::ClassicBrokerError;
pub use error_disposition::ClassicBrokerStage;
pub use rejoin::{
    ClassicCoordinatorRecovery, ClassicGroupFatal, ClassicGroupFatalReason, ClassicRejoinPolicy,
    ClassicRejoinPolicyError, ClassicRejoinSchedule,
};

#[cfg(test)]
pub(super) use super::{
    ClassicGeneration, ClassicGroupInput, ClassicGroupTiming, ClassicHeartbeatPolicy,
};
pub(super) use super::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupMachine, ClassicGroupPhase,
    ClassicGroupTransition, ClassicHeartbeatAttempt, MembershipCycle,
};

#[cfg(test)]
mod broker_error_test;
#[cfg(test)]
mod error_disposition_test;
#[cfg(test)]
mod rejection_transition_test;
#[cfg(test)]
mod rejoin_test;
#[cfg(test)]
mod rejoin_transition_test;
