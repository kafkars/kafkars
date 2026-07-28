//! Deterministic ownership for one leader-election alteration.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    ElectLeadersEffect, ElectLeadersInput, ElectLeadersMachine, ElectLeadersMachineError,
    ElectLeadersState, ElectLeadersTransition,
};
pub use model::{
    ElectLeadersPlan, ElectLeadersPlanError, LeaderElectionTarget, LeaderElectionType,
};
pub use outcome::{
    ElectLeadersBatch, ElectLeadersFailure, ElectLeadersFailureKind, ElectLeadersTerminal,
    LeaderElectionBrokerError, LeaderElectionOutcome, LeaderElectionResult,
};

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod transition_test;
