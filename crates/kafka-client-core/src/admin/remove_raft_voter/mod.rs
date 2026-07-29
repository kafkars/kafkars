//! Deterministic policy for one destructive Admin `RemoveRaftVoter` request.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    RemoveRaftVoterEffect, RemoveRaftVoterInput, RemoveRaftVoterMachine,
    RemoveRaftVoterMachineError, RemoveRaftVoterState, RemoveRaftVoterTransition,
};
pub use model::{
    REMOVE_RAFT_VOTER_MAX_CLUSTER_ID_BYTES, RemoveRaftVoterPlan, RemoveRaftVoterPlanError,
};
pub use outcome::{
    REMOVE_RAFT_VOTER_DIAGNOSTIC_BYTES, RemoveRaftVoterBrokerError, RemoveRaftVoterFailure,
    RemoveRaftVoterFailureKind, RemoveRaftVoterSuccess, RemoveRaftVoterTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
