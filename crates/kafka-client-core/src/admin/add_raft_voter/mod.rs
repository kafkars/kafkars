//! Deterministic policy for one committed Admin `AddRaftVoter` request.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    AddRaftVoterEffect, AddRaftVoterInput, AddRaftVoterMachine, AddRaftVoterMachineError,
    AddRaftVoterState, AddRaftVoterTransition,
};
pub use model::{
    ADD_RAFT_VOTER_MAX_LISTENERS, ADD_RAFT_VOTER_MAX_REQUEST_TEXT_BYTES,
    ADD_RAFT_VOTER_MAX_TEXT_BYTES, AddRaftVoterEndpoint, AddRaftVoterPlan, AddRaftVoterPlanError,
};
pub use outcome::{
    ADD_RAFT_VOTER_DIAGNOSTIC_BYTES, AddRaftVoterBrokerError, AddRaftVoterFailure,
    AddRaftVoterFailureKind, AddRaftVoterSuccess, AddRaftVoterTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
