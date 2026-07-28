//! Declarative facade for deterministic consumer-group member removal policy.

mod machine;
mod model;
mod outcome;
mod transition;

pub use machine::{
    RemoveConsumerGroupMembersEffect, RemoveConsumerGroupMembersInput,
    RemoveConsumerGroupMembersMachine, RemoveConsumerGroupMembersMachineError,
    RemoveConsumerGroupMembersState, RemoveConsumerGroupMembersTransition,
};
pub use model::{
    ConsumerGroupMemberRemoval, RemoveConsumerGroupMembersPlan, RemoveConsumerGroupMembersPlanError,
};
pub use outcome::{
    ConsumerGroupMemberRemovalBrokerError, ConsumerGroupMemberRemovalOutcome,
    ConsumerGroupMemberRemovalResult, RemoveConsumerGroupMembersBatch,
    RemoveConsumerGroupMembersFailure, RemoveConsumerGroupMembersFailureKind,
    RemoveConsumerGroupMembersTerminal,
};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
