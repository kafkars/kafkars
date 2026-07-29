//! Deterministic policy for caller-selected producer fencing.

mod failure;
mod machine;
mod model;
mod outcome;
mod transition;

pub use failure::{AdminFenceProducersFailure, AdminFenceProducersFailureKind};
pub use machine::{
    AdminFenceProducersEffect, AdminFenceProducersInput, AdminFenceProducersMachine,
    AdminFenceProducersMachineError, AdminFenceProducersState, AdminFenceProducersTransition,
};
pub use model::{
    AdminFenceProducersPlan, AdminFenceProducersPlanError,
    FENCE_PRODUCERS_MAX_TRANSACTIONAL_ID_BYTES, FENCE_PRODUCERS_MAX_TRANSACTIONAL_IDS,
};
pub use outcome::{
    AdminFenceProducerBrokerError, AdminFenceProducerOutcome, AdminFenceProducerResult,
    AdminFenceProducersBatch, AdminFenceProducersTerminal, AdminFencedProducerIdentity,
};

#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
