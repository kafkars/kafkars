//! Deterministic producer, consumer, transaction, and admin policy.
#![forbid(unsafe_code)]

mod admin;
mod admission;
mod capacity;
mod completion;
mod operation;
mod operation_outcome;
pub mod partitioning;
mod producer;
mod producer_broker_failure;
mod producer_effect;
mod producer_error;
mod producer_failure;
mod producer_input;
mod producer_policy;
mod producer_record;
mod producer_transition;
mod producer_transition_result;
mod public_api;
mod types;

pub use public_api::*;

#[cfg(test)]
mod capacity_test;
#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod producer_broker_failure_test;
#[cfg(test)]
mod producer_failure_test;
#[cfg(test)]
mod producer_outcome_test;
#[cfg(test)]
mod producer_reclaim_test;
#[cfg(test)]
mod producer_submission_deadline_test;
#[cfg(test)]
mod producer_test;
#[cfg(test)]
mod producer_timer_test;
#[cfg(test)]
mod producer_transition_identity_test;
#[cfg(test)]
mod producer_transition_test;
