//! Declarative owner boundary for one assigned partition's fetch position.

mod access;
mod assignment;
mod control;
mod fetch;
mod resolution;
mod resume;
mod state;

pub(super) use resume::{RetainedResolutionActivation, RetainedResolutionPlan};
pub(super) use state::{AssignedPartitionState, RetainedResumePlan};
#[cfg(test)]
mod resume_test;
