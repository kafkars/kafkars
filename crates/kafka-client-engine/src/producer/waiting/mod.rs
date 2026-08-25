//! Bounded engine ownership for producer callers waiting before active admission.

mod admission;
pub(super) mod model;
mod partitioning;
#[cfg(test)]
mod partitioning_identity_test;
#[cfg(test)]
mod partitioning_recovery_test;
#[cfg(test)]
mod partitioning_test;
#[cfg(test)]
mod recovery_test;
mod settlement;
mod turn;

pub(crate) use admission::{AdmittedWaiting, ProducerWaitingAdmissionFailure};
pub(super) use model::{ProducerWaitingStats, WaitingToken};
pub(crate) use partitioning::{
    ProducerPartitionSource, ProducerPartitioningFailure, ProducerPartitioningRequest,
};
