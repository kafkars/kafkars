//! Declarative private bridge for removing one Kafka metadata-quorum voter.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminRemoveRaftVoter;
pub(crate) use request::{RemoveRaftVoterAdminRequest, translate_request};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
