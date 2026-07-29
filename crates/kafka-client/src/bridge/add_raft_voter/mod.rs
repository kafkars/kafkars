//! Declarative private bridge for adding one Kafka metadata-quorum voter.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminAddRaftVoter;
pub(crate) use request::{AddRaftVoterAdminRequest, translate_request};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
