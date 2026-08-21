//! Declarative facade for adding one Kafka metadata-quorum voter.

mod builder;
mod operation;
mod result;

pub use builder::AddRaftVoterBuilder;
pub use operation::AddRaftVoter;
pub use result::AddRaftVoterResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
